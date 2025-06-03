"""
Raft Server Integration
Integrates Raft consensus with the key-value store
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .node import RaftNode
from .rpc import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse
from .log import WriteAheadLog
from ..kv_store import KeyValueStore, DatabaseConfig

logger = logging.getLogger(__name__)


class RaftCommand(BaseModel):
    """Command to be replicated through Raft"""
    operation: str  # put, update, delete
    key: str
    value: Optional[Any] = None
    version: Optional[int] = None


class KVRequest(BaseModel):
    """Request model for key-value operations"""
    value: Any


class KVUpdateRequest(BaseModel):
    """Request model for key-value update operations"""
    value: Any
    version: int


class KVResponse(BaseModel):
    """Response model for key-value operations"""
    key: str
    value: Any
    version: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class RaftKVStore:
    """
    Distributed key-value store using Raft consensus
    """
    
    def __init__(self,
                 node_id: str,
                 cluster_config: Dict[str, str],
                 db_config: DatabaseConfig,
                 state_dir: str = "./raft_state",
                 wal_dir: str = "./raft_wal",
                 election_timeout_range: tuple = (5.0, 10.0),
                 heartbeat_interval: float = 1.0):
        """
        Initialize Raft-based KV store
        
        Args:
            node_id: Unique identifier for this node
            cluster_config: Dict mapping node_id to address
            db_config: Database configuration for local KV store
            state_dir: Directory for Raft state
            wal_dir: Directory for Write-Ahead Log
            election_timeout_range: Min and max election timeout in seconds
            heartbeat_interval: Heartbeat interval in seconds
        """
        self.node_id = node_id
        self.cluster_config = cluster_config
        
        # Initialize components with timeout parameters
        self.raft_node = RaftNode(
            node_id, 
            cluster_config, 
            state_dir,
            election_timeout_range=election_timeout_range,
            heartbeat_interval=heartbeat_interval
        )
        self.wal = WriteAheadLog(node_id, wal_dir)
        self.kv_store = KeyValueStore(db_config)
        
        # Set up command application callback
        self.raft_node.set_apply_command_callback(self._apply_command)
        
        # Track pending operations
        self.pending_operations: Dict[str, asyncio.Future] = {}
        
        logger.info(f"Initialized RaftKVStore for node {node_id}")
        logger.info(f"Timeout config: election={election_timeout_range}, heartbeat={heartbeat_interval}s")
    
    async def start(self):
        """Start the Raft KV store"""
        logger.info(f"Starting RaftKVStore {self.node_id}")
        
        # Initialize components
        await self.kv_store.initialize()
        await self.wal.initialize()
        await self.raft_node.start()
        
        # Recover from WAL
        await self._recover_from_wal()
        
        logger.info(f"RaftKVStore {self.node_id} started successfully")
    
    async def stop(self):
        """Stop the Raft KV store"""
        logger.info(f"Stopping RaftKVStore {self.node_id}")
        
        await self.raft_node.stop()
        await self.wal.close()
        await self.kv_store.close()
        
        logger.info(f"RaftKVStore {self.node_id} stopped")
    
    async def get(self, key: str, linearizable: bool = True) -> Dict[str, Any]:
        """
        Get a value from the distributed store
        
        Args:
            key: Key to retrieve
            linearizable: If True, ensure linearizable read (may be slower)
            
        Returns:
            Value and metadata
        """
        if linearizable and not self.raft_node.is_leader():
            # For linearizable reads, redirect to leader or use read index
            leader_id = self.raft_node.get_leader_id()
            if not leader_id:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="No leader elected"
                )
            
            # In production, would redirect to leader
            # For now, just indicate not leader
            raise HTTPException(
                status_code=status.HTTP_421_MISDIRECTED_REQUEST,
                detail=f"Not leader, current leader: {leader_id}"
            )
        
        # Read from local store
        return await self.kv_store.get(key)
    
    async def put(self, key: str, value: Any) -> Dict[str, Any]:
        """
        Put a value into the distributed store
        
        Args:
            key: Key to store
            value: Value to store
            
        Returns:
            Stored value and metadata
        """
        if not self.raft_node.is_leader():
            leader_id = self.raft_node.get_leader_id()
            raise HTTPException(
                status_code=status.HTTP_421_MISDIRECTED_REQUEST,
                detail=f"Not leader, current leader: {leader_id}"
            )
        
        # Create command
        command = {
            "operation": "put",
            "key": key,
            "value": value,
            "request_id": f"{self.node_id}_{datetime.utcnow().timestamp()}"
        }
        
        # Propose through Raft
        success = await self.raft_node.propose_command(command)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to propose command"
            )
        
        # Wait for command to be applied
        future = asyncio.Future()
        self.pending_operations[command["request_id"]] = future
        
        try:
            result = await asyncio.wait_for(future, timeout=5.0)
            return result
        except asyncio.TimeoutError:
            del self.pending_operations[command["request_id"]]
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Operation timeout"
            )
    
    async def update(self, key: str, value: Any, version: int) -> Dict[str, Any]:
        """
        Update a value with optimistic locking
        
        Args:
            key: Key to update
            value: New value
            version: Expected version
            
        Returns:
            Updated value and metadata
        """
        if not self.raft_node.is_leader():
            leader_id = self.raft_node.get_leader_id()
            raise HTTPException(
                status_code=status.HTTP_421_MISDIRECTED_REQUEST,
                detail=f"Not leader, current leader: {leader_id}"
            )
        
        # Create command
        command = {
            "operation": "update",
            "key": key,
            "value": value,
            "version": version,
            "request_id": f"{self.node_id}_{datetime.utcnow().timestamp()}"
        }
        
        # Propose through Raft
        success = await self.raft_node.propose_command(command)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to propose command"
            )
        
        # Wait for command to be applied
        future = asyncio.Future()
        self.pending_operations[command["request_id"]] = future
        
        try:
            result = await asyncio.wait_for(future, timeout=5.0)
            return result
        except asyncio.TimeoutError:
            del self.pending_operations[command["request_id"]]
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Operation timeout"
            )
    
    async def delete(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Delete a key from the distributed store
        
        Args:
            key: Key to delete
            
        Returns:
            Deleted value and metadata if key existed
        """
        if not self.raft_node.is_leader():
            leader_id = self.raft_node.get_leader_id()
            raise HTTPException(
                status_code=status.HTTP_421_MISDIRECTED_REQUEST,
                detail=f"Not leader, current leader: {leader_id}"
            )
        
        # Create command
        command = {
            "operation": "delete",
            "key": key,
            "request_id": f"{self.node_id}_{datetime.utcnow().timestamp()}"
        }
        
        # Propose through Raft
        success = await self.raft_node.propose_command(command)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to propose command"
            )
        
        # Wait for command to be applied
        future = asyncio.Future()
        self.pending_operations[command["request_id"]] = future
        
        try:
            result = await asyncio.wait_for(future, timeout=5.0)
            return result
        except asyncio.TimeoutError:
            del self.pending_operations[command["request_id"]]
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Operation timeout"
            )
    
    async def _apply_command(self, command: Dict[str, Any]):
        """
        Apply a committed command to the state machine
        
        This is called by the Raft node when a command is committed
        """
        operation = command.get("operation")
        key = command.get("key")
        request_id = command.get("request_id")
        
        try:
            result = None
            error = None
            
            if operation == "put":
                value = command.get("value")
                result = await self.kv_store.put(key, value)
                
            elif operation == "update":
                value = command.get("value")
                version = command.get("version")
                result = await self.kv_store.update(key, value, version)
                
            elif operation == "delete":
                result = await self.kv_store.delete(key)
                
            else:
                error = f"Unknown operation: {operation}"
                logger.error(error)
            
            # Complete pending operation if exists
            if request_id and request_id in self.pending_operations:
                future = self.pending_operations[request_id]
                if error:
                    future.set_exception(Exception(error))
                else:
                    future.set_result(result)
                del self.pending_operations[request_id]
                
        except Exception as e:
            logger.error(f"Error applying command: {e}")
            if request_id and request_id in self.pending_operations:
                future = self.pending_operations[request_id]
                future.set_exception(e)
                del self.pending_operations[request_id]
    
    async def _recover_from_wal(self):
        """Recover state from Write-Ahead Log"""
        logger.info("Recovering from WAL...")
        
        # Read all entries from WAL
        entries = await self.wal.read_entries_from(1)
        
        # Apply entries up to last_applied
        for entry in entries:
            if entry.index <= self.raft_node.state.last_applied:
                await self._apply_command(entry.command)
        
        logger.info(f"Recovered {len(entries)} entries from WAL")
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get current cluster status"""
        return {
            "node_id": self.node_id,
            "state": self.raft_node.state.state,
            "current_term": self.raft_node.state.current_term,
            "current_leader": self.raft_node.current_leader,
            "log_length": len(self.raft_node.state.log),
            "commit_index": self.raft_node.state.commit_index,
            "last_applied": self.raft_node.state.last_applied,
            "cluster_size": len(self.cluster_config),
            "wal_stats": self.wal.get_stats()
        }


def create_raft_api(raft_store: RaftKVStore) -> FastAPI:
    """Create FastAPI app with Raft endpoints"""
    
    app = FastAPI(title=f"Raft Node {raft_store.node_id}")
    
    # Add CORS middleware for visual dashboard
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Allow all origins for demo
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Raft RPC endpoints
    @app.post("/raft/request_vote")
    async def handle_request_vote(request: RequestVoteRequest):
        """Handle RequestVote RPC"""
        response = await raft_store.raft_node.rpc_handler.handle_request_vote(request)
        return response
    
    @app.post("/raft/append_entries")
    async def handle_append_entries(request: AppendEntriesRequest):
        """Handle AppendEntries RPC"""
        response = await raft_store.raft_node.rpc_handler.handle_append_entries(request)
        return response
    
    # Cluster status endpoint
    @app.get("/raft/status")
    async def get_status():
        """Get Raft cluster status"""
        return raft_store.get_cluster_status()
    @app.get("/raft/debug")
    async def get_debug_info():
        """Get detailed debug information"""
        return {
            "node_id": raft_store.node_id,
            "state": raft_store.raft_node.state.state,
            "current_term": raft_store.raft_node.state.current_term,
            "voted_for": raft_store.raft_node.state.voted_for,
            "current_leader": raft_store.raft_node.current_leader,
            "log_length": len(raft_store.raft_node.state.log),
            "last_log_index": raft_store.raft_node.state.get_last_log_index(),
            "last_log_term": raft_store.raft_node.state.get_last_log_term(),
            "commit_index": raft_store.raft_node.state.commit_index,
            "last_applied": raft_store.raft_node.state.last_applied,
            "next_index": dict(raft_store.raft_node.state.next_index),
            "match_index": dict(raft_store.raft_node.state.match_index),
            "log_entries": [
                {
                    "index": entry.index,
                    "term": entry.term,
                    "command_type": entry.command.get("type", entry.command.get("operation", "unknown"))
                }
                for entry in raft_store.raft_node.state.log[-10:]  # Last 10 entries
            ]
        }
    
    # Key-value operations with proper request handling
    @app.get("/kv/{key}")
    async def get_value(key: str, linearizable: bool = True):
        """Get a value from the distributed store"""
        try:
            result = await raft_store.get(key, linearizable)
            return KVResponse(
                key=key,
                value=result["value"],
                version=result["version"],
                created_at=result.get("created_at"),
                updated_at=result.get("updated_at")
            )
        except Exception as e:
            if "not found" in str(e).lower():
                raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.put("/kv/{key}")
    async def put_value(key: str, request: KVRequest):
        """Put a value into the distributed store"""
        try:
            result = await raft_store.put(key, request.value)
            return KVResponse(
                key=key,
                value=result["value"],
                version=result["version"],
                created_at=result.get("created_at"),
                updated_at=result.get("updated_at")
            )
        except HTTPException:
            raise  # Re-raise HTTP exceptions as-is
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/kv/{key}")
    async def update_value(key: str, request: KVUpdateRequest):
        """Update a value with version check"""
        try:
            result = await raft_store.update(key, request.value, request.version)
            return KVResponse(
                key=key,
                value=result["value"],
                version=result["version"],
                created_at=result.get("created_at"),
                updated_at=result.get("updated_at")
            )
        except HTTPException:
            raise  # Re-raise HTTP exceptions as-is
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.delete("/kv/{key}")
    async def delete_value(key: str):
        """Delete a key from the distributed store"""
        try:
            result = await raft_store.delete(key)
            if result:
                return KVResponse(
                    key=key,
                    value=result["value"],
                    version=result["version"],
                    created_at=result.get("created_at"),
                    updated_at=result.get("updated_at")
                )
            return {"deleted": True}
        except HTTPException:
            raise  # Re-raise HTTP exceptions as-is
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    return app


# Example usage
async def main():
    """Example of running a Raft cluster"""
    
    # Example 3-node cluster configuration
    cluster_config = {
        "node1": "localhost:8001",
        "node2": "localhost:8002",
        "node3": "localhost:8003"
    }
    
    # Create node1
    db_config = DatabaseConfig(
        host="localhost",
        port=5432,
        database="kvstore_node1",
        user="siren",
        password=""
    )
    
    raft_store = RaftKVStore("node1", cluster_config, db_config)
    await raft_store.start()
    
    # Create FastAPI app
    app = create_raft_api(raft_store)
    
    # Run with uvicorn
    import uvicorn
    config = uvicorn.Config(app, host="0.0.0.0", port=8001)
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())