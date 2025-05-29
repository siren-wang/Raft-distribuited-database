"""
Raft Cluster Node Runner
Main entry point for running a Raft consensus node
"""

import os
import json
import asyncio
import logging
import signal
from typing import Dict
import uvicorn

from src.kv_store import DatabaseConfig
from src.raft.server import RaftKVStore, create_raft_api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RaftClusterNode:
    """Manages a single Raft cluster node"""
    
    def __init__(self):
        # Get configuration from environment
        self.node_id = os.environ.get("NODE_ID", "node1")
        self.node_port = int(os.environ.get("NODE_PORT", "8001"))
        
        # Parse cluster configuration
        cluster_config_str = os.environ.get(
            "CLUSTER_CONFIG",
            '{"node1":"localhost:8001","node2":"localhost:8002","node3":"localhost:8003"}'
        )
        self.cluster_config = json.loads(cluster_config_str)
        
        # Database configuration
        self.db_config = DatabaseConfig(
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT", "5432")),
            database=os.environ.get("DB_NAME", f"kvstore_{self.node_id}"),
            user=os.environ.get("DB_USER", "siren"),
            password=os.environ.get("DB_PASSWORD", "")
        )
        
        # State directories
        self.state_dir = os.environ.get("RAFT_STATE_DIR", "./raft_state")
        self.wal_dir = os.environ.get("RAFT_WAL_DIR", "./raft_wal")
        
        # Initialize components
        self.raft_store = None
        self.app = None
        self.server = None
        
        logger.info(f"Initialized RaftClusterNode {self.node_id} on port {self.node_port}")
    
    async def start(self):
        """Start the Raft node"""
        logger.info(f"Starting Raft node {self.node_id}...")
        
        # Create Raft KV store
        self.raft_store = RaftKVStore(
            node_id=self.node_id,
            cluster_config=self.cluster_config,
            db_config=self.db_config,
            state_dir=self.state_dir,
            wal_dir=self.wal_dir
        )
        
        # Start Raft components
        await self.raft_store.start()
        
        # Create FastAPI app
        self.app = create_raft_api(self.raft_store)
        
        # Configure and start server
        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=self.node_port,
            log_level="info"
        )
        self.server = uvicorn.Server(config)
        
        logger.info(f"Raft node {self.node_id} started on port {self.node_port}")
        
        # Run server
        await self.server.serve()
    
    async def stop(self):
        """Stop the Raft node"""
        logger.info(f"Stopping Raft node {self.node_id}...")
        
        if self.server:
            self.server.should_exit = True
        
        if self.raft_store:
            await self.raft_store.stop()
        
        logger.info(f"Raft node {self.node_id} stopped")


async def main():
    """Main entry point"""
    node = RaftClusterNode()
    
    # Set up signal handlers
    stop_event = asyncio.Event()
    
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start node
    try:
        await node.start()
    except Exception as e:
        logger.error(f"Error running node: {e}")
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())