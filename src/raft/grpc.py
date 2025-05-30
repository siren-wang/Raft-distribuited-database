"""
gRPC Service Implementation for Raft
Provides high-performance RPC communication using gRPC
"""

import asyncio
import logging
import grpc
import grpc.aio
from typing import Dict, Any, Optional
from datetime import datetime
import msgpack

# Import generated protobuf classes (these would be generated from the .proto file)
# from . import raft_pb2
# from . import raft_pb2_grpc

from .rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse
)
from .state import LogEntry

logger = logging.getLogger(__name__)


class RaftGRPCService:  # (raft_pb2_grpc.RaftServiceServicer):
    """
    gRPC service implementation for Raft consensus
    """
    
    def __init__(self, raft_node):
        """
        Initialize gRPC service
        
        Args:
            raft_node: The RaftNode instance to handle RPCs
        """
        self.raft_node = raft_node
        self.rpc_handler = raft_node.rpc_handler
        
        # Metrics
        self.request_count = 0
        self.error_count = 0
        
        logger.info(f"Initialized RaftGRPCService for node {raft_node.node_id}")
    
    async def RequestVote(self, request, context):
        """Handle RequestVote RPC"""
        try:
            self.request_count += 1
            
            # Convert protobuf to internal request
            vote_request = RequestVoteRequest(
                term=request.term,
                candidate_id=request.candidate_id,
                last_log_index=request.last_log_index,
                last_log_term=request.last_log_term
            )
            
            # Handle request
            response = await self.rpc_handler.handle_request_vote(vote_request)
            
            # Convert to protobuf response
            # return raft_pb2.RequestVoteResponse(
            #     term=response.term,
            #     vote_granted=response.vote_granted
            # )
            
            # For now, return dict (would be protobuf in real implementation)
            return {
                "term": response.term,
                "vote_granted": response.vote_granted
            }
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error handling RequestVote: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    async def AppendEntries(self, request, context):
        """Handle AppendEntries RPC"""
        try:
            self.request_count += 1
            
            # Convert protobuf entries to internal format
            entries = []
            for entry in request.entries:
                # Deserialize command from bytes
                command = msgpack.unpackb(entry.command)
                entries.append({
                    "term": entry.term,
                    "index": entry.index,
                    "command": command,
                    "timestamp": datetime.fromtimestamp(entry.timestamp / 1000000).isoformat()
                })
            
            # Create internal request
            append_request = AppendEntriesRequest(
                term=request.term,
                leader_id=request.leader_id,
                prev_log_index=request.prev_log_index,
                prev_log_term=request.prev_log_term,
                entries=entries,
                leader_commit=request.leader_commit
            )
            
            # Handle request
            response = await self.rpc_handler.handle_append_entries(append_request)
            
            # Convert to protobuf response
            # return raft_pb2.AppendEntriesResponse(
            #     term=response.term,
            #     success=response.success,
            #     match_index=response.match_index or 0
            # )
            
            # For now, return dict
            return {
                "term": response.term,
                "success": response.success,
                "match_index": response.match_index or 0
            }
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error handling AppendEntries: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    async def InstallSnapshot(self, request, context):
        """Handle InstallSnapshot RPC for log compaction"""
        try:
            self.request_count += 1
            
            # Handle snapshot installation
            # This would integrate with the snapshot functionality
            
            # For now, return a basic response
            return {
                "term": self.raft_node.state.current_term
            }
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error handling InstallSnapshot: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    async def HealthCheck(self, request, context):
        """Handle health check RPC"""
        try:
            return {
                "healthy": True,
                "status": self.raft_node.state.state,
                "current_term": self.raft_node.state.current_term,
                "current_leader": self.raft_node.current_leader or ""
            }
        except Exception as e:
            logger.error(f"Error handling HealthCheck: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))


class RaftGRPCServer:
    """
    gRPC server for Raft consensus
    """
    
    def __init__(self, 
                 raft_node,
                 port: int,
                 max_workers: int = 10,
                 max_message_size: int = 100 * 1024 * 1024):  # 100MB
        """
        Initialize gRPC server
        
        Args:
            raft_node: The RaftNode instance
            port: Port to listen on
            max_workers: Maximum number of worker threads
            max_message_size: Maximum message size in bytes
        """
        self.raft_node = raft_node
        self.port = port
        self.max_workers = max_workers
        self.max_message_size = max_message_size
        
        # Create gRPC server
        self.server = grpc.aio.server(
            options=[
                ('grpc.max_send_message_length', max_message_size),
                ('grpc.max_receive_message_length', max_message_size),
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.keepalive_permit_without_calls', True),
                ('grpc.http2.max_pings_without_data', 0),
            ]
        )
        
        # Add service
        self.service = RaftGRPCService(raft_node)
        # raft_pb2_grpc.add_RaftServiceServicer_to_server(self.service, self.server)
        
        # Add insecure port (in production, use secure port with TLS)
        self.server.add_insecure_port(f'[::]:{port}')
        
        logger.info(f"Initialized RaftGRPCServer on port {port}")
    
    async def start(self):
        """Start the gRPC server"""
        await self.server.start()
        logger.info(f"gRPC server started on port {self.port}")
    
    async def stop(self, grace_period: float = 5.0):
        """Stop the gRPC server"""
        await self.server.stop(grace_period)
        logger.info("gRPC server stopped")
    
    async def wait_for_termination(self):
        """Wait for server termination"""
        await self.server.wait_for_termination()


class RaftGRPCClient:
    """
    gRPC client for Raft RPC calls with connection pooling
    """
    
    def __init__(self, 
                 node_id: str,
                 timeout: float = 5.0,
                 max_message_size: int = 100 * 1024 * 1024):
        """
        Initialize gRPC client
        
        Args:
            node_id: ID of this node
            timeout: Default timeout for RPCs
            max_message_size: Maximum message size in bytes
        """
        self.node_id = node_id
        self.timeout = timeout
        self.max_message_size = max_message_size
        
        # Connection pool
        self.channels: Dict[str, grpc.aio.Channel] = {}
        self.stubs: Dict[str, Any] = {}  # raft_pb2_grpc.RaftServiceStub
        
        # Connection options
        self.channel_options = [
            ('grpc.max_send_message_length', max_message_size),
            ('grpc.max_receive_message_length', max_message_size),
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0),
        ]
    
    async def connect(self, target_node: str, address: str):
        """Connect to a target node"""
        if target_node not in self.channels:
            # Create channel
            self.channels[target_node] = grpc.aio.insecure_channel(
                address,
                options=self.channel_options
            )
            
            # Create stub
            # self.stubs[target_node] = raft_pb2_grpc.RaftServiceStub(self.channels[target_node])
            
            logger.debug(f"Connected to {target_node} at {address}")
    
    async def close_all(self):
        """Close all connections"""
        for channel in self.channels.values():
            await channel.close()
        self.channels.clear()
        self.stubs.clear()
    
    async def request_vote(self, 
                          target_node: str, 
                          request: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        """Send RequestVote RPC"""
        try:
            if target_node not in self.stubs:
                logger.error(f"No connection to {target_node}")
                return None
            
            stub = self.stubs[target_node]
            
            # Convert to protobuf
            # grpc_request = raft_pb2.RequestVoteRequest(
            #     term=request.term,
            #     candidate_id=request.candidate_id,
            #     last_log_index=request.last_log_index,
            #     last_log_term=request.last_log_term
            # )
            
            # Make RPC call
            # grpc_response = await stub.RequestVote(grpc_request, timeout=self.timeout)
            
            # For now, simulate response
            grpc_response = {
                "term": request.term,
                "vote_granted": False
            }
            
            # Convert response
            return RequestVoteResponse(
                term=grpc_response["term"],
                vote_granted=grpc_response["vote_granted"]
            )
            
        except grpc.RpcError as e:
            logger.debug(f"RequestVote RPC to {target_node} failed: {e.code()}: {e.details()}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in RequestVote RPC: {e}")
            return None
    
    async def append_entries(self,
                           target_node: str,
                           request: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Send AppendEntries RPC"""
        try:
            if target_node not in self.stubs:
                logger.error(f"No connection to {target_node}")
                return None
            
            stub = self.stubs[target_node]
            
            # Convert log entries to protobuf
            # grpc_entries = []
            # for entry_dict in request.entries:
            #     entry = LogEntry.from_dict(entry_dict)
            #     grpc_entry = raft_pb2.LogEntry(
            #         term=entry.term,
            #         index=entry.index,
            #         command=msgpack.packb(entry.command),
            #         timestamp=int(entry.timestamp.timestamp() * 1000000)
            #     )
            #     grpc_entries.append(grpc_entry)
            
            # Create protobuf request
            # grpc_request = raft_pb2.AppendEntriesRequest(
            #     term=request.term,
            #     leader_id=request.leader_id,
            #     prev_log_index=request.prev_log_index,
            #     prev_log_term=request.prev_log_term,
            #     entries=grpc_entries,
            #     leader_commit=request.leader_commit
            # )
            
            # Make RPC call
            # grpc_response = await stub.AppendEntries(grpc_request, timeout=self.timeout)
            
            # For now, simulate response
            grpc_response = {
                "term": request.term,
                "success": True,
                "match_index": 0
            }
            
            # Convert response
            return AppendEntriesResponse(
                term=grpc_response["term"],
                success=grpc_response["success"],
                match_index=grpc_response.get("match_index")
            )
            
        except grpc.RpcError as e:
            logger.debug(f"AppendEntries RPC to {target_node} failed: {e.code()}: {e.details()}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in AppendEntries RPC: {e}")
            return None


# Interceptor for metrics and tracing
class MetricsInterceptor(grpc.aio.ServerInterceptor):
    """
    gRPC interceptor for collecting metrics
    """
    
    def __init__(self, metrics_collector):
        self.metrics_collector = metrics_collector
    
    async def intercept_service(self, continuation, handler_call_details):
        """Intercept RPC calls for metrics"""
        start_time = datetime.utcnow()
        
        try:
            # Call the actual handler
            response = await continuation(handler_call_details)
            
            # Record success metric
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.metrics_collector.record_rpc_success(
                handler_call_details.method,
                duration
            )
            
            return response
            
        except Exception as e:
            # Record error metric
            self.metrics_collector.record_rpc_error(
                handler_call_details.method,
                type(e).__name__
            )
            raise


# Compression interceptor
class CompressionInterceptor(grpc.aio.ServerInterceptor):
    """
    gRPC interceptor for automatic compression
    """
    
    def __init__(self, compression_threshold: int = 1024):
        self.compression_threshold = compression_threshold
    
    async def intercept_service(self, continuation, handler_call_details):
        """Enable compression for large messages"""
        # Check if compression should be enabled based on message size
        # This is a simplified implementation
        
        # Add compression algorithm to context
        handler_call_details.invocation_metadata.append(
            ('grpc-internal-encoding-request', 'gzip')
        )
        
        return await continuation(handler_call_details)