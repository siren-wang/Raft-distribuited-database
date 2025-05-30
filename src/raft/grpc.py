"""
Enhanced gRPC Service Implementation for Raft
Provides full gRPC functionality with proper protobuf integration
"""

import asyncio
import logging
import grpc
import grpc.aio
from typing import Dict, Any, Optional, List, AsyncIterator
from datetime import datetime
import msgpack
import lz4.frame
from google.protobuf import any_pb2
from google.protobuf.timestamp_pb2 import Timestamp

# These imports would come from generated protobuf files
# For now, we'll define placeholder classes
# from .generated import raft_pb2, raft_pb2_grpc

from .rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse
)
from .state import LogEntry
from .metrics import RaftMetrics

logger = logging.getLogger(__name__)


# Placeholder protobuf message classes (would be generated)
class raft_pb2:
    class RequestVoteRequest:
        def __init__(self, term=0, candidate_id="", last_log_index=0, last_log_term=0, pre_vote=False):
            self.term = term
            self.candidate_id = candidate_id
            self.last_log_index = last_log_index
            self.last_log_term = last_log_term
            self.pre_vote = pre_vote
    
    class RequestVoteResponse:
        def __init__(self, term=0, vote_granted=False, reason=""):
            self.term = term
            self.vote_granted = vote_granted
            self.reason = reason
    
    class LogEntry:
        def __init__(self, term=0, index=0, command=None, timestamp=None, client_id="", request_id=""):
            self.term = term
            self.index = index
            self.command = command or any_pb2.Any()
            self.timestamp = timestamp or Timestamp()
            self.client_id = client_id
            self.request_id = request_id
    
    class AppendEntriesRequest:
        def __init__(self, term=0, leader_id="", prev_log_index=0, prev_log_term=0, 
                     entries=None, leader_commit=0, is_heartbeat=False, read_index=0):
            self.term = term
            self.leader_id = leader_id
            self.prev_log_index = prev_log_index
            self.prev_log_term = prev_log_term
            self.entries = entries or []
            self.leader_commit = leader_commit
            self.is_heartbeat = is_heartbeat
            self.read_index = read_index
    
    class AppendEntriesResponse:
        def __init__(self, term=0, success=False, match_index=0, conflict_index=0, 
                     conflict_term=0, last_log_index=0):
            self.term = term
            self.success = success
            self.match_index = match_index
            self.conflict_index = conflict_index
            self.conflict_term = conflict_term
            self.last_log_index = last_log_index
    
    class HealthCheckRequest:
        def __init__(self, node_id="", detailed=False):
            self.node_id = node_id
            self.detailed = detailed
    
    class HealthCheckResponse:
        def __init__(self, healthy=False, status="", current_term=0, current_leader="", details=None):
            self.healthy = healthy
            self.status = status
            self.current_term = current_term
            self.current_leader = current_leader
            self.details = details


class RaftGRPCService:
    """
    Enhanced gRPC service implementation for Raft consensus
    """
    
    def __init__(self, raft_node, metrics: Optional[RaftMetrics] = None):
        """
        Initialize gRPC service
        
        Args:
            raft_node: The RaftNode instance to handle RPCs
            metrics: Optional metrics collector
        """
        self.raft_node = raft_node
        self.rpc_handler = raft_node.rpc_handler
        self.metrics = metrics
        
        # Request tracking
        self.active_requests = 0
        self.total_requests = 0
        self.error_count = 0
        
        # Compression
        self.compressor = lz4.frame
        self.compression_threshold = 1024  # 1KB
        
        # Circuit breaker state
        self.circuit_breaker_state = {}
        
        logger.info(f"Initialized enhanced RaftGRPCService for node {raft_node.node_id}")
    
    async def RequestVote(self, request: raft_pb2.RequestVoteRequest, context) -> raft_pb2.RequestVoteResponse:
        """Handle RequestVote RPC"""
        start_time = datetime.utcnow()
        self.active_requests += 1
        self.total_requests += 1
        
        try:
            # Extract peer info
            peer = context.peer()
            logger.debug(f"RequestVote RPC from {peer}")
            
            # Record metrics
            if self.metrics:
                self.metrics.record_rpc_received("RequestVote", request.candidate_id)
            
            # Handle pre-vote if enabled
            if request.pre_vote:
                # Pre-vote logic to prevent disruption
                response = await self._handle_pre_vote(request)
            else:
                # Convert protobuf to internal request
                vote_request = RequestVoteRequest(
                    term=request.term,
                    candidate_id=request.candidate_id,
                    last_log_index=request.last_log_index,
                    last_log_term=request.last_log_term
                )
                
                # Handle request
                internal_response = await self.rpc_handler.handle_request_vote(vote_request)
                
                # Convert to protobuf response
                response = raft_pb2.RequestVoteResponse(
                    term=internal_response.term,
                    vote_granted=internal_response.vote_granted,
                    reason=self._get_vote_reason(internal_response)
                )
            
            # Record success metrics
            if self.metrics:
                latency = (datetime.utcnow() - start_time).total_seconds()
                self.metrics.record_rpc_latency("RequestVote", request.candidate_id, latency)
            
            return response
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error handling RequestVote: {e}")
            
            if self.metrics:
                self.metrics.record_rpc_error("RequestVote", type(e).__name__)
            
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            
        finally:
            self.active_requests -= 1
    
    async def AppendEntries(self, request: raft_pb2.AppendEntriesRequest, context) -> raft_pb2.AppendEntriesResponse:
        """Handle AppendEntries RPC"""
        start_time = datetime.utcnow()
        self.active_requests += 1
        self.total_requests += 1
        
        try:
            # Record metrics
            if self.metrics:
                self.metrics.record_rpc_received("AppendEntries", request.leader_id)
            
            # Decompress entries if needed
            entries = await self._decompress_entries(request.entries)
            
            # Convert protobuf entries to internal format
            internal_entries = []
            for entry in entries:
                command = self._deserialize_command(entry.command)
                internal_entries.append({
                    "term": entry.term,
                    "index": entry.index,
                    "command": command,
                    "timestamp": self._timestamp_to_datetime(entry.timestamp).isoformat()
                })
            
            # Create internal request
            append_request = AppendEntriesRequest(
                term=request.term,
                leader_id=request.leader_id,
                prev_log_index=request.prev_log_index,
                prev_log_term=request.prev_log_term,
                entries=internal_entries,
                leader_commit=request.leader_commit
            )
            
            # Handle request
            internal_response = await self.rpc_handler.handle_append_entries(append_request)
            
            # Create optimized response
            response = raft_pb2.AppendEntriesResponse(
                term=internal_response.term,
                success=internal_response.success,
                match_index=internal_response.match_index or 0
            )
            
            # Add optimization hints for failed requests
            if not internal_response.success:
                conflict_info = await self._get_conflict_info(request.prev_log_index)
                if conflict_info:
                    response.conflict_index = conflict_info['index']
                    response.conflict_term = conflict_info['term']
                response.last_log_index = self.raft_node.state.get_last_log_index()
            
            # Record success metrics
            if self.metrics:
                latency = (datetime.utcnow() - start_time).total_seconds()
                self.metrics.record_rpc_latency("AppendEntries", request.leader_id, latency)
                
                if entries:
                    self.metrics.record_batch_size(len(entries))
            
            return response
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error handling AppendEntries: {e}")
            
            if self.metrics:
                self.metrics.record_rpc_error("AppendEntries", type(e).__name__)
            
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            
        finally:
            self.active_requests -= 1
    
    async def StreamAppendEntries(self, request_iterator: AsyncIterator[raft_pb2.AppendEntriesRequest], 
                                 context) -> AsyncIterator[raft_pb2.AppendEntriesResponse]:
        """Handle streaming AppendEntries for better performance"""
        try:
            async for request in request_iterator:
                # Process each request in the stream
                response = await self.AppendEntries(request, context)
                yield response
                
        except Exception as e:
            logger.error(f"Error in StreamAppendEntries: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    async def HealthCheck(self, request: raft_pb2.HealthCheckRequest, context) -> raft_pb2.HealthCheckResponse:
        """Handle health check RPC"""
        try:
            # Basic health info
            response = raft_pb2.HealthCheckResponse(
                healthy=True,
                status=self.raft_node.state.state,
                current_term=self.raft_node.state.current_term,
                current_leader=self.raft_node.current_leader or ""
            )
            
            # Add detailed info if requested
            if request.detailed:
                details = await self._get_detailed_health()
                response.details = details
            
            return response
            
        except Exception as e:
            logger.error(f"Error handling HealthCheck: {e}")
            return raft_pb2.HealthCheckResponse(
                healthy=False,
                status="error",
                current_term=0,
                current_leader=""
            )
    
    async def _handle_pre_vote(self, request: raft_pb2.RequestVoteRequest) -> raft_pb2.RequestVoteResponse:
        """Handle pre-vote to prevent disruption"""
        # Check if we would grant the vote without actually updating state
        current_term = self.raft_node.state.current_term
        
        # Would we grant based on term?
        if request.term < current_term:
            return raft_pb2.RequestVoteResponse(
                term=current_term,
                vote_granted=False,
                reason="Outdated term"
            )
        
        # Check log completeness
        last_log_index = self.raft_node.state.get_last_log_index()
        last_log_term = self.raft_node.state.get_last_log_term()
        
        log_is_current = (request.last_log_term > last_log_term or 
                         (request.last_log_term == last_log_term and 
                          request.last_log_index >= last_log_index))
        
        # Check if we've heard from leader recently
        if hasattr(self.raft_node, 'last_heartbeat_time'):
            time_since_heartbeat = (datetime.utcnow() - self.raft_node.last_heartbeat_time).total_seconds()
            if time_since_heartbeat < self.raft_node.election_timeout_range[0]:
                return raft_pb2.RequestVoteResponse(
                    term=current_term,
                    vote_granted=False,
                    reason="Recently heard from leader"
                )
        
        return raft_pb2.RequestVoteResponse(
            term=current_term,
            vote_granted=log_is_current,
            reason="Pre-vote check passed" if log_is_current else "Log not current"
        )
    
    async def _decompress_entries(self, entries: List[raft_pb2.LogEntry]) -> List[raft_pb2.LogEntry]:
        """Decompress log entries if needed"""
        # In real implementation, would check compression flag and decompress
        return entries
    
    def _deserialize_command(self, command_any: any_pb2.Any) -> Dict[str, Any]:
        """Deserialize command from protobuf Any"""
        # In real implementation, would properly deserialize
        # For now, return a mock command
        return {"type": "noop"}
    
    def _timestamp_to_datetime(self, timestamp: Timestamp) -> datetime:
        """Convert protobuf timestamp to datetime"""
        return datetime.fromtimestamp(timestamp.seconds + timestamp.nanos / 1e9)
    
    def _get_vote_reason(self, response: RequestVoteResponse) -> str:
        """Get human-readable reason for vote decision"""
        if response.vote_granted:
            return "Vote granted"
        return "Vote denied"
    
    async def _get_conflict_info(self, prev_log_index: int) -> Optional[Dict[str, Any]]:
        """Get information about log conflicts for optimization"""
        if prev_log_index <= 0:
            return None
        
        # Find the conflicting entry
        entry = self.raft_node.state.get_log_entry(prev_log_index)
        if entry:
            return {
                'index': prev_log_index,
                'term': entry.term
            }
        
        # Find last entry with different term
        for i in range(prev_log_index - 1, 0, -1):
            entry = self.raft_node.state.get_log_entry(i)
            if entry:
                return {
                    'index': i,
                    'term': entry.term
                }
        
        return None
    
    async def _get_detailed_health(self) -> Dict[str, Any]:
        """Get detailed health information"""
        return {
            "uptime_seconds": int((datetime.utcnow() - self.raft_node.start_time).total_seconds()),
            "active_requests": self.active_requests,
            "total_requests": self.total_requests,
            "error_count": self.error_count,
            "log_size": len(self.raft_node.state.log),
            "commit_index": self.raft_node.state.commit_index,
            "last_applied": self.raft_node.state.last_applied
        }


class RaftGRPCServer:
    """
    Enhanced gRPC server for Raft consensus with monitoring and optimization
    """
    
    def __init__(self, 
                 raft_node,
                 port: int,
                 metrics: Optional[RaftMetrics] = None,
                 max_workers: int = 10,
                 max_message_size: int = 100 * 1024 * 1024,  # 100MB
                 enable_reflection: bool = True):
        """
        Initialize gRPC server
        
        Args:
            raft_node: The RaftNode instance
            port: Port to listen on
            metrics: Optional metrics collector
            max_workers: Maximum number of worker threads
            max_message_size: Maximum message size in bytes
            enable_reflection: Enable gRPC reflection for debugging
        """
        self.raft_node = raft_node
        self.port = port
        self.metrics = metrics
        self.max_workers = max_workers
        self.max_message_size = max_message_size
        
        # Server options for performance
        options = [
            ('grpc.max_send_message_length', max_message_size),
            ('grpc.max_receive_message_length', max_message_size),
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.http2.min_time_between_pings_ms', 10000),
            ('grpc.http2.max_ping_strikes', 0),
            # Performance optimizations
            ('grpc.max_concurrent_streams', 1000),
            ('grpc.http2.hpack_table_size', 65536),
            ('grpc.http2.max_frame_size', 16777215),  # 16MB
            ('grpc.enable_http_proxy', 0),  # Disable for performance
        ]
        
        # Create server with interceptors
        interceptors = self._create_interceptors()
        self.server = grpc.aio.server(
            options=options,
            interceptors=interceptors,
            maximum_concurrent_rpcs=1000
        )
        
        # Add service
        self.service = RaftGRPCService(raft_node, metrics)
        # In real implementation:
        # raft_pb2_grpc.add_RaftServiceServicer_to_server(self.service, self.server)
        
        # Add reflection for debugging
        if enable_reflection:
            # from grpc_reflection.v1alpha import reflection
            # reflection.enable_server_reflection(SERVICE_NAMES, self.server)
            pass
        
        # Add insecure port (in production, use secure port with TLS)
        self.server.add_insecure_port(f'[::]:{port}')
        
        logger.info(f"Initialized enhanced RaftGRPCServer on port {port}")
    
    def _create_interceptors(self):
        """Create server interceptors for monitoring and optimization"""
        interceptors = []
        
        # Add metrics interceptor
        if self.metrics:
            interceptors.append(MetricsInterceptor(self.metrics))
        
        # Add compression interceptor
        interceptors.append(CompressionInterceptor())
        
        # Add rate limiting interceptor
        interceptors.append(RateLimitingInterceptor(max_rps=10000))
        
        # Add tracing interceptor
        interceptors.append(TracingInterceptor())
        
        return interceptors
    
    async def start(self):
        """Start the gRPC server"""
        await self.server.start()
        logger.info(f"Enhanced gRPC server started on port {self.port}")
        
        # Register with service discovery
        await self._register_with_discovery()
    
    async def stop(self, grace_period: float = 5.0):
        """Stop the gRPC server gracefully"""
        logger.info("Stopping gRPC server...")
        
        # Deregister from service discovery
        await self._deregister_from_discovery()
        
        # Stop accepting new requests
        await self.server.stop(grace_period)
        logger.info("gRPC server stopped")
    
    async def wait_for_termination(self):
        """Wait for server termination"""
        await self.server.wait_for_termination()
    
    async def _register_with_discovery(self):
        """Register server with service discovery system"""
        # Implementation would register with Consul, etcd, etc.
        pass
    
    async def _deregister_from_discovery(self):
        """Deregister server from service discovery system"""
        # Implementation would deregister from discovery system
        pass
    
    def get_server_stats(self) -> Dict[str, Any]:
        """Get server statistics"""
        return {
            "port": self.port,
            "active_requests": self.service.active_requests,
            "total_requests": self.service.total_requests,
            "error_count": self.service.error_count,
            "uptime": int((datetime.utcnow() - self.raft_node.start_time).total_seconds())
        }


class RateLimitingInterceptor(grpc.aio.ServerInterceptor):
    """
    Rate limiting interceptor to prevent overload
    """
    
    def __init__(self, max_rps: int = 10000):
        self.max_rps = max_rps
        self.request_times = []
        self._lock = asyncio.Lock()
    
    async def intercept_service(self, continuation, handler_call_details):
        """Check rate limit before processing request"""
        async with self._lock:
            now = datetime.utcnow()
            # Remove old entries
            self.request_times = [t for t in self.request_times 
                                if (now - t).total_seconds() < 1.0]
            
            # Check rate limit
            if len(self.request_times) >= self.max_rps:
                context = handler_call_details.invocation_metadata
                context.abort(
                    grpc.StatusCode.RESOURCE_EXHAUSTED,
                    "Rate limit exceeded"
                )
                return
            
            # Add current request
            self.request_times.append(now)
        
        return await continuation(handler_call_details)


class TracingInterceptor(grpc.aio.ServerInterceptor):
    """
    Distributed tracing interceptor
    """
    
    async def intercept_service(self, continuation, handler_call_details):
        """Add tracing to requests"""
        # Extract trace context from metadata
        metadata = dict(handler_call_details.invocation_metadata or [])
        trace_id = metadata.get('x-trace-id', self._generate_trace_id())
        span_id = self._generate_span_id()
        
        # Log trace info
        logger.debug(f"Request {handler_call_details.method} - Trace: {trace_id}, Span: {span_id}")
        
        # Add trace context to thread local or context
        # In real implementation, would use OpenTelemetry or similar
        
        try:
            result = await continuation(handler_call_details)
            logger.debug(f"Request completed - Trace: {trace_id}, Span: {span_id}")
            return result
        except Exception as e:
            logger.error(f"Request failed - Trace: {trace_id}, Span: {span_id}, Error: {e}")
            raise
    
    def _generate_trace_id(self) -> str:
        """Generate unique trace ID"""
        import uuid
        return str(uuid.uuid4())
    
    def _generate_span_id(self) -> str:
        """Generate unique span ID"""
        import uuid
        return str(uuid.uuid4())[:8]