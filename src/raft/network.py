"""
Network Communication Layer for Raft Consensus
Provides robust network communication with connection pooling, retries, and partition detection
"""

import asyncio
import logging
import time
import random
from typing import Dict, Any, Optional, List, Callable, Set, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict, deque
import httpx
import grpc
import grpc.aio
from concurrent import futures
import lz4.frame
import msgpack

from .rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse
)

logger = logging.getLogger(__name__)


@dataclass
class NetworkMetrics:
    """Network performance metrics"""
    bytes_sent: int = 0
    bytes_received: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    compression_ratio: float = 1.0
    latency_samples: deque = None
    
    def __post_init__(self):
        if self.latency_samples is None:
            self.latency_samples = deque(maxlen=1000)


class NetworkPartitionDetector:
    """
    Detects network partitions by monitoring communication patterns
    """
    
    def __init__(self, 
                 node_id: str,
                 cluster_size: int,
                 partition_threshold: float = 0.5,
                 detection_window: float = 30.0):
        """
        Initialize partition detector
        
        Args:
            node_id: ID of this node
            cluster_size: Total number of nodes in cluster
            partition_threshold: Fraction of unreachable nodes to declare partition
            detection_window: Time window for partition detection (seconds)
        """
        self.node_id = node_id
        self.cluster_size = cluster_size
        self.partition_threshold = partition_threshold
        self.detection_window = detection_window
        
        # Track node reachability
        self.node_reachability: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.last_contact: Dict[str, datetime] = {}
        
        # Partition state
        self.is_partitioned = False
        self.partition_start_time: Optional[datetime] = None
        self.reachable_nodes: Set[str] = set()
        self.unreachable_nodes: Set[str] = set()
    
    def record_success(self, node_id: str):
        """Record successful communication with a node"""
        self.node_reachability[node_id].append((datetime.utcnow(), True))
        self.last_contact[node_id] = datetime.utcnow()
    
    def record_failure(self, node_id: str):
        """Record failed communication with a node"""
        self.node_reachability[node_id].append((datetime.utcnow(), False))
    
    def check_partition(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if we're in a network partition
        
        Returns:
            Tuple of (is_partitioned, partition_info)
        """
        now = datetime.utcnow()
        window_start = now - timedelta(seconds=self.detection_window)
        
        # Calculate reachability for each node
        reachable = set()
        unreachable = set()
        
        for node_id, history in self.node_reachability.items():
            # Get recent communication attempts
            recent = [(t, success) for t, success in history if t >= window_start]
            
            if not recent:
                # No recent communication attempts
                if node_id in self.last_contact:
                    time_since_contact = (now - self.last_contact[node_id]).total_seconds()
                    if time_since_contact > self.detection_window:
                        unreachable.add(node_id)
                continue
            
            # Calculate success rate
            successes = sum(1 for _, success in recent if success)
            success_rate = successes / len(recent)
            
            if success_rate > 0.5:
                reachable.add(node_id)
            else:
                unreachable.add(node_id)
        
        # Check if we're partitioned
        unreachable_fraction = len(unreachable) / (self.cluster_size - 1)  # Exclude self
        newly_partitioned = unreachable_fraction >= self.partition_threshold
        
        # Update partition state
        if newly_partitioned and not self.is_partitioned:
            self.is_partitioned = True
            self.partition_start_time = now
            logger.warning(f"Network partition detected! Unreachable nodes: {unreachable}")
        elif not newly_partitioned and self.is_partitioned:
            self.is_partitioned = False
            partition_duration = (now - self.partition_start_time).total_seconds()
            logger.info(f"Network partition resolved after {partition_duration:.1f}s")
            self.partition_start_time = None
        
        self.reachable_nodes = reachable
        self.unreachable_nodes = unreachable
        
        return self.is_partitioned, {
            "is_partitioned": self.is_partitioned,
            "reachable_nodes": list(reachable),
            "unreachable_nodes": list(unreachable),
            "partition_start_time": self.partition_start_time.isoformat() if self.partition_start_time else None,
            "reachable_fraction": len(reachable) / (self.cluster_size - 1) if self.cluster_size > 1 else 1.0
        }


class ConnectionPool:
    """
    Connection pool for HTTP/gRPC connections with health monitoring
    """
    
    def __init__(self, 
                 node_id: str,
                 max_connections: int = 10,
                 connection_timeout: float = 5.0,
                 use_grpc: bool = False):
        self.node_id = node_id
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.use_grpc = use_grpc
        
        # Connection pools
        self.http_clients: Dict[str, httpx.AsyncClient] = {}
        self.grpc_channels: Dict[str, grpc.aio.Channel] = {}
        
        # Connection health
        self.connection_health: Dict[str, bool] = {}
        self.last_health_check: Dict[str, datetime] = {}
        
        # Metrics
        self.metrics = defaultdict(NetworkMetrics)
    
    async def get_connection(self, target_node: str, address: str):
        """Get a connection to target node"""
        if self.use_grpc:
            return await self._get_grpc_connection(target_node, address)
        else:
            return await self._get_http_connection(target_node, address)
    
    async def _get_http_connection(self, target_node: str, address: str) -> httpx.AsyncClient:
        """Get HTTP connection from pool"""
        if target_node not in self.http_clients:
            self.http_clients[target_node] = httpx.AsyncClient(
                base_url=f"http://{address}",
                timeout=self.connection_timeout,
                limits=httpx.Limits(max_connections=self.max_connections)
            )
        return self.http_clients[target_node]
    
    async def _get_grpc_connection(self, target_node: str, address: str) -> grpc.aio.Channel:
        """Get gRPC connection from pool"""
        if target_node not in self.grpc_channels:
            self.grpc_channels[target_node] = grpc.aio.insecure_channel(
                address,
                options=[
                    ('grpc.max_send_message_length', 100 * 1024 * 1024),  # 100MB
                    ('grpc.max_receive_message_length', 100 * 1024 * 1024),
                    ('grpc.keepalive_time_ms', 10000),
                    ('grpc.keepalive_timeout_ms', 5000),
                    ('grpc.keepalive_permit_without_calls', True),
                    ('grpc.http2.max_pings_without_data', 0),
                ]
            )
        return self.grpc_channels[target_node]
    
    async def check_connection_health(self, target_node: str) -> bool:
        """Check if connection to target node is healthy"""
        try:
            if self.use_grpc and target_node in self.grpc_channels:
                channel = self.grpc_channels[target_node]
                state = channel.get_state()
                return state == grpc.ChannelConnectivity.READY
            elif target_node in self.http_clients:
                client = self.http_clients[target_node]
                response = await client.get("/health", timeout=2.0)
                return response.status_code == 200
            return False
        except Exception:
            return False
    
    async def close_all(self):
        """Close all connections"""
        # Close HTTP clients
        for client in self.http_clients.values():
            await client.aclose()
        self.http_clients.clear()
        
        # Close gRPC channels
        for channel in self.grpc_channels.values():
            await channel.close()
        self.grpc_channels.clear()
    
    def record_metrics(self, target_node: str, bytes_sent: int, bytes_received: int, latency: float):
        """Record connection metrics"""
        metrics = self.metrics[target_node]
        metrics.bytes_sent += bytes_sent
        metrics.bytes_received += bytes_received
        metrics.messages_sent += 1
        metrics.messages_received += 1
        metrics.latency_samples.append(latency)


class MessageCompressor:
    """
    Handles message compression for large payloads
    """
    
    def __init__(self, compression_threshold: int = 1024):  # 1KB
        self.compression_threshold = compression_threshold
        self.compressor = lz4.frame
    
    def compress(self, data: bytes) -> Tuple[bytes, bool]:
        """
        Compress data if it exceeds threshold
        
        Returns:
            Tuple of (compressed_data, was_compressed)
        """
        if len(data) < self.compression_threshold:
            return data, False
        
        compressed = self.compressor.compress(data)
        
        # Only use compression if it actually reduces size
        if len(compressed) < len(data) * 0.9:  # At least 10% reduction
            return compressed, True
        
        return data, False
    
    def decompress(self, data: bytes, was_compressed: bool) -> bytes:
        """Decompress data if it was compressed"""
        if was_compressed:
            return self.compressor.decompress(data)
        return data


class RaftNetworkTransport:
    """
    Main network transport layer for Raft with advanced features
    """
    
    def __init__(self,
                 node_id: str,
                 cluster_config: Dict[str, str],
                 use_grpc: bool = False,
                 enable_compression: bool = True,
                 enable_pipelining: bool = True,
                 max_batch_size: int = 100,
                 pipeline_depth: int = 10):
        """
        Initialize network transport
        
        Args:
            node_id: ID of this node
            cluster_config: Cluster configuration
            use_grpc: Use gRPC instead of HTTP
            enable_compression: Enable message compression
            enable_pipelining: Enable pipeline replication
            max_batch_size: Maximum batch size for entries
            pipeline_depth: Maximum pipeline depth
        """
        self.node_id = node_id
        self.cluster_config = cluster_config
        self.use_grpc = use_grpc
        self.enable_compression = enable_compression
        self.enable_pipelining = enable_pipelining
        self.max_batch_size = max_batch_size
        self.pipeline_depth = pipeline_depth
        
        # Initialize components
        self.connection_pool = ConnectionPool(node_id, use_grpc=use_grpc)
        self.partition_detector = NetworkPartitionDetector(node_id, len(cluster_config))
        self.compressor = MessageCompressor() if enable_compression else None
        
        # Pipeline tracking
        self.pipeline_tracker: Dict[str, List[asyncio.Task]] = defaultdict(list)
        self.pipeline_semaphores: Dict[str, asyncio.Semaphore] = {}
        
        # Retry configuration
        self.max_retries = 3
        self.base_retry_delay = 0.1
        self.max_retry_delay = 5.0
        
        # Callbacks
        self.partition_callback: Optional[Callable] = None
        
        logger.info(f"Initialized RaftNetworkTransport for {node_id} with "
                   f"{'gRPC' if use_grpc else 'HTTP'} transport")
    
    async def initialize(self):
        """Initialize network transport"""
        # Initialize pipeline semaphores
        for node_id in self.cluster_config:
            if node_id != self.node_id:
                self.pipeline_semaphores[node_id] = asyncio.Semaphore(self.pipeline_depth)
    
    async def close(self):
        """Close network transport"""
        await self.connection_pool.close_all()
    
    async def send_request_vote(self, 
                               target_node: str, 
                               request: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        """Send RequestVote RPC with retry and metrics"""
        return await self._send_with_retry(
            target_node,
            "request_vote",
            request,
            RequestVoteResponse
        )
    
    async def send_append_entries(self,
                                 target_node: str,
                                 request: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Send AppendEntries RPC with retry and metrics"""
        # Use pipelining if enabled and we have entries
        if self.enable_pipelining and request.entries:
            return await self._send_pipelined(target_node, request)
        else:
            return await self._send_with_retry(
                target_node,
                "append_entries",
                request,
                AppendEntriesResponse
            )
    
    async def _send_with_retry(self,
                              target_node: str,
                              rpc_type: str,
                              request: Any,
                              response_class: type) -> Optional[Any]:
        """Send RPC with exponential backoff retry"""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                # Get connection
                if target_node not in self.cluster_config:
                    logger.error(f"Unknown target node: {target_node}")
                    return None
                
                address = self.cluster_config[target_node]
                connection = await self.connection_pool.get_connection(target_node, address)
                
                # Serialize request
                if self.use_grpc:
                    # gRPC serialization would go here
                    raise NotImplementedError("gRPC support not yet implemented")
                else:
                    request_data = msgpack.packb(request.__dict__)
                
                # Compress if enabled
                if self.compressor:
                    request_data, was_compressed = self.compressor.compress(request_data)
                else:
                    was_compressed = False
                
                # Send request
                start_time = time.time()
                
                if isinstance(connection, httpx.AsyncClient):
                    headers = {"X-Compressed": str(was_compressed)}
                    response = await connection.post(
                        f"/raft/{rpc_type}",
                        content=request_data,
                        headers=headers
                    )
                    
                    if response.status_code == 200:
                        # Decompress response if needed
                        response_data = response.content
                        if response.headers.get("X-Compressed") == "True":
                            response_data = self.compressor.decompress(response_data, True)
                        
                        # Deserialize response
                        response_dict = msgpack.unpackb(response_data)
                        result = response_class(**response_dict)
                        
                        # Record metrics
                        latency = time.time() - start_time
                        self.connection_pool.record_metrics(
                            target_node,
                            len(request_data),
                            len(response_data),
                            latency
                        )
                        
                        # Record success
                        self.partition_detector.record_success(target_node)
                        
                        return result
                
                # Request failed
                self.partition_detector.record_failure(target_node)
                
            except Exception as e:
                last_exception = e
                self.partition_detector.record_failure(target_node)
                
                # Calculate retry delay with jitter
                delay = min(
                    self.base_retry_delay * (2 ** attempt) + random.uniform(0, 0.1),
                    self.max_retry_delay
                )
                
                if attempt < self.max_retries - 1:
                    logger.debug(f"RPC to {target_node} failed (attempt {attempt + 1}): {e}. "
                               f"Retrying in {delay:.2f}s")
                    await asyncio.sleep(delay)
        
        logger.warning(f"RPC to {target_node} failed after {self.max_retries} attempts: {last_exception}")
        
        # Check for network partition
        is_partitioned, partition_info = self.partition_detector.check_partition()
        if is_partitioned and self.partition_callback:
            await self.partition_callback(partition_info)
        
        return None
    
    async def _send_pipelined(self,
                            target_node: str,
                            request: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Send AppendEntries with pipelining"""
        # Acquire pipeline slot
        semaphore = self.pipeline_semaphores[target_node]
        
        async with semaphore:
            # Send request without waiting for response
            task = asyncio.create_task(
                self._send_with_retry(target_node, "append_entries", request, AppendEntriesResponse)
            )
            
            # Track pipeline task
            self.pipeline_tracker[target_node].append(task)
            
            # Clean up completed tasks
            self.pipeline_tracker[target_node] = [
                t for t in self.pipeline_tracker[target_node] if not t.done()
            ]
            
            # Wait for this specific task
            return await task
    
    def get_network_stats(self) -> Dict[str, Any]:
        """Get network statistics"""
        stats = {
            "node_id": self.node_id,
            "transport": "grpc" if self.use_grpc else "http",
            "compression_enabled": self.enable_compression,
            "pipelining_enabled": self.enable_pipelining,
            "connections": {}
        }
        
        # Add per-node statistics
        for node_id, metrics in self.connection_pool.metrics.items():
            if metrics.latency_samples:
                latencies = list(metrics.latency_samples)
                stats["connections"][node_id] = {
                    "bytes_sent": metrics.bytes_sent,
                    "bytes_received": metrics.bytes_received,
                    "messages_sent": metrics.messages_sent,
                    "messages_received": metrics.messages_received,
                    "avg_latency": sum(latencies) / len(latencies),
                    "min_latency": min(latencies),
                    "max_latency": max(latencies),
                    "pipeline_depth": len(self.pipeline_tracker.get(node_id, []))
                }
        
        # Add partition detection info
        is_partitioned, partition_info = self.partition_detector.check_partition()
        stats["partition_info"] = partition_info
        
        return stats


class AdaptiveTimeout:
    """
    Adaptive timeout mechanism based on network conditions
    """
    
    def __init__(self, 
                 min_timeout: float = 0.1,
                 max_timeout: float = 5.0,
                 target_success_rate: float = 0.95):
        self.min_timeout = min_timeout
        self.max_timeout = max_timeout
        self.target_success_rate = target_success_rate
        
        # Track success rates
        self.success_window = deque(maxlen=100)
        self.current_timeout = min_timeout
        
        # Adjustment parameters
        self.increase_factor = 1.5
        self.decrease_factor = 0.9
    
    def record_result(self, success: bool, actual_time: float):
        """Record RPC result"""
        self.success_window.append((success, actual_time))
        
        # Adjust timeout based on success rate
        if len(self.success_window) >= 10:
            success_rate = sum(1 for s, _ in self.success_window if s) / len(self.success_window)
            
            if success_rate < self.target_success_rate:
                # Increase timeout
                self.current_timeout = min(
                    self.current_timeout * self.increase_factor,
                    self.max_timeout
                )
            elif success_rate > self.target_success_rate + 0.02:
                # Decrease timeout if we're doing well
                avg_time = sum(t for s, t in self.success_window if s) / sum(1 for s, _ in self.success_window if s)
                if avg_time < self.current_timeout * 0.8:
                    self.current_timeout = max(
                        self.current_timeout * self.decrease_factor,
                        self.min_timeout
                    )
    
    def get_timeout(self) -> float:
        """Get current timeout value"""
        # Add jitter to prevent synchronized timeouts
        jitter = random.uniform(-0.1, 0.1) * self.current_timeout
        return max(self.min_timeout, self.current_timeout + jitter)


# Network message batching for improved throughput
class MessageBatcher:
    """
    Batches multiple small messages for improved network efficiency
    """
    
    def __init__(self, 
                 max_batch_size: int = 100,
                 max_batch_bytes: int = 1024 * 1024,  # 1MB
                 batch_timeout: float = 0.01):  # 10ms
        self.max_batch_size = max_batch_size
        self.max_batch_bytes = max_batch_bytes
        self.batch_timeout = batch_timeout
        
        # Pending batches per node
        self.pending_batches: Dict[str, List[Any]] = defaultdict(list)
        self.batch_sizes: Dict[str, int] = defaultdict(int)
        self.batch_timers: Dict[str, asyncio.Task] = {}
        
        # Callbacks
        self.send_callback: Optional[Callable] = None
    
    async def add_message(self, target_node: str, message: Any, size_bytes: int):
        """Add message to batch"""
        self.pending_batches[target_node].append(message)
        self.batch_sizes[target_node] += size_bytes
        
        # Check if we should send immediately
        if (len(self.pending_batches[target_node]) >= self.max_batch_size or
            self.batch_sizes[target_node] >= self.max_batch_bytes):
            await self._send_batch(target_node)
        elif target_node not in self.batch_timers:
            # Start batch timer
            self.batch_timers[target_node] = asyncio.create_task(
                self._batch_timeout(target_node)
            )
    
    async def _batch_timeout(self, target_node: str):
        """Handle batch timeout"""
        await asyncio.sleep(self.batch_timeout)
        await self._send_batch(target_node)
    
    async def _send_batch(self, target_node: str):
        """Send batched messages"""
        if target_node in self.batch_timers:
            self.batch_timers[target_node].cancel()
            del self.batch_timers[target_node]
        
        if self.pending_batches[target_node] and self.send_callback:
            batch = self.pending_batches[target_node]
            self.pending_batches[target_node] = []
            self.batch_sizes[target_node] = 0
            
            await self.send_callback(target_node, batch)
    
    async def flush_all(self):
        """Flush all pending batches"""
        nodes = list(self.pending_batches.keys())
        for node in nodes:
            if self.pending_batches[node]:
                await self._send_batch(node)