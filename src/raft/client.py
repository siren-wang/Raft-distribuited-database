"""
Client Request Routing and Failover for Raft
Handles leader discovery, request forwarding, and automatic failover
"""

import asyncio
import httpx
import logging
import random
import time
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict
import hashlib

logger = logging.getLogger(__name__)


@dataclass
class NodeEndpoint:
    """Represents a node endpoint with health status"""
    node_id: str
    address: str
    is_leader: bool = False
    last_contact: Optional[datetime] = None
    consecutive_failures: int = 0
    is_healthy: bool = True


class RequestIdempotencyManager:
    """
    Manages request idempotency to prevent duplicate operations
    """
    
    def __init__(self, ttl_seconds: int = 300):
        self.ttl_seconds = ttl_seconds
        self.request_cache: Dict[str, tuple[datetime, Any]] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
    
    async def start(self):
        """Start the cleanup task"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def stop(self):
        """Stop the cleanup task"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
    
    def generate_request_id(self, operation: str, key: str, value: Any = None) -> str:
        """Generate a unique request ID based on operation parameters"""
        content = f"{operation}:{key}:{value}:{time.time()}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    async def check_request(self, request_id: str) -> Optional[Any]:
        """Check if a request has been processed before"""
        async with self._lock:
            if request_id in self.request_cache:
                timestamp, result = self.request_cache[request_id]
                # Check if still valid
                if (datetime.utcnow() - timestamp).total_seconds() < self.ttl_seconds:
                    return result
                else:
                    # Expired, remove it
                    del self.request_cache[request_id]
            return None
    
    async def store_result(self, request_id: str, result: Any):
        """Store the result of a request"""
        async with self._lock:
            self.request_cache[request_id] = (datetime.utcnow(), result)
    
    async def _cleanup_loop(self):
        """Periodically clean up expired entries"""
        while True:
            try:
                await asyncio.sleep(60)  # Run every minute
                await self._cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in idempotency cleanup: {e}")
    
    async def _cleanup_expired(self):
        """Remove expired entries"""
        async with self._lock:
            now = datetime.utcnow()
            expired_keys = [
                key for key, (timestamp, _) in self.request_cache.items()
                if (now - timestamp).total_seconds() > self.ttl_seconds
            ]
            for key in expired_keys:
                del self.request_cache[key]
            
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired idempotency entries")


class RaftClient:
    """
    Smart client for Raft cluster with automatic leader discovery and failover
    """
    
    def __init__(self, 
                 nodes: Dict[str, str],
                 timeout: float = 5.0,
                 max_retries: int = 3,
                 retry_delay: float = 0.5,
                 health_check_interval: float = 5.0):
        """
        Initialize Raft client
        
        Args:
            nodes: Dict mapping node_id to address
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries
            retry_delay: Base delay between retries (with exponential backoff)
            health_check_interval: Interval for health checks
        """
        self.nodes = {
            node_id: NodeEndpoint(node_id, address)
            for node_id, address in nodes.items()
        }
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.health_check_interval = health_check_interval
        
        # HTTP client
        self.client = httpx.AsyncClient(timeout=timeout)
        
        # Leader tracking
        self.current_leader: Optional[str] = None
        self.leader_discovery_in_progress = False
        self._leader_lock = asyncio.Lock()
        
        # Request idempotency
        self.idempotency_manager = RequestIdempotencyManager()
        
        # Health checking
        self._health_check_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Metrics
        self.request_count = 0
        self.retry_count = 0
        self.failover_count = 0
        
        logger.info(f"Initialized RaftClient with {len(nodes)} nodes")
    
    async def start(self):
        """Start the client"""
        self._running = True
        await self.idempotency_manager.start()
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        
        # Discover initial leader
        await self.discover_leader()
        
        logger.info("RaftClient started")
    
    async def stop(self):
        """Stop the client"""
        self._running = False
        
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        
        await self.idempotency_manager.stop()
        await self.client.aclose()
        
        logger.info("RaftClient stopped")
    
    async def discover_leader(self, force: bool = False) -> Optional[str]:
        """
        Discover the current leader
        
        Args:
            force: Force rediscovery even if we think we know the leader
            
        Returns:
            Node ID of the leader, or None if no leader found
        """
        async with self._leader_lock:
            if self.leader_discovery_in_progress and not force:
                return self.current_leader
            
            if self.current_leader and not force:
                # Verify current leader is still valid
                if await self._verify_leader(self.current_leader):
                    return self.current_leader
            
            self.leader_discovery_in_progress = True
            
            try:
                # Try all nodes to find leader
                tasks = []
                for node_id, endpoint in self.nodes.items():
                    if endpoint.is_healthy or force:
                        tasks.append(self._check_node_status(node_id))
                
                if not tasks:
                    logger.warning("No healthy nodes available for leader discovery")
                    return None
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Find leader from results
                for node_id, result in zip(self.nodes.keys(), results):
                    if isinstance(result, dict) and result.get("state") == "leader":
                        self.current_leader = node_id
                        self.nodes[node_id].is_leader = True
                        logger.info(f"Discovered leader: {node_id}")
                        return node_id
                
                # No leader found
                logger.warning("No leader found in cluster")
                self.current_leader = None
                return None
                
            finally:
                self.leader_discovery_in_progress = False
    
    async def _check_node_status(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Check status of a single node"""
        endpoint = self.nodes[node_id]
        
        try:
            response = await self.client.get(
                f"http://{endpoint.address}/raft/status",
                timeout=2.0  # Shorter timeout for status checks
            )
            
            if response.status_code == 200:
                endpoint.last_contact = datetime.utcnow()
                endpoint.consecutive_failures = 0
                endpoint.is_healthy = True
                return response.json()
            else:
                endpoint.consecutive_failures += 1
                return None
                
        except Exception as e:
            endpoint.consecutive_failures += 1
            logger.debug(f"Failed to check status of {node_id}: {e}")
            return None
    
    async def _verify_leader(self, node_id: str) -> bool:
        """Verify if a node is still the leader"""
        status = await self._check_node_status(node_id)
        return status is not None and status.get("state") == "leader"
    
    async def _health_check_loop(self):
        """Background health check loop"""
        while self._running:
            try:
                await asyncio.sleep(self.health_check_interval)
                
                # Check all nodes
                tasks = []
                for node_id in self.nodes:
                    tasks.append(self._check_node_status(node_id))
                
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # Update health status
                for node_id, endpoint in self.nodes.items():
                    if endpoint.consecutive_failures >= 3:
                        if endpoint.is_healthy:
                            logger.warning(f"Node {node_id} marked as unhealthy")
                        endpoint.is_healthy = False
                    else:
                        if not endpoint.is_healthy:
                            logger.info(f"Node {node_id} marked as healthy")
                        endpoint.is_healthy = True
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
    
    async def _execute_request(self, 
                             method: str, 
                             path: str, 
                             node_id: Optional[str] = None,
                             **kwargs) -> httpx.Response:
        """
        Execute a request with retries and failover
        
        Args:
            method: HTTP method
            path: URL path
            node_id: Specific node to target (None for leader)
            **kwargs: Additional arguments for httpx request
            
        Returns:
            HTTP response
        """
        # Use specific node or discover leader
        if node_id is None:
            target_node = await self.discover_leader()
            if not target_node:
                raise Exception("No leader available")
        else:
            target_node = node_id
        
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                endpoint = self.nodes[target_node]
                url = f"http://{endpoint.address}{path}"
                
                response = await self.client.request(
                    method, url, **kwargs
                )
                
                # Check for leader redirect
                if response.status_code == 421:  # Misdirected request
                    logger.info(f"Node {target_node} is not leader, rediscovering...")
                    self.failover_count += 1
                    
                    # Force leader rediscovery
                    target_node = await self.discover_leader(force=True)
                    if not target_node:
                        raise Exception("No leader available after redirect")
                    
                    # Retry with new leader
                    continue
                
                # Success
                return response
                
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                last_exception = e
                self.retry_count += 1
                
                logger.warning(f"Request failed to {target_node}: {e}")
                
                # Mark node as potentially unhealthy
                endpoint.consecutive_failures += 1
                
                # Try another node if this was the leader
                if target_node == self.current_leader:
                    self.failover_count += 1
                    target_node = await self.discover_leader(force=True)
                    if not target_node:
                        break
                
                # Exponential backoff
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
        
        raise Exception(f"Request failed after {self.max_retries} attempts: {last_exception}")
    
    async def get(self, key: str, linearizable: bool = True) -> Dict[str, Any]:
        """
        Get a value from the cluster
        
        Args:
            key: Key to retrieve
            linearizable: If True, ensure linearizable read
            
        Returns:
            Value and metadata
        """
        self.request_count += 1
        
        # For linearizable reads, must go through leader
        # For non-linearizable, can read from any node
        node_id = None if linearizable else self._get_random_healthy_node()
        
        response = await self._execute_request(
            "GET", f"/kv/{key}", node_id=node_id
        )
        
        response.raise_for_status()
        return response.json()
    
    async def put(self, key: str, value: Any, request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Put a value into the cluster
        
        Args:
            key: Key to store
            value: Value to store
            request_id: Optional request ID for idempotency
            
        Returns:
            Stored value and metadata
        """
        self.request_count += 1
        
        # Generate request ID if not provided
        if request_id is None:
            request_id = self.idempotency_manager.generate_request_id("put", key, value)
        
        # Check for duplicate request
        cached_result = await self.idempotency_manager.check_request(request_id)
        if cached_result is not None:
            logger.debug(f"Returning cached result for request {request_id}")
            return cached_result
        
        # Execute request
        response = await self._execute_request(
            "PUT", f"/kv/{key}",
            json={"value": value},
            headers={"X-Request-ID": request_id}
        )
        
        response.raise_for_status()
        result = response.json()
        
        # Cache result
        await self.idempotency_manager.store_result(request_id, result)
        
        return result
    
    async def update(self, key: str, value: Any, version: int, 
                    request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Update a value with optimistic locking
        
        Args:
            key: Key to update
            value: New value
            version: Expected version
            request_id: Optional request ID for idempotency
            
        Returns:
            Updated value and metadata
        """
        self.request_count += 1
        
        # Generate request ID if not provided
        if request_id is None:
            request_id = self.idempotency_manager.generate_request_id("update", key, value)
        
        # Check for duplicate request
        cached_result = await self.idempotency_manager.check_request(request_id)
        if cached_result is not None:
            logger.debug(f"Returning cached result for request {request_id}")
            return cached_result
        
        # Execute request
        response = await self._execute_request(
            "POST", f"/kv/{key}",
            json={"value": value, "version": version},
            headers={"X-Request-ID": request_id}
        )
        
        response.raise_for_status()
        result = response.json()
        
        # Cache result
        await self.idempotency_manager.store_result(request_id, result)
        
        return result
    
    async def delete(self, key: str, request_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Delete a key from the cluster
        
        Args:
            key: Key to delete
            request_id: Optional request ID for idempotency
            
        Returns:
            Deleted value and metadata if key existed
        """
        self.request_count += 1
        
        # Generate request ID if not provided
        if request_id is None:
            request_id = self.idempotency_manager.generate_request_id("delete", key)
        
        # Check for duplicate request
        cached_result = await self.idempotency_manager.check_request(request_id)
        if cached_result is not None:
            logger.debug(f"Returning cached result for request {request_id}")
            return cached_result
        
        # Execute request
        response = await self._execute_request(
            "DELETE", f"/kv/{key}",
            headers={"X-Request-ID": request_id}
        )
        
        if response.status_code == 404:
            return None
        
        response.raise_for_status()
        result = response.json()
        
        # Cache result
        await self.idempotency_manager.store_result(request_id, result)
        
        return result
    
    def _get_random_healthy_node(self) -> Optional[str]:
        """Get a random healthy node for non-linearizable reads"""
        healthy_nodes = [
            node_id for node_id, endpoint in self.nodes.items()
            if endpoint.is_healthy
        ]
        
        if healthy_nodes:
            return random.choice(healthy_nodes)
        return None
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get information about the cluster"""
        return {
            "nodes": {
                node_id: {
                    "address": endpoint.address,
                    "is_leader": endpoint.is_leader,
                    "is_healthy": endpoint.is_healthy,
                    "consecutive_failures": endpoint.consecutive_failures,
                    "last_contact": endpoint.last_contact.isoformat() if endpoint.last_contact else None
                }
                for node_id, endpoint in self.nodes.items()
            },
            "current_leader": self.current_leader,
            "metrics": {
                "request_count": self.request_count,
                "retry_count": self.retry_count,
                "failover_count": self.failover_count
            }
        }


class RaftClientPool:
    """
    Connection pool for Raft clients
    """
    
    def __init__(self, 
                 nodes: Dict[str, str],
                 pool_size: int = 10,
                 **client_kwargs):
        """
        Initialize client pool
        
        Args:
            nodes: Dict mapping node_id to address
            pool_size: Number of clients in pool
            **client_kwargs: Additional arguments for RaftClient
        """
        self.nodes = nodes
        self.pool_size = pool_size
        self.client_kwargs = client_kwargs
        
        self.clients: List[RaftClient] = []
        self.available: asyncio.Queue = asyncio.Queue()
        self._initialized = False
    
    async def initialize(self):
        """Initialize the client pool"""
        if self._initialized:
            return
        
        # Create clients
        for _ in range(self.pool_size):
            client = RaftClient(self.nodes, **self.client_kwargs)
            await client.start()
            self.clients.append(client)
            await self.available.put(client)
        
        self._initialized = True
        logger.info(f"Initialized RaftClientPool with {self.pool_size} clients")
    
    async def acquire(self) -> RaftClient:
        """Acquire a client from the pool"""
        if not self._initialized:
            await self.initialize()
        
        return await self.available.get()
    
    async def release(self, client: RaftClient):
        """Release a client back to the pool"""
        await self.available.put(client)
    
    async def close(self):
        """Close all clients in the pool"""
        for client in self.clients:
            await client.stop()
        
        self.clients.clear()
        self._initialized = False