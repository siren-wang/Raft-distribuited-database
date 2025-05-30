"""
Node Discovery and Registration Service for Raft
Provides automatic node discovery and dynamic cluster membership
"""

import asyncio
import json
import logging
import aiohttp
from typing import Dict, List, Optional, Set, Callable, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import socket
import consul.aio
import etcd3.aio as etcd3
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class NodeInfo:
    """Information about a cluster node"""
    node_id: str
    address: str
    grpc_port: int
    http_port: int
    region: str = "default"
    datacenter: str = "default"
    tags: List[str] = None
    metadata: Dict[str, Any] = None
    last_seen: datetime = None
    health_status: str = "unknown"
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.metadata is None:
            self.metadata = {}
        if self.last_seen is None:
            self.last_seen = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "node_id": self.node_id,
            "address": self.address,
            "grpc_port": self.grpc_port,
            "http_port": self.http_port,
            "region": self.region,
            "datacenter": self.datacenter,
            "tags": self.tags,
            "metadata": self.metadata,
            "last_seen": self.last_seen.isoformat(),
            "health_status": self.health_status
        }


class DiscoveryBackend(ABC):
    """Abstract base class for discovery backends"""
    
    @abstractmethod
    async def register_node(self, node_info: NodeInfo, ttl: int = 30) -> bool:
        """Register a node with the discovery service"""
        pass
    
    @abstractmethod
    async def deregister_node(self, node_id: str) -> bool:
        """Deregister a node from the discovery service"""
        pass
    
    @abstractmethod
    async def get_nodes(self) -> List[NodeInfo]:
        """Get all registered nodes"""
        pass
    
    @abstractmethod
    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get information about a specific node"""
        pass
    
    @abstractmethod
    async def update_health(self, node_id: str, health_status: str) -> bool:
        """Update node health status"""
        pass
    
    @abstractmethod
    async def watch_nodes(self, callback: Callable[[List[NodeInfo]], None]):
        """Watch for changes in node membership"""
        pass


class ConsulDiscoveryBackend(DiscoveryBackend):
    """Consul-based discovery backend"""
    
    def __init__(self, consul_host: str = "localhost", consul_port: int = 8500):
        self.consul = consul.aio.Consul(host=consul_host, port=consul_port)
        self.service_name = "raft-cluster"
        self._watch_index = None
        
    async def register_node(self, node_info: NodeInfo, ttl: int = 30) -> bool:
        """Register node with Consul"""
        try:
            # Create service registration
            service = {
                "ID": node_info.node_id,
                "Name": self.service_name,
                "Tags": node_info.tags + [node_info.region, node_info.datacenter],
                "Address": node_info.address,
                "Port": node_info.grpc_port,
                "Meta": {
                    "http_port": str(node_info.http_port),
                    "region": node_info.region,
                    "datacenter": node_info.datacenter,
                    **{k: str(v) for k, v in node_info.metadata.items()}
                },
                "Check": {
                    "CheckID": f"{node_info.node_id}-health",
                    "Name": "Node Health Check",
                    "TTL": f"{ttl}s",
                    "Status": "passing"
                }
            }
            
            # Register service
            await self.consul.agent.service.register(**service)
            logger.info(f"Registered node {node_info.node_id} with Consul")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register node with Consul: {e}")
            return False
    
    async def deregister_node(self, node_id: str) -> bool:
        """Deregister node from Consul"""
        try:
            await self.consul.agent.service.deregister(node_id)
            logger.info(f"Deregistered node {node_id} from Consul")
            return True
        except Exception as e:
            logger.error(f"Failed to deregister node from Consul: {e}")
            return False
    
    async def get_nodes(self) -> List[NodeInfo]:
        """Get all registered nodes from Consul"""
        try:
            # Get all service instances
            _, services = await self.consul.health.service(self.service_name, passing=True)
            
            nodes = []
            for service in services:
                node_info = self._consul_service_to_node_info(service)
                if node_info:
                    nodes.append(node_info)
            
            return nodes
            
        except Exception as e:
            logger.error(f"Failed to get nodes from Consul: {e}")
            return []
    
    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get specific node from Consul"""
        try:
            _, service = await self.consul.agent.service(node_id)
            if service:
                return self._consul_service_to_node_info({"Service": service})
            return None
        except Exception as e:
            logger.error(f"Failed to get node {node_id} from Consul: {e}")
            return None
    
    async def update_health(self, node_id: str, health_status: str) -> bool:
        """Update node health check in Consul"""
        try:
            check_id = f"{node_id}-health"
            status = "passing" if health_status == "healthy" else "critical"
            await self.consul.agent.check.update(check_id, status=status)
            return True
        except Exception as e:
            logger.error(f"Failed to update health for {node_id}: {e}")
            return False
    
    async def watch_nodes(self, callback: Callable[[List[NodeInfo]], None]):
        """Watch for changes in Consul service registry"""
        while True:
            try:
                # Long poll for changes
                index = self._watch_index or "0"
                _, services = await self.consul.health.service(
                    self.service_name,
                    index=index,
                    wait="30s"
                )
                
                # Update watch index
                if services:
                    self._watch_index = services[0].get("ModifyIndex", index)
                
                # Convert to NodeInfo list
                nodes = []
                for service in services:
                    node_info = self._consul_service_to_node_info(service)
                    if node_info:
                        nodes.append(node_info)
                
                # Notify callback
                await callback(nodes)
                
            except Exception as e:
                logger.error(f"Error watching Consul: {e}")
                await asyncio.sleep(5)
    
    def _consul_service_to_node_info(self, service: Dict) -> Optional[NodeInfo]:
        """Convert Consul service to NodeInfo"""
        try:
            svc = service.get("Service", {})
            meta = svc.get("Meta", {})
            
            return NodeInfo(
                node_id=svc.get("ID"),
                address=svc.get("Address"),
                grpc_port=svc.get("Port"),
                http_port=int(meta.get("http_port", 0)),
                region=meta.get("region", "default"),
                datacenter=meta.get("datacenter", "default"),
                tags=svc.get("Tags", []),
                metadata={k: v for k, v in meta.items() 
                         if k not in ["http_port", "region", "datacenter"]},
                health_status="healthy"  # Since we query passing services
            )
        except Exception as e:
            logger.error(f"Failed to parse Consul service: {e}")
            return None


class EtcdDiscoveryBackend(DiscoveryBackend):
    """etcd-based discovery backend"""
    
    def __init__(self, etcd_host: str = "localhost", etcd_port: int = 2379):
        self.etcd = etcd3.client(host=etcd_host, port=etcd_port)
        self.prefix = "/raft/nodes/"
        self._watch_task = None
        
    async def register_node(self, node_info: NodeInfo, ttl: int = 30) -> bool:
        """Register node with etcd"""
        try:
            key = f"{self.prefix}{node_info.node_id}"
            value = json.dumps(node_info.to_dict())
            
            # Create lease for TTL
            lease = await self.etcd.lease(ttl)
            
            # Put with lease
            await self.etcd.put(key, value, lease=lease)
            
            # Keep lease alive
            asyncio.create_task(self._keep_alive_lease(lease, ttl))
            
            logger.info(f"Registered node {node_info.node_id} with etcd")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register node with etcd: {e}")
            return False
    
    async def deregister_node(self, node_id: str) -> bool:
        """Deregister node from etcd"""
        try:
            key = f"{self.prefix}{node_id}"
            await self.etcd.delete(key)
            logger.info(f"Deregistered node {node_id} from etcd")
            return True
        except Exception as e:
            logger.error(f"Failed to deregister node from etcd: {e}")
            return False
    
    async def get_nodes(self) -> List[NodeInfo]:
        """Get all registered nodes from etcd"""
        try:
            nodes = []
            
            # Get all keys with prefix
            async for value, metadata in self.etcd.get_prefix(self.prefix):
                if value:
                    node_data = json.loads(value.decode('utf-8'))
                    node_info = self._dict_to_node_info(node_data)
                    if node_info:
                        nodes.append(node_info)
            
            return nodes
            
        except Exception as e:
            logger.error(f"Failed to get nodes from etcd: {e}")
            return []
    
    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get specific node from etcd"""
        try:
            key = f"{self.prefix}{node_id}"
            value, _ = await self.etcd.get(key)
            
            if value:
                node_data = json.loads(value.decode('utf-8'))
                return self._dict_to_node_info(node_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get node {node_id} from etcd: {e}")
            return None
    
    async def update_health(self, node_id: str, health_status: str) -> bool:
        """Update node health status in etcd"""
        try:
            node_info = await self.get_node(node_id)
            if node_info:
                node_info.health_status = health_status
                node_info.last_seen = datetime.utcnow()
                
                key = f"{self.prefix}{node_id}"
                value = json.dumps(node_info.to_dict())
                await self.etcd.put(key, value)
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to update health for {node_id}: {e}")
            return False
    
    async def watch_nodes(self, callback: Callable[[List[NodeInfo]], None]):
        """Watch for changes in etcd"""
        async def watch_handler():
            watch_id = await self.etcd.add_watch_prefix_callback(
                self.prefix,
                lambda event: asyncio.create_task(self._handle_watch_event(event, callback))
            )
            
            # Keep watching until cancelled
            while True:
                await asyncio.sleep(1)
        
        self._watch_task = asyncio.create_task(watch_handler())
    
    async def _handle_watch_event(self, event, callback: Callable):
        """Handle watch event from etcd"""
        # Get current nodes and notify callback
        nodes = await self.get_nodes()
        await callback(nodes)
    
    async def _keep_alive_lease(self, lease, ttl: int):
        """Keep lease alive"""
        while True:
            try:
                await asyncio.sleep(ttl / 3)  # Refresh at 1/3 of TTL
                await lease.refresh()
            except Exception as e:
                logger.error(f"Failed to refresh lease: {e}")
                break
    
    def _dict_to_node_info(self, data: Dict) -> Optional[NodeInfo]:
        """Convert dictionary to NodeInfo"""
        try:
            return NodeInfo(
                node_id=data["node_id"],
                address=data["address"],
                grpc_port=data["grpc_port"],
                http_port=data["http_port"],
                region=data.get("region", "default"),
                datacenter=data.get("datacenter", "default"),
                tags=data.get("tags", []),
                metadata=data.get("metadata", {}),
                last_seen=datetime.fromisoformat(data["last_seen"]),
                health_status=data.get("health_status", "unknown")
            )
        except Exception as e:
            logger.error(f"Failed to parse node data: {e}")
            return None


class GossipDiscoveryBackend(DiscoveryBackend):
    """Gossip-based discovery backend for peer-to-peer discovery"""
    
    def __init__(self, node_id: str, bind_address: str, bind_port: int, 
                 seeds: List[str] = None):
        self.node_id = node_id
        self.bind_address = bind_address
        self.bind_port = bind_port
        self.seeds = seeds or []
        
        # Node state
        self.nodes: Dict[str, NodeInfo] = {}
        self.node_versions: Dict[str, int] = {}  # For vector clocks
        self._callbacks: List[Callable] = []
        
        # Gossip parameters
        self.gossip_interval = 1.0  # seconds
        self.gossip_fanout = 3
        self.max_gossip_age = 300  # 5 minutes
        
        # Network
        self._socket = None
        self._listen_task = None
        self._gossip_task = None
        
    async def start(self):
        """Start gossip protocol"""
        # Create UDP socket
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.bind((self.bind_address, self.bind_port))
        self._socket.setblocking(False)
        
        # Start listening for gossip messages
        self._listen_task = asyncio.create_task(self._listen_loop())
        
        # Start gossip timer
        self._gossip_task = asyncio.create_task(self._gossip_loop())
        
        # Join seeds
        for seed in self.seeds:
            await self._send_join_request(seed)
        
        logger.info(f"Started gossip discovery on {self.bind_address}:{self.bind_port}")
    
    async def stop(self):
        """Stop gossip protocol"""
        if self._listen_task:
            self._listen_task.cancel()
        if self._gossip_task:
            self._gossip_task.cancel()
        if self._socket:
            self._socket.close()
    
    async def register_node(self, node_info: NodeInfo, ttl: int = 30) -> bool:
        """Register local node"""
        self.nodes[node_info.node_id] = node_info
        self.node_versions[node_info.node_id] = 1
        
        # Gossip immediately
        await self._gossip_state()
        
        return True
    
    async def deregister_node(self, node_id: str) -> bool:
        """Deregister node"""
        if node_id in self.nodes:
            del self.nodes[node_id]
            del self.node_versions[node_id]
            
            # Gossip removal
            await self._gossip_removal(node_id)
            
            return True
        return False
    
    async def get_nodes(self) -> List[NodeInfo]:
        """Get all known nodes"""
        # Filter out stale nodes
        now = datetime.utcnow()
        active_nodes = []
        
        for node in self.nodes.values():
            age = (now - node.last_seen).total_seconds()
            if age < self.max_gossip_age:
                active_nodes.append(node)
        
        return active_nodes
    
    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get specific node"""
        return self.nodes.get(node_id)
    
    async def update_health(self, node_id: str, health_status: str) -> bool:
        """Update node health"""
        if node_id in self.nodes:
            self.nodes[node_id].health_status = health_status
            self.nodes[node_id].last_seen = datetime.utcnow()
            self.node_versions[node_id] += 1
            
            # Gossip update
            await self._gossip_state()
            
            return True
        return False
    
    async def watch_nodes(self, callback: Callable[[List[NodeInfo]], None]):
        """Register callback for node changes"""
        self._callbacks.append(callback)
    
    async def _listen_loop(self):
        """Listen for gossip messages"""
        while True:
            try:
                data, addr = await asyncio.get_event_loop().sock_recvfrom(
                    self._socket, 65536
                )
                
                message = json.loads(data.decode('utf-8'))
                await self._handle_gossip_message(message, addr)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in gossip listen loop: {e}")
    
    async def _gossip_loop(self):
        """Periodically gossip state"""
        while True:
            try:
                await asyncio.sleep(self.gossip_interval)
                await self._gossip_state()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in gossip loop: {e}")
    
    async def _gossip_state(self):
        """Send gossip to random nodes"""
        if not self.nodes:
            return
        
        # Select random nodes
        active_nodes = await self.get_nodes()
        if len(active_nodes) <= 1:
            return
        
        # Create gossip message
        message = {
            "type": "gossip",
            "sender": self.node_id,
            "nodes": {
                node_id: {
                    "info": node.to_dict(),
                    "version": self.node_versions.get(node_id, 0)
                }
                for node_id, node in self.nodes.items()
            }
        }
        
        # Send to random subset
        import random
        targets = random.sample(active_nodes, min(self.gossip_fanout, len(active_nodes) - 1))
        
        for target in targets:
            if target.node_id != self.node_id:
                await self._send_message(message, target.address, target.grpc_port)
    
    async def _handle_gossip_message(self, message: Dict, addr):
        """Handle incoming gossip message"""
        msg_type = message.get("type")
        
        if msg_type == "join":
            await self._handle_join(message, addr)
        elif msg_type == "gossip":
            await self._handle_gossip(message)
        elif msg_type == "removal":
            await self._handle_removal(message)
    
    async def _handle_gossip(self, message: Dict):
        """Handle gossip update"""
        sender = message.get("sender")
        nodes_data = message.get("nodes", {})
        
        updated = False
        
        for node_id, node_data in nodes_data.items():
            info = node_data["info"]
            version = node_data["version"]
            
            # Check if we have newer information
            if node_id not in self.node_versions or version > self.node_versions[node_id]:
                # Update node
                node_info = NodeInfo(
                    node_id=info["node_id"],
                    address=info["address"],
                    grpc_port=info["grpc_port"],
                    http_port=info["http_port"],
                    region=info.get("region", "default"),
                    datacenter=info.get("datacenter", "default"),
                    tags=info.get("tags", []),
                    metadata=info.get("metadata", {}),
                    last_seen=datetime.fromisoformat(info["last_seen"]),
                    health_status=info.get("health_status", "unknown")
                )
                
                self.nodes[node_id] = node_info
                self.node_versions[node_id] = version
                updated = True
        
        # Notify callbacks if updated
        if updated:
            nodes = await self.get_nodes()
            for callback in self._callbacks:
                await callback(nodes)
    
    async def _send_message(self, message: Dict, address: str, port: int):
        """Send gossip message"""
        try:
            data = json.dumps(message).encode('utf-8')
            await asyncio.get_event_loop().sock_sendto(
                self._socket, data, (address, port)
            )
        except Exception as e:
            logger.error(f"Failed to send gossip message: {e}")


class DiscoveryService:
    """
    Main discovery service that can use different backends
    """
    
    def __init__(self, backend: DiscoveryBackend):
        self.backend = backend
        self.local_node: Optional[NodeInfo] = None
        self._health_check_task = None
        self._registration_task = None
        
        # Callbacks
        self.node_added_callbacks: List[Callable] = []
        self.node_removed_callbacks: List[Callable] = []
        self.node_updated_callbacks: List[Callable] = []
        
        # Cache
        self._nodes_cache: Dict[str, NodeInfo] = {}
        
    async def start(self, node_info: NodeInfo, ttl: int = 30):
        """Start discovery service for local node"""
        self.local_node = node_info
        
        # Register local node
        await self.backend.register_node(node_info, ttl)
        
        # Start health check loop
        self._health_check_task = asyncio.create_task(
            self._health_check_loop(ttl)
        )
        
        # Start registration refresh loop
        self._registration_task = asyncio.create_task(
            self._registration_loop(ttl)
        )
        
        # Start watching for changes
        asyncio.create_task(
            self.backend.watch_nodes(self._handle_nodes_change)
        )
        
        logger.info(f"Started discovery service for node {node_info.node_id}")
    
    async def stop(self):
        """Stop discovery service"""
        # Cancel tasks
        if self._health_check_task:
            self._health_check_task.cancel()
        if self._registration_task:
            self._registration_task.cancel()
        
        # Deregister node
        if self.local_node:
            await self.backend.deregister_node(self.local_node.node_id)
    
    async def get_nodes(self) -> List[NodeInfo]:
        """Get all nodes in cluster"""
        return await self.backend.get_nodes()
    
    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get specific node"""
        return await self.backend.get_node(node_id)
    
    async def get_leader(self) -> Optional[NodeInfo]:
        """Get current leader node"""
        nodes = await self.get_nodes()
        for node in nodes:
            if node.metadata.get("is_leader") == "true":
                return node
        return None
    
    async def update_leader_status(self, is_leader: bool):
        """Update local node's leader status"""
        if self.local_node:
            self.local_node.metadata["is_leader"] = str(is_leader).lower()
            await self.backend.register_node(self.local_node)
    
    def on_node_added(self, callback: Ca