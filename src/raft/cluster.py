"""
Cluster Management for Raft Consensus
Handles dynamic cluster membership and configuration changes
"""

import json
import asyncio
import logging
from typing import Dict, List, Set, Optional, Any, Tuple
from datetime import datetime
from pathlib import Path
import aiofiles
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class ConfigChangeType(Enum):
    """Types of configuration changes"""
    ADD_NODE = "add_node"
    REMOVE_NODE = "remove_node"
    REPLACE_NODE = "replace_node"


@dataclass
class ClusterNode:
    """Represents a node in the cluster"""
    node_id: str
    address: str
    join_time: datetime
    last_heartbeat: Optional[datetime] = None
    is_voter: bool = True  # Can participate in voting
    is_learner: bool = False  # Non-voting member (for catching up)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "address": self.address,
            "join_time": self.join_time.isoformat(),
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "is_voter": self.is_voter,
            "is_learner": self.is_learner
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClusterNode':
        return cls(
            node_id=data["node_id"],
            address=data["address"],
            join_time=datetime.fromisoformat(data["join_time"]),
            last_heartbeat=datetime.fromisoformat(data["last_heartbeat"]) if data["last_heartbeat"] else None,
            is_voter=data.get("is_voter", True),
            is_learner=data.get("is_learner", False)
        )


@dataclass
class ConfigurationChange:
    """Represents a configuration change in the cluster"""
    change_type: ConfigChangeType
    node_id: str
    address: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "change_type": self.change_type.value,
            "node_id": self.node_id,
            "address": self.address,
            "timestamp": self.timestamp.isoformat()
        }


class ClusterManager:
    """
    Manages cluster membership and configuration changes
    """
    
    def __init__(self, node_id: str, state_dir: str = "./raft_state"):
        self.node_id = node_id
        self.state_dir = Path(state_dir) / node_id
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Current cluster configuration
        self.nodes: Dict[str, ClusterNode] = {}
        self.voters: Set[str] = set()
        self.learners: Set[str] = set()
        
        # Configuration change state (joint consensus)
        self.old_config: Optional[Set[str]] = None
        self.new_config: Optional[Set[str]] = None
        self.config_change_in_progress = False
        
        # Node health tracking
        self.unhealthy_nodes: Set[str] = set()
        self.node_health_check_interval = 5.0  # seconds
        self.node_unhealthy_threshold = 30.0  # seconds
        
        # Lock for configuration changes
        self._config_lock = asyncio.Lock()
        
        # Background tasks
        self._health_check_task: Optional[asyncio.Task] = None
        
        logger.info(f"Initialized ClusterManager for node {node_id}")
    
    async def initialize(self, initial_nodes: Dict[str, str]):
        """Initialize cluster with initial nodes"""
        async with self._config_lock:
            for node_id, address in initial_nodes.items():
                node = ClusterNode(
                    node_id=node_id,
                    address=address,
                    join_time=datetime.utcnow(),
                    is_voter=True
                )
                self.nodes[node_id] = node
                self.voters.add(node_id)
            
            await self._save_configuration()
        
        # Start health monitoring
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        
        logger.info(f"Initialized cluster with {len(self.nodes)} nodes")
    
    async def shutdown(self):
        """Shutdown cluster manager"""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
    
    async def add_node(self, node_id: str, address: str, as_learner: bool = True) -> bool:
        """
        Add a new node to the cluster
        
        Args:
            node_id: ID of the new node
            address: Network address of the new node
            as_learner: Whether to add as learner first (safer)
            
        Returns:
            True if node was added successfully
        """
        async with self._config_lock:
            if node_id in self.nodes:
                logger.warning(f"Node {node_id} already exists in cluster")
                return False
            
            if self.config_change_in_progress:
                logger.warning("Configuration change already in progress")
                return False
            
            # Create new node
            node = ClusterNode(
                node_id=node_id,
                address=address,
                join_time=datetime.utcnow(),
                is_voter=not as_learner,
                is_learner=as_learner
            )
            
            # Add to configuration
            self.nodes[node_id] = node
            if as_learner:
                self.learners.add(node_id)
            else:
                self.voters.add(node_id)
            
            await self._save_configuration()
            
            logger.info(f"Added node {node_id} as {'learner' if as_learner else 'voter'}")
            return True
    
    async def remove_node(self, node_id: str) -> bool:
        """
        Remove a node from the cluster
        
        Args:
            node_id: ID of the node to remove
            
        Returns:
            True if node was removed successfully
        """
        async with self._config_lock:
            if node_id not in self.nodes:
                logger.warning(f"Node {node_id} not found in cluster")
                return False
            
            if self.config_change_in_progress:
                logger.warning("Configuration change already in progress")
                return False
            
            # Remove from all sets
            self.nodes.pop(node_id)
            self.voters.discard(node_id)
            self.learners.discard(node_id)
            self.unhealthy_nodes.discard(node_id)
            
            await self._save_configuration()
            
            logger.info(f"Removed node {node_id} from cluster")
            return True
    
    async def promote_learner(self, node_id: str) -> bool:
        """
        Promote a learner to voting member
        
        Args:
            node_id: ID of the learner to promote
            
        Returns:
            True if promotion was successful
        """
        async with self._config_lock:
            if node_id not in self.learners:
                logger.warning(f"Node {node_id} is not a learner")
                return False
            
            node = self.nodes[node_id]
            node.is_learner = False
            node.is_voter = True
            
            self.learners.remove(node_id)
            self.voters.add(node_id)
            
            await self._save_configuration()
            
            logger.info(f"Promoted learner {node_id} to voter")
            return True
    
    async def start_configuration_change(self, change: ConfigurationChange):
        """
        Start a configuration change using joint consensus
        
        This implements the two-phase configuration change protocol:
        1. Transition to joint configuration (Cold,new)
        2. Transition to new configuration (Cnew)
        """
        async with self._config_lock:
            if self.config_change_in_progress:
                raise Exception("Configuration change already in progress")
            
            self.config_change_in_progress = True
            self.old_config = self.voters.copy()
            
            # Apply the change to create new configuration
            if change.change_type == ConfigChangeType.ADD_NODE:
                self.new_config = self.old_config.copy()
                self.new_config.add(change.node_id)
            elif change.change_type == ConfigChangeType.REMOVE_NODE:
                self.new_config = self.old_config.copy()
                self.new_config.discard(change.node_id)
            
            logger.info(f"Started configuration change: {change.change_type.value}")
    
    async def commit_configuration_change(self):
        """Commit the configuration change (phase 2)"""
        async with self._config_lock:
            if not self.config_change_in_progress:
                raise Exception("No configuration change in progress")
            
            # Transition to new configuration
            self.voters = self.new_config.copy()
            self.old_config = None
            self.new_config = None
            self.config_change_in_progress = False
            
            await self._save_configuration()
            
            logger.info("Committed configuration change")
    
    def get_voting_members(self) -> Set[str]:
        """Get current voting members (considering joint consensus)"""
        if self.config_change_in_progress:
            # During joint consensus, need majority from both configurations
            return self.old_config.union(self.new_config)
        return self.voters.copy()
    
    def requires_joint_majority(self) -> bool:
        """Check if we're in joint consensus mode"""
        return self.config_change_in_progress
    
    def calculate_quorum_size(self) -> int:
        """Calculate quorum size for current configuration"""
        if self.config_change_in_progress:
            # Need majority from both old and new configurations
            old_quorum = (len(self.old_config) + 1) // 2
            new_quorum = (len(self.new_config) + 1) // 2
            return max(old_quorum, new_quorum)
        return (len(self.voters) + 1) // 2
    
    def check_quorum(self, votes: Set[str]) -> bool:
        """Check if we have quorum with given votes"""
        if self.config_change_in_progress:
            # Need majority from both configurations
            old_votes = votes.intersection(self.old_config)
            new_votes = votes.intersection(self.new_config)
            
            old_quorum = (len(self.old_config) + 1) // 2
            new_quorum = (len(self.new_config) + 1) // 2
            
            return len(old_votes) >= old_quorum and len(new_votes) >= new_quorum
        
        # Simple majority
        return len(votes) >= self.calculate_quorum_size()
    
    async def update_node_heartbeat(self, node_id: str):
        """Update last heartbeat time for a node"""
        if node_id in self.nodes:
            self.nodes[node_id].last_heartbeat = datetime.utcnow()
            self.unhealthy_nodes.discard(node_id)
    
    async def _health_check_loop(self):
        """Background task to monitor node health"""
        while True:
            try:
                await asyncio.sleep(self.node_health_check_interval)
                
                now = datetime.utcnow()
                newly_unhealthy = set()
                
                for node_id, node in self.nodes.items():
                    if node_id == self.node_id:
                        continue  # Skip self
                    
                    if node.last_heartbeat:
                        time_since_heartbeat = (now - node.last_heartbeat).total_seconds()
                        if time_since_heartbeat > self.node_unhealthy_threshold:
                            newly_unhealthy.add(node_id)
                
                # Update unhealthy nodes set
                previously_unhealthy = self.unhealthy_nodes
                self.unhealthy_nodes = newly_unhealthy
                
                # Log changes
                became_unhealthy = newly_unhealthy - previously_unhealthy
                became_healthy = previously_unhealthy - newly_unhealthy
                
                for node_id in became_unhealthy:
                    logger.warning(f"Node {node_id} is now unhealthy")
                
                for node_id in became_healthy:
                    logger.info(f"Node {node_id} is now healthy")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
    
    async def _save_configuration(self):
        """Save cluster configuration to disk"""
        config_data = {
            "nodes": {
                node_id: node.to_dict() 
                for node_id, node in self.nodes.items()
            },
            "voters": list(self.voters),
            "learners": list(self.learners),
            "config_change_in_progress": self.config_change_in_progress,
            "old_config": list(self.old_config) if self.old_config else None,
            "new_config": list(self.new_config) if self.new_config else None
        }
        
        config_file = self.state_dir / "cluster_config.json"
        temp_file = self.state_dir / "cluster_config.json.tmp"
        
        try:
            async with aiofiles.open(temp_file, 'w') as f:
                await f.write(json.dumps(config_data, indent=2))
            
            # Atomic rename
            temp_file.replace(config_file)
            
            logger.debug("Saved cluster configuration")
        except Exception as e:
            logger.error(f"Failed to save cluster configuration: {e}")
            raise
    
    async def load_configuration(self):
        """Load cluster configuration from disk"""
        config_file = self.state_dir / "cluster_config.json"
        
        if not config_file.exists():
            logger.info("No existing cluster configuration found")
            return
        
        try:
            async with aiofiles.open(config_file, 'r') as f:
                content = await f.read()
                config_data = json.loads(content)
            
            # Restore nodes
            self.nodes = {
                node_id: ClusterNode.from_dict(node_data)
                for node_id, node_data in config_data["nodes"].items()
            }
            
            # Restore sets
            self.voters = set(config_data["voters"])
            self.learners = set(config_data["learners"])
            
            # Restore configuration change state
            self.config_change_in_progress = config_data["config_change_in_progress"]
            self.old_config = set(config_data["old_config"]) if config_data["old_config"] else None
            self.new_config = set(config_data["new_config"]) if config_data["new_config"] else None
            
            logger.info(f"Loaded cluster configuration with {len(self.nodes)} nodes")
        except Exception as e:
            logger.error(f"Failed to load cluster configuration: {e}")
            raise
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get current cluster status"""
        return {
            "node_id": self.node_id,
            "total_nodes": len(self.nodes),
            "voting_members": len(self.voters),
            "learners": len(self.learners),
            "unhealthy_nodes": len(self.unhealthy_nodes),
            "config_change_in_progress": self.config_change_in_progress,
            "nodes": {
                node_id: {
                    "address": node.address,
                    "is_voter": node.is_voter,
                    "is_learner": node.is_learner,
                    "is_healthy": node_id not in self.unhealthy_nodes,
                    "join_time": node.join_time.isoformat()
                }
                for node_id, node in self.nodes.items()
            }
        }