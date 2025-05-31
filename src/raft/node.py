"""
Raft Node Implementation - FIXED
Core Raft consensus algorithm with leader election and log replication
"""

import asyncio
import random
import logging
from typing import Dict, Any, List, Optional, Callable, Tuple
from datetime import datetime, timedelta
from enum import Enum

from .state import RaftState, LogEntry
from .rpc import (
    RaftRPCClient, RaftRPCHandler, BatchedAppendEntries,
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse
)

logger = logging.getLogger(__name__)


class RaftNodeState(Enum):
    """Raft node states"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    """
    Main Raft consensus node implementation
    """
    
    def __init__(self, 
                 node_id: str,
                 cluster_config: Dict[str, str],
                 state_dir: str = "./raft_state",
                 election_timeout_range: Tuple[float, float] = (1.5, 3.0),
                 heartbeat_interval: float = 0.5):
        """
        Initialize Raft node
        
        Args:
            node_id: Unique identifier for this node
            cluster_config: Dict mapping node_id to address (host:port)
            state_dir: Directory for persistent state storage
            election_timeout_range: Min and max election timeout in seconds
            heartbeat_interval: Interval for leader heartbeats in seconds
        """
        self.node_id = node_id
        self.cluster_config = cluster_config
        self.cluster_nodes = list(cluster_config.keys())
        
        # State management
        self.state = RaftState(node_id, state_dir)
        
        # RPC handling
        self.rpc_client = RaftRPCClient(node_id, timeout=5.0)
        self.rpc_handler = RaftRPCHandler(self)
        self.batched_append = BatchedAppendEntries(self.rpc_client)
        
        # Timing configuration
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        
        # Runtime state
        self.current_leader: Optional[str] = None
        self.election_timer_task: Optional[asyncio.Task] = None
        self.heartbeat_task: Optional[asyncio.Task] = None
        
        # State machine callback
        self.apply_command_callback: Optional[Callable] = None
        
        # Control flags
        self.running = False
        self._shutdown_event = asyncio.Event()
        
        logger.info(f"Initialized RaftNode {node_id} with {len(cluster_config)} nodes in cluster")
        logger.info(f"Election timeout range: {election_timeout_range}, Heartbeat interval: {heartbeat_interval}")
    
    async def start(self):
        """Start the Raft node"""
        logger.info(f"Starting Raft node {self.node_id}")
        
        # Load persistent state
        await self.state.load_persistent_state()
        
        # Initialize RPC client
        await self.rpc_client.initialize(self.cluster_config)
        
        # Set running flag BEFORE transitioning to follower
        self.running = True
        
        # Wait for cluster to stabilize and all HTTP endpoints to be ready
        logger.info(f"Waiting for cluster to stabilize...")
        await asyncio.sleep(8.0)  # Long delay to ensure all nodes have HTTP endpoints ready
        
        # Start as follower
        self.transition_to_follower()
        
        # Start background tasks
        asyncio.create_task(self._run_state_machine())
        
        logger.info(f"Raft node {self.node_id} started successfully")
    
    async def stop(self):
        """Stop the Raft node"""
        logger.info(f"Stopping Raft node {self.node_id}")
        
        self.running = False
        self._shutdown_event.set()
        
        # Cancel timers
        if self.election_timer_task:
            self.election_timer_task.cancel()
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        # Close RPC connections
        await self.rpc_client.close()
        
        logger.info(f"Raft node {self.node_id} stopped")
    
    def transition_to_follower(self):
        """Transition to follower state"""
        logger.info(f"Node {self.node_id} transitioning to FOLLOWER")
        
        self.state.state = RaftNodeState.FOLLOWER.value
        self.current_leader = None
        
        # Cancel heartbeat if we were leader
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            self.heartbeat_task = None
        
        # Start election timer
        self.reset_election_timer()
    
    def transition_to_candidate(self):
        """Transition to candidate state"""
        logger.info(f"Node {self.node_id} transitioning to CANDIDATE")
        
        self.state.state = RaftNodeState.CANDIDATE.value
        self.current_leader = None
        
        # Start election
        asyncio.create_task(self._run_election())
    
    def transition_to_leader(self):
        """Transition to leader state"""
        logger.info(f"Node {self.node_id} transitioning to LEADER")
        
        self.state.state = RaftNodeState.LEADER.value
        self.current_leader = self.node_id
        
        # Cancel election timer
        if self.election_timer_task:
            self.election_timer_task.cancel()
            self.election_timer_task = None
        
        # Initialize leader state
        self.state.initialize_leader_state(self.cluster_nodes)
        
        # Send initial heartbeats
        asyncio.create_task(self._send_heartbeats())
        
        # Start heartbeat timer
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        # Append no-op entry to establish leadership
        asyncio.create_task(self._append_noop_entry())
    
    def reset_election_timer(self):
        """Reset the election timeout timer"""
        if self.election_timer_task:
            self.election_timer_task.cancel()
        
        # Check if we should start the timer
        if self.running and self.state.state != RaftNodeState.LEADER.value:
            timeout = random.uniform(*self.election_timeout_range)
            self.election_timer_task = asyncio.create_task(self._election_timeout(timeout))
            logger.info(f"Started election timer for node {self.node_id} with timeout {timeout:.3f}s")
    
    async def _election_timeout(self, timeout: float):
        """Handle election timeout"""
        try:
            logger.debug(f"Election timer started for node {self.node_id}, waiting {timeout:.3f}s")
            await asyncio.sleep(timeout)
            
            # Double-check we're still a follower
            if self.running and self.state.state == RaftNodeState.FOLLOWER.value:
                logger.info(f"Election timeout on node {self.node_id} after {timeout:.3f}s")
                self.transition_to_candidate()
            else:
                logger.debug(f"Election timer expired but node {self.node_id} is no longer follower (state={self.state.state})")
                
        except asyncio.CancelledError:
            logger.debug(f"Election timer cancelled for node {self.node_id}")
            pass
        except Exception as e:
            logger.error(f"Error in election timeout for node {self.node_id}: {e}")
    
    async def _run_election(self):
        """Run leader election"""
        try:
            # Increment current term and vote for self
            await self.state.update_term(self.state.current_term + 1)
            await self.state.record_vote(self.node_id)
            
            logger.info(f"Node {self.node_id} starting election for term {self.state.current_term}")
            
            # Reset election timer
            self.reset_election_timer()
            
            # Request votes from all other nodes
            votes_received = 1  # Vote for self
            votes_needed = (len(self.cluster_nodes) + 1) // 2
            
            logger.info(f"Node {self.node_id} needs {votes_needed} votes to win election")
            
            vote_requests = []
            for node_id in self.cluster_nodes:
                if node_id != self.node_id:
                    request = RequestVoteRequest(
                        term=self.state.current_term,
                        candidate_id=self.node_id,
                        last_log_index=self.state.get_last_log_index(),
                        last_log_term=self.state.get_last_log_term()
                    )
                    vote_requests.append(self._request_vote(node_id, request))
            
            # Wait for votes
            if vote_requests:
                responses = await asyncio.gather(*vote_requests, return_exceptions=True)
                
                for i, response in enumerate(responses):
                    peer_node = [n for n in self.cluster_nodes if n != self.node_id][i]
                    
                    if isinstance(response, RequestVoteResponse):
                        if response.vote_granted:
                            votes_received += 1
                            logger.info(f"Node {self.node_id} received vote from {peer_node} (total: {votes_received}/{votes_needed})")
                            
                            if votes_received >= votes_needed:
                                # Won election
                                if self.state.state == RaftNodeState.CANDIDATE.value:
                                    logger.info(f"Node {self.node_id} won election with {votes_received} votes")
                                    self.transition_to_leader()
                                return
                        else:
                            logger.info(f"Node {self.node_id} vote denied by {peer_node} (term: {response.term})")
                        
                        # Check if we discovered a higher term
                        if response.term > self.state.current_term:
                            await self.state.update_term(response.term)
                            self.transition_to_follower()
                            return
                    else:
                        logger.warning(f"Node {self.node_id} failed to get vote from {peer_node}: {response}")
            
            # Did not win election
            logger.info(f"Node {self.node_id} lost election with {votes_received} votes (needed {votes_needed})")
            
            # Transition back to follower and wait for next timeout
            if self.state.state == RaftNodeState.CANDIDATE.value:
                self.transition_to_follower()
                
        except Exception as e:
            logger.error(f"Error in election for node {self.node_id}: {e}")
            # On error, transition back to follower
            if self.state.state == RaftNodeState.CANDIDATE.value:
                self.transition_to_follower()
    
    async def _request_vote(self, node_id: str, request: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        """Request vote from a single node"""
        logger.debug(f"Node {self.node_id} requesting vote from {node_id}")
        return await self.rpc_client.request_vote(node_id, request)
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats as leader"""
        try:
            while self.running and self.state.state == RaftNodeState.LEADER.value:
                await self._send_heartbeats()
                await asyncio.sleep(self.heartbeat_interval)
                
        except asyncio.CancelledError:
            logger.debug(f"Heartbeat loop cancelled for node {self.node_id}")
            pass
    
    async def _send_heartbeats(self):
        """Send heartbeats to all followers"""
        if self.state.state != RaftNodeState.LEADER.value:
            return
        
        logger.debug(f"Leader {self.node_id} sending heartbeats")
        
        heartbeat_tasks = []
        for node_id in self.cluster_nodes:
            if node_id != self.node_id:
                task = self._send_append_entries(node_id)
                heartbeat_tasks.append(task)
        
        if heartbeat_tasks:
            await asyncio.gather(*heartbeat_tasks, return_exceptions=True)
        
        # Update commit index based on replication
        new_commit_index = self.state.calculate_commit_index(len(self.cluster_nodes))
        if new_commit_index > self.state.commit_index:
            self.state.commit_index = new_commit_index
            logger.debug(f"Updated commit index to {new_commit_index}")
            asyncio.create_task(self.apply_committed_entries())
    
    async def _send_append_entries(self, follower_id: str):
        """Send AppendEntries RPC to a follower"""
        if follower_id not in self.state.next_index:
            return
        
        # Get entries to send
        next_index = self.state.next_index[follower_id]
        entries_to_send = self.state.get_entries_from(next_index)
        
        # Use batched sender
        success = await self.batched_append.send_append_entries(
            follower_id, self.state, entries_to_send
        )
        
        if not success and not entries_to_send:
            # Send heartbeat if no entries and batching failed
            prev_log_index = next_index - 1
            prev_log_term = 0
            if prev_log_index > 0:
                prev_entry = self.state.get_log_entry(prev_log_index)
                prev_log_term = prev_entry.term if prev_entry else 0
            
            response = await self.rpc_client.send_heartbeat(
                follower_id,
                self.state.current_term,
                self.node_id,
                self.state.commit_index,
                prev_log_index,
                prev_log_term
            )
            
            if response and response.term > self.state.current_term:
                await self.state.update_term(response.term)
                self.transition_to_follower()
    
    async def _append_noop_entry(self):
        """Append a no-op entry when becoming leader"""
        noop_command = {
            "type": "noop",
            "leader_id": self.node_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        entry = LogEntry(
            term=self.state.current_term,
            index=self.state.get_last_log_index() + 1,
            command=noop_command
        )
        
        await self.state.append_entries([entry])
        logger.info(f"Appended no-op entry as new leader")
    
    async def _run_state_machine(self):
        """Background task to apply committed entries to state machine"""
        # Add initial delay to ensure everything is ready
        await asyncio.sleep(1.0)
        
        while self.running:
            try:
                # Check for committed entries to apply
                entries_to_apply = await self.state.apply_committed_entries()
                
                for entry in entries_to_apply:
                    if self.apply_command_callback and entry.command.get("type") != "noop":
                        try:
                            await self.apply_command_callback(entry.command)
                            logger.debug(f"Applied command from log entry {entry.index}")
                        except Exception as e:
                            logger.error(f"Failed to apply command: {e}")
                
                # Wait before checking again
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in state machine loop: {e}")
                await asyncio.sleep(0.1)
    
    async def apply_committed_entries(self):
        """Apply committed entries to the state machine"""
        entries_to_apply = await self.state.apply_committed_entries()
        
        for entry in entries_to_apply:
            if self.apply_command_callback and entry.command.get("type") != "noop":
                try:
                    await self.apply_command_callback(entry.command)
                    logger.debug(f"Applied command from log entry {entry.index}")
                except Exception as e:
                    logger.error(f"Failed to apply command: {e}")
    
    async def propose_command(self, command: Dict[str, Any]) -> bool:
        """
        Propose a new command to be added to the log (leader only)
        
        Args:
            command: Command to be executed on the state machine
            
        Returns:
            True if command was accepted, False otherwise
        """
        if self.state.state != RaftNodeState.LEADER.value:
            logger.warning(f"Cannot propose command - not leader (current leader: {self.current_leader})")
            return False
        
        # Create log entry
        entry = LogEntry(
            term=self.state.current_term,
            index=self.state.get_last_log_index() + 1,
            command=command
        )
        
        # Append to local log
        await self.state.append_entries([entry])
        
        logger.info(f"Leader appended command to log at index {entry.index}")
        
        # Trigger replication
        asyncio.create_task(self._send_heartbeats())
        
        return True
    
    def is_leader(self) -> bool:
        """Check if this node is the current leader"""
        return self.state.state == RaftNodeState.LEADER.value
    
    def get_leader_id(self) -> Optional[str]:
        """Get the ID of the current leader"""
        return self.current_leader
    
    def set_apply_command_callback(self, callback: Callable):
        """Set callback for applying committed commands to state machine"""
        self.apply_command_callback = callback