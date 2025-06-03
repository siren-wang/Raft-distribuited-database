"""
Raft Node Implementation
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
                 election_timeout_range: Tuple[float, float] = (5.0, 10.0),  # FIXED: Realistic timeouts in seconds
                 heartbeat_interval: float = 1.0):  # FIXED: Reasonable heartbeat interval in seconds
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
        self.rpc_client = RaftRPCClient(node_id, timeout=10.0)  # FIXED: Longer timeout
        self.rpc_handler = RaftRPCHandler(self)
        self.batched_append = BatchedAppendEntries(self.rpc_client)
        
        # Timing configuration - now using seconds directly
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
        
        # FIXED: Add randomized startup delay to prevent simultaneous elections
        self._startup_delay = random.uniform(1.0, 3.0)  # Increased startup delay
        
        logger.info(f"Initialized RaftNode {node_id} with {len(cluster_config)} nodes in cluster")
        logger.info(f"Election timeout range: {self.election_timeout_range[0]:.1f}-{self.election_timeout_range[1]:.1f}s, Heartbeat interval: {self.heartbeat_interval:.1f}s")
        logger.info(f"Startup delay: {self._startup_delay:.2f}s")
    
    async def start(self):
        """Start the Raft node"""
        logger.info(f"Starting Raft node {self.node_id}")
        
        # Load persistent state
        await self.state.load_persistent_state()
        
        # Initialize RPC client
        await self.rpc_client.initialize(self.cluster_config)
        
        # FIXED: Add randomized startup delay to prevent simultaneous elections
        logger.info(f"Waiting {self._startup_delay:.2f}s before starting to prevent simultaneous elections...")
        await asyncio.sleep(self._startup_delay)
        
        # FIXED: Add additional delay to ensure all nodes are up
        logger.info(f"Waiting for cluster to stabilize...")
        await asyncio.sleep(2.0)
        
        self.running = True

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
        
        # Start election with proper error handling
        election_task = asyncio.create_task(self._run_election())
        # Add callback to log any unhandled exceptions
        election_task.add_done_callback(self._handle_election_task_done)
        
        logger.debug(f"Created election task for node {self.node_id}")
    
    def _handle_election_task_done(self, task: asyncio.Task):
        """Handle completion of election task"""
        try:
            if task.exception():
                logger.error(f"Election task failed for node {self.node_id}: {task.exception()}", exc_info=task.exception())
            else:
                logger.debug(f"Election task completed for node {self.node_id}")
        except Exception as e:
            logger.error(f"Error handling election task completion for node {self.node_id}: {e}")
    
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
        
        if self.running and self.state.state != RaftNodeState.LEADER.value:
            # Use proper randomization to prevent split votes  
            base_timeout = random.uniform(*self.election_timeout_range)
            # Add a node-specific offset to spread out elections even more
            node_offset = hash(self.node_id) % 1000 / 1000  # 0-1s offset based on node ID
            timeout = base_timeout + node_offset
            
            logger.debug(f"Setting election timeout for {self.node_id}: {timeout:.3f}s")
            self.election_timer_task = asyncio.create_task(self._election_timeout(timeout))
    
    async def _election_timeout(self, timeout: float):
        """Handle election timeout"""
        try:
            await asyncio.sleep(timeout)
            
            if self.state.state == RaftNodeState.FOLLOWER.value:
                logger.info(f"Election timeout on node {self.node_id} after {timeout:.3f}s")
                self.transition_to_candidate()
                
        except asyncio.CancelledError:
            logger.debug(f"Election timer cancelled for node {self.node_id}")
            pass
    
    async def _run_election(self):
        """Run leader election"""
        logger.info(f"Node {self.node_id} _run_election method starting")
        try:
            # Increment current term and vote for self with timeout handling
            logger.info(f"Node {self.node_id} updating term and voting for self")
            
            try:
                # Use lock-free methods for elections to prevent deadlocks
                await self.state.update_term_for_election(self.state.current_term + 1)
                await self.state.record_vote_for_election(self.node_id)
            except Exception as e:
                logger.error(f"Node {self.node_id} failed during lock-free term update/vote: {e}")
                # If we can't update our own state, transition back to follower and try again
                if self.state.state == RaftNodeState.CANDIDATE.value:
                    await asyncio.sleep(random.uniform(1.0, 3.0))
                    self.transition_to_follower()
                return
            
            logger.info(f"Node {self.node_id} starting election for term {self.state.current_term}")
            
            # Reset election timer
            self.reset_election_timer()
            
            # Request votes from all other nodes
            votes_received = 1  # Vote for self
            votes_needed = (len(self.cluster_nodes) + 1) // 2
            
            logger.info(f"Node {self.node_id} needs {votes_needed} votes to win election (cluster size: {len(self.cluster_nodes)})")
            
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
            
            # Wait for votes with timeout
            if vote_requests:
                logger.info(f"Node {self.node_id} sending {len(vote_requests)} vote requests")
                try:
                    responses = await asyncio.wait_for(
                        asyncio.gather(*vote_requests, return_exceptions=True),
                        timeout=8.0  # Reasonable timeout for vote collection
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Node {self.node_id} vote collection timed out, retrying later")
                    if self.state.state == RaftNodeState.CANDIDATE.value:
                        await asyncio.sleep(random.uniform(0.5, 1.5))
                        self.transition_to_follower()
                    return
                
                # FIXED: Better logging and error handling
                none_count = 0
                exception_count = 0
                response_count = 0
                
                for i, response in enumerate(responses):
                    peer_node = [n for n in self.cluster_nodes if n != self.node_id][i]
                    
                    if response is None:
                        none_count += 1
                        logger.warning(f"Node {self.node_id} got None response from {peer_node}")
                    elif isinstance(response, Exception):
                        exception_count += 1
                        logger.warning(f"Node {self.node_id} got exception from {peer_node}: {response}")
                    elif isinstance(response, RequestVoteResponse):
                        response_count += 1
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
                            logger.info(f"Node {self.node_id} discovered higher term {response.term}, stepping down")
                            try:
                                # Use lock-free method for term updates
                                await self.state.update_term_for_election(response.term)
                            except Exception as e:
                                logger.error(f"Node {self.node_id} failed updating to higher term {response.term}: {e}")
                            self.transition_to_follower()
                            return
                    else:
                        logger.warning(f"Node {self.node_id} got unexpected response type from {peer_node}: {type(response)}")
                
                logger.info(f"Election results for {self.node_id}: {response_count} valid responses, {none_count} None, {exception_count} exceptions")
                
                # FIXED: More aggressive retry for network partitions
                if response_count == 0:
                    logger.error(f"Node {self.node_id} failed to get any valid responses during election")
                    if self.state.state == RaftNodeState.CANDIDATE.value:
                        # For network partitions, retry more quickly
                        await asyncio.sleep(random.uniform(0.2, 0.8))
                        self.transition_to_follower()
                    return
            
            # Did not win election
            logger.info(f"Node {self.node_id} lost election with {votes_received} votes (needed {votes_needed})")
            
            # Transition back to follower and wait for next timeout
            if self.state.state == RaftNodeState.CANDIDATE.value:
                # FIXED: Add small delay before transitioning to prevent rapid cycling
                await asyncio.sleep(random.uniform(0.5, 1.5))
                self.transition_to_follower()
                
        except Exception as e:
            logger.error(f"Error in election for node {self.node_id}: {e}", exc_info=True)
            # On error, transition back to follower
            if self.state.state == RaftNodeState.CANDIDATE.value:
                # FIXED: Add small delay before transitioning to prevent rapid cycling
                await asyncio.sleep(random.uniform(0.5, 1.5))
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
            results = await asyncio.gather(*heartbeat_tasks, return_exceptions=True)
            
            # Count successful heartbeats
            success_count = sum(1 for r in results if r is True)
            logger.debug(f"Heartbeat round: {success_count}/{len(heartbeat_tasks)} successful")
        
        # Always try to update commit index after heartbeats
        old_commit = self.state.commit_index
        new_commit_index = self.state.calculate_commit_index(len(self.cluster_nodes))
        
        if new_commit_index > old_commit:
            self.state.commit_index = new_commit_index
            logger.info(f"Advanced commit index: {old_commit} -> {new_commit_index}")
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
        
        # FIXED: Immediately check for commit index updates after successful replication
        if success:
            new_commit_index = self.state.calculate_commit_index(len(self.cluster_nodes))
            if new_commit_index > self.state.commit_index:
                old_commit = self.state.commit_index
                self.state.commit_index = new_commit_index
                logger.info(f"Updated commit index from {old_commit} to {new_commit_index} after successful replication to {follower_id}")
                asyncio.create_task(self.apply_committed_entries())
        
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
            
            if response:
                if response.success:
                    # Update follower progress with match_index from response
                    if response.match_index is not None:
                        self.state.update_follower_progress(follower_id, response.match_index)
                        logger.info(f"Updated match_index for {follower_id} to {response.match_index} from heartbeat")
                        
                        # FIXED: Check commit index after heartbeat success too
                        new_commit_index = self.state.calculate_commit_index(len(self.cluster_nodes))
                        if new_commit_index > self.state.commit_index:
                            old_commit = self.state.commit_index
                            self.state.commit_index = new_commit_index
                            logger.info(f"Updated commit index from {old_commit} to {new_commit_index} after heartbeat to {follower_id}")
                            asyncio.create_task(self.apply_committed_entries())
                elif response.term > self.state.current_term:
                    # We're no longer leader
                    await self.state.update_term(response.term)
                    self.transition_to_follower()
                else:
                    # Heartbeat failed - might need to decrement nextIndex
                    logger.info(f"Heartbeat to {follower_id} failed")
    
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
        # FIXED: Add initial delay to ensure everything is ready
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
        
        logger.info(f"Leader appended command to log at index {entry.index}, current commit_index={self.state.commit_index}")
        logger.info(f"Current match_index state: {self.state.match_index}")
        
        # Trigger immediate replication to all followers
        await self._send_heartbeats()
        
        return True
    
    def is_leader(self) -> bool:
        """Check if this node is the current leader"""
        return self.state.state == RaftNodeState.LEADER.value
    
    def get_leader_id(self) -> Optional[str]:
        """Get the ID of the current leader"""
        # If we are the leader, always return our own ID
        if self.state.state == RaftNodeState.LEADER.value:
            return self.node_id
        # Otherwise return who we think is the leader
        return self.current_leader
    
    def set_apply_command_callback(self, callback: Callable):
        """Set callback for applying committed commands to state machine"""
        self.apply_command_callback = callback