"""
Raft RPC Implementation
Handles RequestVote and AppendEntries RPCs with proper networking
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
import httpx
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class RequestVoteRequest:
    """RequestVote RPC request"""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    """RequestVote RPC response"""
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    """AppendEntries RPC request"""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict[str, Any]]  # Serialized LogEntry objects
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    """AppendEntries RPC response"""
    term: int
    success: bool
    match_index: Optional[int] = None  # Index of highest log entry known to be replicated


class RaftRPCClient:
    """Client for making RPC calls to other Raft nodes"""
    
    def __init__(self, node_id: str, timeout: float = 5.0):  # FIXED: Increased default timeout
        self.node_id = node_id
        self.timeout = timeout
        self.clients: Dict[str, httpx.AsyncClient] = {}
        
    async def initialize(self, cluster_config: Dict[str, str]):
        """Initialize HTTP clients for each node in the cluster"""
        for node_id, address in cluster_config.items():
            if node_id != self.node_id:
                # FIXED: Create client with better timeout configuration
                self.clients[node_id] = httpx.AsyncClient(
                    base_url=f"http://{address}",
                    timeout=httpx.Timeout(
                        total=self.timeout,
                        connect=5.0,  # Connection timeout
                        read=5.0,     # Read timeout
                        write=5.0     # Write timeout
                    ),
                    limits=httpx.Limits(
                        max_keepalive_connections=5,
                        max_connections=10,
                        keepalive_expiry=30.0
                    )
                )
        logger.info(f"Initialized RPC clients for {len(self.clients)} peers with timeout={self.timeout}s")
    
    async def close(self):
        """Close all HTTP clients"""
        for client in self.clients.values():
            await client.aclose()
    
    async def request_vote(self, target_node: str, request: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        """Send RequestVote RPC to target node"""
        if target_node not in self.clients:
            logger.error(f"No client configured for node {target_node}")
            return None
        
        try:
            logger.debug(f"Sending RequestVote from {self.node_id} to {target_node} for term {request.term}")
            response = await self.clients[target_node].post(
                "/raft/request_vote",
                json=asdict(request)
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"RequestVote response from {target_node}: granted={data.get('vote_granted')}, term={data.get('term')}")
                return RequestVoteResponse(**data)
            else:
                logger.warning(f"RequestVote to {target_node} failed with status {response.status_code}")
                return None
                
        except httpx.ConnectError as e:
            logger.debug(f"RequestVote to {target_node} failed - connection error: {e}")
            return None
        except httpx.TimeoutException as e:
            logger.debug(f"RequestVote to {target_node} failed - timeout: {e}")
            return None
        except Exception as e:
            logger.error(f"RequestVote to {target_node} failed with unexpected error: {e}")
            return None
    
    async def append_entries(self, target_node: str, request: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Send AppendEntries RPC to target node"""
        if target_node not in self.clients:
            logger.error(f"No client configured for node {target_node}")
            return None
        
        try:
            response = await self.clients[target_node].post(
                "/raft/append_entries",
                json=asdict(request)
            )
            
            if response.status_code == 200:
                data = response.json()
                return AppendEntriesResponse(**data)
            else:
                logger.warning(f"AppendEntries to {target_node} failed with status {response.status_code}")
                return None
                
        except httpx.ConnectError as e:
            logger.debug(f"AppendEntries to {target_node} failed - connection error: {e}")
            return None
        except httpx.TimeoutException as e:
            logger.debug(f"AppendEntries to {target_node} failed - timeout: {e}")
            return None
        except Exception as e:
            logger.error(f"AppendEntries to {target_node} failed with unexpected error: {e}")
            return None
    
    async def send_heartbeat(self, target_node: str, term: int, leader_id: str, 
                           leader_commit: int, prev_log_index: int, prev_log_term: int) -> Optional[AppendEntriesResponse]:
        """Send heartbeat (empty AppendEntries) to target node"""
        request = AppendEntriesRequest(
            term=term,
            leader_id=leader_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=[],
            leader_commit=leader_commit
        )
        return await self.append_entries(target_node, request)


class RaftRPCHandler:
    """Handler for incoming RPC requests"""
    
    def __init__(self, node):
        """Initialize with reference to RaftNode"""
        self.node = node  # Will be set when RaftNode is created
    
    async def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        """Handle incoming RequestVote RPC"""
        logger.debug(f"Node {self.node.node_id} handling RequestVote from {request.candidate_id} for term {request.term}")
        
        async with self.node.state._state_lock:
            current_term = self.node.state.current_term
            
            # Reply false if term < currentTerm
            if request.term < current_term:
                logger.debug(f"Rejecting vote request from {request.candidate_id}: "
                           f"outdated term {request.term} < {current_term}")
                return RequestVoteResponse(term=current_term, vote_granted=False)
            
            # Update term if necessary
            if request.term > current_term:
                await self.node.state.update_term(request.term)
                self.node.transition_to_follower()
            
            # Check if we've already voted
            if self.node.state.voted_for is not None and self.node.state.voted_for != request.candidate_id:
                logger.debug(f"Rejecting vote request from {request.candidate_id}: "
                           f"already voted for {self.node.state.voted_for}")
                return RequestVoteResponse(term=self.node.state.current_term, vote_granted=False)
            
            # Check log completeness
            last_log_index = self.node.state.get_last_log_index()
            last_log_term = self.node.state.get_last_log_term()
            
            log_is_current = (request.last_log_term > last_log_term or 
                            (request.last_log_term == last_log_term and 
                             request.last_log_index >= last_log_index))
            
            if log_is_current:
                # Grant vote
                await self.node.state.record_vote(request.candidate_id)
                self.node.reset_election_timer()
                logger.info(f"Node {self.node.node_id} granted vote to {request.candidate_id} for term {request.term}")
                return RequestVoteResponse(term=self.node.state.current_term, vote_granted=True)
            else:
                logger.debug(f"Rejecting vote request from {request.candidate_id}: "
                           f"candidate log not up-to-date")
                return RequestVoteResponse(term=self.node.state.current_term, vote_granted=False)
    
    async def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle incoming AppendEntries RPC"""
        async with self.node.state._state_lock:
            current_term = self.node.state.current_term
            
            # Reply false if term < currentTerm
            if request.term < current_term:
                logger.debug(f"Rejecting AppendEntries from {request.leader_id}: "
                           f"outdated term {request.term} < {current_term}")
                return AppendEntriesResponse(term=current_term, success=False)
            
            # Update term and convert to follower if necessary
            if request.term > current_term:
                await self.node.state.update_term(request.term)
            
            # Always transition to follower when receiving valid AppendEntries
            if self.node.state.state != "follower":
                self.node.transition_to_follower()
            
            # Reset election timer
            self.node.reset_election_timer()
            self.node.current_leader = request.leader_id
            
            # FIXED: Log heartbeat reception
            if not request.entries:
                logger.debug(f"Node {self.node.node_id} received heartbeat from leader {request.leader_id}")
            
            # Check if we have the previous log entry
            if request.prev_log_index > 0:
                prev_entry = self.node.state.get_log_entry(request.prev_log_index)
                if not prev_entry or prev_entry.term != request.prev_log_term:
                    logger.debug(f"Log consistency check failed at index {request.prev_log_index}")
                    return AppendEntriesResponse(
                        term=self.node.state.current_term, 
                        success=False,
                        match_index=self.node.state.get_last_log_index()
                    )
            
            # Delete conflicting entries and append new ones
            if request.entries:
                # Find the point of divergence
                new_entries = []
                for i, entry_dict in enumerate(request.entries):
                    entry_index = request.prev_log_index + i + 1
                    existing_entry = self.node.state.get_log_entry(entry_index)
                    
                    if existing_entry and existing_entry.term != entry_dict["term"]:
                        # Delete this and all following entries
                        await self.node.state.delete_conflicting_entries(entry_index)
                        new_entries = request.entries[i:]
                        break
                    elif not existing_entry:
                        new_entries = request.entries[i:]
                        break
                
                # Append new entries
                if new_entries:
                    from .state import LogEntry
                    entries_to_append = [LogEntry.from_dict(e) for e in new_entries]
                    await self.node.state.append_entries(entries_to_append)
            
            # Update commit index
            if request.leader_commit > self.node.state.commit_index:
                self.node.state.commit_index = min(
                    request.leader_commit,
                    self.node.state.get_last_log_index()
                )
                # Trigger state machine application
                asyncio.create_task(self.node.apply_committed_entries())
            
            return AppendEntriesResponse(
                term=self.node.state.current_term,
                success=True,
                match_index=self.node.state.get_last_log_index()
            )


class BatchedAppendEntries:
    """Handles batching and flow control for AppendEntries RPCs"""
    
    def __init__(self, rpc_client: RaftRPCClient, max_batch_size: int = 100):
        self.rpc_client = rpc_client
        self.max_batch_size = max_batch_size
        self.in_flight: Dict[str, bool] = {}
        
    async def send_append_entries(self, follower_id: str, state, entries_to_send: List) -> bool:
        """Send AppendEntries with batching and flow control"""
        # Check if we already have a request in flight
        if self.in_flight.get(follower_id, False):
            return False
        
        self.in_flight[follower_id] = True
        
        try:
            # Batch entries
            batched_entries = entries_to_send[:self.max_batch_size]
            
            # Prepare request
            prev_log_index = state.next_index[follower_id] - 1
            prev_log_term = 0
            if prev_log_index > 0:
                prev_entry = state.get_log_entry(prev_log_index)
                prev_log_term = prev_entry.term if prev_entry else 0
            
            request = AppendEntriesRequest(
                term=state.current_term,
                leader_id=state.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=[e.to_dict() for e in batched_entries],
                leader_commit=state.commit_index
            )
            
            # Send request
            response = await self.rpc_client.append_entries(follower_id, request)
            
            if response:
                if response.success:
                    # Update follower progress
                    new_match_index = prev_log_index + len(batched_entries)
                    state.update_follower_progress(follower_id, new_match_index)
                    return True
                else:
                    # Handle failure - decrement nextIndex
                    if response.term > state.current_term:
                        # We're no longer leader
                        await state.update_term(response.term)
                        return False
                    else:
                        # Log inconsistency - decrement nextIndex
                        state.next_index[follower_id] = max(1, state.next_index[follower_id] - 1)
                        logger.debug(f"Decremented nextIndex for {follower_id} to {state.next_index[follower_id]}")
            
            return False
            
        finally:
            self.in_flight[follower_id] = False