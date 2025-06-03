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
    
    def __init__(self, node_id: str, timeout: float = 10.0):  # FIXED: Longer default timeout
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
                        connect=5.0,    # Connection timeout
                        read=self.timeout,     # Read timeout
                        write=5.0,      # Write timeout
                        pool=self.timeout      # Pool timeout
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
        current_term = self.node.state.current_term
        
        # Reply false if term < currentTerm
        if request.term < current_term:
            logger.debug(f"Rejecting vote request from {request.candidate_id}: "
                       f"outdated term {request.term} < {current_term}")
            return RequestVoteResponse(term=current_term, vote_granted=False)
        
        # Update term if necessary - use lock-free method for elections
        if request.term > current_term:
            try:
                # Use lock-free method to prevent deadlocks
                await self.node.state.update_term_for_election(request.term)
                current_term = self.node.state.current_term
                logger.info(f"Updated term to {request.term} for vote from {request.candidate_id} (lock-free)")
            except Exception as e:
                logger.error(f"Failed to update term in vote handler: {e}")
                return RequestVoteResponse(term=current_term, vote_granted=False)
        
        # Check if we already voted for someone else
        voted_for = self.node.state.voted_for
        if voted_for is not None and voted_for != request.candidate_id:
            logger.debug(f"Rejecting vote request from {request.candidate_id}: "
                       f"already voted for {voted_for}")
            return RequestVoteResponse(term=current_term, vote_granted=False)
        
        # Check log completeness with better debugging
        last_log_index = self.node.state.get_last_log_index()
        last_log_term = self.node.state.get_last_log_term()
        
        # FIXED: More lenient log completeness check for initial elections
        log_is_current = (request.last_log_term > last_log_term or 
                        (request.last_log_term == last_log_term and 
                         request.last_log_index >= last_log_index))
        
        logger.info(f"Vote request from {request.candidate_id}: "
                   f"candidate_log=({request.last_log_index}, {request.last_log_term}), "
                   f"my_log=({last_log_index}, {last_log_term}), "
                   f"log_current={log_is_current}")
        
        # FIXED: Allow votes for empty logs to help initial leader election
        if last_log_index == 0 or log_is_current:
            # Grant vote - use lock-free method to avoid blocking elections
            try:
                await self.node.state.record_vote_for_election(request.candidate_id)
                self.node.reset_election_timer()
                logger.info(f"Node {self.node.node_id} granted vote to {request.candidate_id} for term {request.term} (lock-free)")
                return RequestVoteResponse(term=self.node.state.current_term, vote_granted=True)
            except Exception as e:
                logger.error(f"Failed to record vote in vote handler: {e}")
                return RequestVoteResponse(term=current_term, vote_granted=False)
        else:
            logger.info(f"Rejecting vote request from {request.candidate_id}: "
                       f"candidate log not up-to-date")
            return RequestVoteResponse(term=current_term, vote_granted=False)
    
    async def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle incoming AppendEntries RPC"""
        current_term = self.node.state.current_term
        
        # Reply false if term < currentTerm
        if request.term < current_term:
            logger.debug(f"Rejecting AppendEntries from {request.leader_id}: "
                    f"outdated term {request.term} < {current_term}")
            return AppendEntriesResponse(
                term=current_term, 
                success=False,
                match_index=self.node.state.get_last_log_index()
            )
        
        # Update term if necessary
        if request.term > current_term:
            try:
                await asyncio.wait_for(
                    self.node.state.update_term(request.term),
                    timeout=2.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"AppendEntries handler timed out updating term")
                return AppendEntriesResponse(
                    term=current_term, 
                    success=False,
                    match_index=self.node.state.get_last_log_index()
                )
        
        # Always transition to follower when receiving valid AppendEntries
        if self.node.state.state != "follower":
            self.node.transition_to_follower()
        
        # Reset election timer and record leader
        self.node.reset_election_timer()
        self.node.current_leader = request.leader_id
        
        # Handle heartbeat
        if not request.entries:
            logger.debug(f"Node {self.node.node_id} received heartbeat from leader {request.leader_id}")
            # Still need to update commit index for heartbeats
            if request.leader_commit > self.node.state.commit_index:
                old_commit = self.node.state.commit_index
                self.node.state.commit_index = min(
                    request.leader_commit,
                    self.node.state.get_last_log_index()
                )
                if self.node.state.commit_index > old_commit:
                    logger.info(f"Updated commit index from {old_commit} to {self.node.state.commit_index} via heartbeat")
                    asyncio.create_task(self.node.apply_committed_entries())
            
            return AppendEntriesResponse(
                term=self.node.state.current_term,
                success=True,
                match_index=self.node.state.get_last_log_index()
            )
        
        # FIXED: More robust log consistency check
        try:
            last_log_index = self.node.state.get_last_log_index()
            
            # FIX 1: Handle the case where prev_log_index > last_log_index
            if request.prev_log_index > last_log_index:
                logger.info(f"Missing entries: follower has {last_log_index}, leader expects {request.prev_log_index}")
                return AppendEntriesResponse(
                    term=self.node.state.current_term,
                    success=False,
                    match_index=last_log_index  # Tell leader what we actually have
                )
            
            # FIX 2: Check log consistency at prev_log_index
            if request.prev_log_index > 0:
                prev_entry = self.node.state.get_log_entry(request.prev_log_index)
                if not prev_entry:
                    logger.warning(f"Missing entry at index {request.prev_log_index}")
                    # Find the last entry we do have
                    match_index = 0
                    for i in range(request.prev_log_index - 1, 0, -1):
                        if self.node.state.get_log_entry(i):
                            match_index = i
                            break
                    return AppendEntriesResponse(
                        term=self.node.state.current_term,
                        success=False,
                        match_index=match_index
                    )
                
                if prev_entry.term != request.prev_log_term:
                    logger.info(f"Term mismatch at index {request.prev_log_index}: "
                            f"expected {request.prev_log_term}, got {prev_entry.term}")
                    # FIX 3: Find the last matching entry
                    match_index = 0
                    for i in range(request.prev_log_index - 1, 0, -1):
                        entry = self.node.state.get_log_entry(i)
                        if entry and entry.term <= request.prev_log_term:
                            match_index = i
                            break
                    
                    # Delete conflicting entries
                    await self.node.state.delete_conflicting_entries(request.prev_log_index)
                    
                    return AppendEntriesResponse(
                        term=self.node.state.current_term,
                        success=False,
                        match_index=match_index
                    )
            
            # FIX 4: Process entries more carefully
            if request.entries:
                async with self.node.state._state_lock:
                    # Find where to start appending
                    start_index = request.prev_log_index + 1
                    new_entries = []
                    
                    for i, entry_dict in enumerate(request.entries):
                        entry_index = start_index + i
                        existing_entry = self.node.state.get_log_entry(entry_index)
                        
                        if existing_entry:
                            if existing_entry.term != entry_dict["term"]:
                                # Conflict - delete this and all following entries
                                logger.info(f"Deleting conflicting entries from index {entry_index}")
                                await self.node.state.delete_conflicting_entries(entry_index)
                                new_entries = request.entries[i:]
                                break
                        else:
                            # No existing entry - append remaining entries
                            new_entries = request.entries[i:]
                            break
                    
                    # Append new entries
                    if new_entries:
                        from .state import LogEntry
                        entries_to_append = [LogEntry.from_dict(e) for e in new_entries]
                        await self.node.state.append_entries(entries_to_append)
                        logger.info(f"Appended {len(entries_to_append)} entries")
            
            # Update commit index
            if request.leader_commit > self.node.state.commit_index:
                old_commit = self.node.state.commit_index
                self.node.state.commit_index = min(
                    request.leader_commit,
                    self.node.state.get_last_log_index()
                )
                if self.node.state.commit_index > old_commit:
                    logger.info(f"Updated commit index from {old_commit} to {self.node.state.commit_index}")
                    asyncio.create_task(self.node.apply_committed_entries())
            
            final_match_index = self.node.state.get_last_log_index()
            return AppendEntriesResponse(
                term=self.node.state.current_term,
                success=True,
                match_index=final_match_index
            )
            
        except Exception as e:
            logger.error(f"Error in append_entries handler: {e}", exc_info=True)
            return AppendEntriesResponse(
                term=self.node.state.current_term,
                success=False,
                match_index=self.node.state.get_last_log_index()
            )


class BatchedAppendEntries:
    """Handles batching and flow control for AppendEntries RPCs"""
    
    def __init__(self, rpc_client: RaftRPCClient, max_batch_size: int = 5):  # FIXED: Even smaller batch size to prevent timeouts
        self.rpc_client = rpc_client
        self.max_batch_size = max_batch_size
        self.in_flight: Dict[str, bool] = {}
        
    async def send_append_entries(self, follower_id: str, state, entries_to_send: List) -> bool:
        """Send AppendEntries with batching and flow control"""
        if self.in_flight.get(follower_id, False):
            logger.debug(f"Request already in flight for {follower_id}")
            return False
        
        self.in_flight[follower_id] = True
        
        try:
            # Get current indices
            next_index = state.next_index.get(follower_id, 1)
            prev_log_index = next_index - 1
            prev_log_term = 0
            
            if prev_log_index > 0:
                prev_entry = state.get_log_entry(prev_log_index)
                if prev_entry:
                    prev_log_term = prev_entry.term
                else:
                    # FIX: If we don't have the prev entry, we need to go back further
                    logger.warning(f"Missing entry at prev_log_index {prev_log_index}")
                    state.next_index[follower_id] = max(1, prev_log_index)
                    return False
            
            # Batch entries
            batched_entries = entries_to_send[:self.max_batch_size] if entries_to_send else []
            
            logger.info(f"Sending AppendEntries to {follower_id}: "
                    f"prev_index={prev_log_index}, entries={len(batched_entries)}")
            
            request = AppendEntriesRequest(
                term=state.current_term,
                leader_id=state.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=[e.to_dict() for e in batched_entries],
                leader_commit=state.commit_index
            )
            
            response = await self.rpc_client.append_entries(follower_id, request)
            
            if response:
                if response.success:
                    # Update follower progress
                    new_match_index = prev_log_index + len(batched_entries)
                    state.update_follower_progress(follower_id, new_match_index)
                    logger.info(f"Successfully replicated to {follower_id}, match_index={new_match_index}")
                    return True
                else:
                    # Handle failure
                    if response.term > state.current_term:
                        await state.update_term(response.term)
                        return False
                    
                    # FIX: Use the match_index from response to update next_index
                    if response.match_index is not None:
                        # Set next_index to one past the last matching entry
                        new_next_index = response.match_index + 1
                        if new_next_index < state.next_index[follower_id]:
                            state.next_index[follower_id] = new_next_index
                            logger.info(f"Updated next_index for {follower_id} to {new_next_index} based on match_index")
                    else:
                        # Conservative backoff
                        state.next_index[follower_id] = max(1, state.next_index[follower_id] - 1)
                        logger.info(f"Decremented next_index for {follower_id} to {state.next_index[follower_id]}")
            
            return False
            
        finally:
            self.in_flight[follower_id] = False