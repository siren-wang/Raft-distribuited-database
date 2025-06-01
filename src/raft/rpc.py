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
        
        # Update term if necessary - use shorter timeout
        if request.term > current_term:
            try:
                await asyncio.wait_for(
                    self.node.state.update_term(request.term),
                    timeout=2.0  # Short timeout to avoid blocking elections
                )
                current_term = self.node.state.current_term
            except asyncio.TimeoutError:
                logger.warning(f"Vote handler timed out updating term from {request.candidate_id}, rejecting vote")
                return RequestVoteResponse(term=current_term, vote_granted=False)
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
            # Grant vote - use a shorter timeout to avoid blocking elections
            try:
                await asyncio.wait_for(
                    self.node.state.record_vote(request.candidate_id),
                    timeout=2.0  # Short timeout to avoid blocking elections
                )
                self.node.reset_election_timer()
                logger.info(f"Node {self.node.node_id} granted vote to {request.candidate_id} for term {request.term}")
                return RequestVoteResponse(term=self.node.state.current_term, vote_granted=True)
            except asyncio.TimeoutError:
                logger.warning(f"Vote handler timed out recording vote for {request.candidate_id}, rejecting vote")
                return RequestVoteResponse(term=current_term, vote_granted=False)
            except Exception as e:
                logger.error(f"Failed to record vote in vote handler: {e}")
                return RequestVoteResponse(term=current_term, vote_granted=False)
        else:
            logger.info(f"Rejecting vote request from {request.candidate_id}: "
                       f"candidate log not up-to-date")
            return RequestVoteResponse(term=current_term, vote_granted=False)
    
    async def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle incoming AppendEntries RPC"""
        # Use a non-blocking approach to avoid deadlocks
        current_term = self.node.state.current_term
        
        # Reply false if term < currentTerm
        if request.term < current_term:
            logger.debug(f"Rejecting AppendEntries from {request.leader_id}: "
                       f"outdated term {request.term} < {current_term}")
            return AppendEntriesResponse(term=current_term, success=False)
        
        # Update term and convert to follower if necessary - use shorter timeout
        if request.term > current_term:
            try:
                await asyncio.wait_for(
                    self.node.state.update_term(request.term),
                    timeout=2.0  # Short timeout to avoid blocking elections
                )
                current_term = self.node.state.current_term
            except asyncio.TimeoutError:
                logger.warning(f"AppendEntries handler timed out updating term from {request.leader_id}")
                return AppendEntriesResponse(term=current_term, success=False)
            except Exception as e:
                logger.error(f"Failed to update term in append_entries handler: {e}")
                return AppendEntriesResponse(term=current_term, success=False)
        
        # Always transition to follower when receiving valid AppendEntries
        if self.node.state.state != "follower":
            self.node.transition_to_follower()
        
        # Reset election timer
        self.node.reset_election_timer()
        self.node.current_leader = request.leader_id
        
        # FIXED: Log heartbeat reception
        if not request.entries:
            logger.debug(f"Node {self.node.node_id} received heartbeat from leader {request.leader_id}")
        
        # FIXED: Simplified log consistency check and repair
        try:
            # Get current log state
            last_log_index = self.node.state.get_last_log_index()
            
            logger.debug(f"AppendEntries from {request.leader_id}: prev_log_index={request.prev_log_index}, prev_log_term={request.prev_log_term}, entries={len(request.entries)}, last_log_index={last_log_index}")
            
            # Check if we have the previous log entry
            if request.prev_log_index > 0:
                if request.prev_log_index > last_log_index:
                    # We're missing entries - return our last log index for repair
                    logger.debug(f"Missing entries: prev_log_index={request.prev_log_index}, last_log_index={last_log_index}")
                    return AppendEntriesResponse(
                        term=self.node.state.current_term, 
                        success=False,
                        match_index=last_log_index
                    )
                
                prev_entry = self.node.state.get_log_entry(request.prev_log_index)
                if not prev_entry or prev_entry.term != request.prev_log_term:
                    # Log inconsistency - find the highest matching index
                    match_index = 0
                    for i in range(min(request.prev_log_index, last_log_index), 0, -1):
                        entry = self.node.state.get_log_entry(i)
                        if entry and entry.term <= request.prev_log_term:
                            match_index = i
                            break
                    
                    logger.debug(f"Log consistency check failed at index {request.prev_log_index}, match_index={match_index}")
                    return AppendEntriesResponse(
                        term=self.node.state.current_term, 
                        success=False,
                        match_index=match_index
                    )
            else:
                logger.debug(f"AppendEntries with prev_log_index=0, proceeding to append entries")
                # FIXED: When prev_log_index=0, we should truncate our entire log if we have entries
                # This handles the case where a new leader has fewer entries than us
                if last_log_index > 0:
                    logger.info(f"Truncating entire log (had {last_log_index} entries) to match new leader")
                    async with self.node.state._state_lock:
                        self.node.state.log = []
                        # Don't save persistent state since it's disabled for testing
            
            # Process entries if any
            if request.entries:
                logger.debug(f"Processing {len(request.entries)} entries")
                # FIXED: Much longer timeout to handle large batches and persistent state saving
                timeout_seconds = max(10.0, len(request.entries) * 0.1)  # 10s base + 100ms per entry
                try:
                    # FIXED: Try to acquire lock with timeout to avoid deadlocks
                    logger.debug(f"Attempting to acquire state lock for {len(request.entries)} entries with timeout {timeout_seconds}s")
                    
                    # Use asyncio.wait_for with the lock acquisition
                    async def _process_with_lock():
                        async with self.node.state._state_lock:
                            logger.debug(f"Acquired state lock, processing entries")
                            # Delete conflicting entries and append new ones
                            new_entries = []
                            for i, entry_dict in enumerate(request.entries):
                                entry_index = request.prev_log_index + i + 1
                                existing_entry = self.node.state.get_log_entry(entry_index)
                                
                                if existing_entry and existing_entry.term != entry_dict["term"]:
                                    # Delete this and all following entries
                                    logger.debug(f"Deleting conflicting entries from index {entry_index}")
                                    await self.node.state.delete_conflicting_entries(entry_index)
                                    new_entries = request.entries[i:]
                                    break
                                elif not existing_entry:
                                    new_entries = request.entries[i:]
                                    break
                            
                            # Append new entries
                            if new_entries:
                                logger.debug(f"Appending {len(new_entries)} new entries")
                                from .state import LogEntry
                                entries_to_append = [LogEntry.from_dict(e) for e in new_entries]
                                await self.node.state.append_entries(entries_to_append)
                                logger.info(f"Appended {len(entries_to_append)} entries starting at index {request.prev_log_index + 1}")
                            else:
                                logger.debug(f"No new entries to append")
                            logger.debug(f"Finished processing entries, releasing state lock")
                    
                    await asyncio.wait_for(_process_with_lock(), timeout=timeout_seconds)
                    
                except asyncio.TimeoutError:
                    logger.error(f"AppendEntries handler timed out after {timeout_seconds}s processing {len(request.entries)} entries from {request.leader_id}")
                    return AppendEntriesResponse(
                        term=current_term, 
                        success=False, 
                        match_index=self.node.state.get_last_log_index()
                    )
            else:
                logger.debug(f"No entries to process (heartbeat)")
            
            # Update commit index
            if request.leader_commit > self.node.state.commit_index:
                old_commit = self.node.state.commit_index
                self.node.state.commit_index = min(
                    request.leader_commit,
                    self.node.state.get_last_log_index()
                )
                if self.node.state.commit_index > old_commit:
                    logger.debug(f"Updated commit index from {old_commit} to {self.node.state.commit_index}")
                    # Trigger state machine application
                    asyncio.create_task(self.node.apply_committed_entries())
            
            final_match_index = self.node.state.get_last_log_index()
            logger.debug(f"AppendEntries success, returning match_index={final_match_index}")
            return AppendEntriesResponse(
                term=self.node.state.current_term,
                success=True,
                match_index=final_match_index
            )
        
        except asyncio.TimeoutError:
            logger.warning(f"AppendEntries handler timed out processing {len(request.entries) if request.entries else 0} entries from {request.leader_id}")
            # Return current state even on timeout to help leader make progress
            return AppendEntriesResponse(
                term=current_term, 
                success=False, 
                match_index=self.node.state.get_last_log_index()
            )
        except Exception as e:
            logger.error(f"Error in append_entries handler: {e}", exc_info=True)
            return AppendEntriesResponse(
                term=current_term, 
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
        # Check if we already have a request in flight
        if self.in_flight.get(follower_id, False):
            logger.info(f"BatchedAppendEntries: Request already in flight for {follower_id}")
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
            
            logger.info(f"BatchedAppendEntries: Sending to {follower_id}, entries={len(batched_entries)}, prev_log_index={prev_log_index}")
            
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
                logger.info(f"BatchedAppendEntries: Response from {follower_id}: success={response.success}, match_index={response.match_index}")
                if response.success:
                    # Update follower progress
                    new_match_index = prev_log_index + len(batched_entries)
                    state.update_follower_progress(follower_id, new_match_index)
                    logger.info(f"BatchedAppendEntries: Successfully replicated {len(batched_entries)} entries to {follower_id}, match_index={new_match_index}")
                    return True
                else:
                    # Handle failure - use more aggressive backoff for faster convergence
                    if response.term > state.current_term:
                        # We're no longer leader
                        await state.update_term(response.term)
                        return False
                    else:
                        # Log inconsistency - use binary search approach for faster convergence
                        current_next = state.next_index[follower_id]
                        if response.match_index is not None and response.match_index >= 0:
                            # Follower told us their highest matching index
                            state.next_index[follower_id] = response.match_index + 1
                            logger.info(f"BatchedAppendEntries: Set nextIndex for {follower_id} to {response.match_index + 1} based on match_index")
                        else:
                            # Use more conservative backoff - decrement by 1 for safety
                            if current_next > 1:
                                new_next = current_next - 1
                                state.next_index[follower_id] = new_next
                                logger.info(f"BatchedAppendEntries: Conservative backoff for {follower_id}: {current_next} -> {new_next}")
                            else:
                                state.next_index[follower_id] = 1
                                logger.info(f"BatchedAppendEntries: Set nextIndex for {follower_id} to 1 (minimum)")
            
            return False
            
        finally:
            self.in_flight[follower_id] = False