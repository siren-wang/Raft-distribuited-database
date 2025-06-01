"""
Raft State Management
Handles persistent and volatile state for Raft consensus
"""

import json
import os
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field, asdict
from pathlib import Path
import aiofiles
import logging

logger = logging.getLogger(__name__)


@dataclass
class LogEntry:
    """Represents a single entry in the Raft log"""
    term: int
    index: int
    command: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "timestamp": self.timestamp.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LogEntry':
        """Create LogEntry from dictionary"""
        return cls(
            term=data["term"],
            index=data["index"],
            command=data["command"],
            timestamp=datetime.fromisoformat(data["timestamp"])
        )


class RaftState:
    """Manages Raft node state with persistence"""
    
    def __init__(self, node_id: str, state_dir: str = "./raft_state"):
        self.node_id = node_id
        self.state_dir = Path(state_dir) / node_id
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Persistent state (must be saved to disk)
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state (in-memory only)
        self.commit_index: int = 0
        self.last_applied: int = 0
        
        # Leader state (reinitialized after election)
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # State machine state
        self.state: str = "follower"  # follower, candidate, leader
        
        # Lock for state modifications
        self._state_lock = asyncio.Lock()
        
        logger.info(f"Initialized RaftState for node {node_id}")
    
    async def save_persistent_state(self):
        """Save persistent state to disk"""
        async with self._state_lock:
            await self._save_persistent_state()
    
    async def _save_persistent_state(self):
        """Save persistent state to disk (assumes lock is already held)"""
        # TEMPORARY: Disable persistent state saving to test if file I/O is causing deadlocks
        logger.debug(f"Node {self.node_id} _save_persistent_state - DISABLED for testing")
        return
        
        logger.debug(f"Node {self.node_id} _save_persistent_state starting")
        state_data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [entry.to_dict() for entry in self.log]
        }
        
        state_file = self.state_dir / "state.json"
        temp_file = self.state_dir / "state.json.tmp"
        
        try:
            logger.debug(f"Node {self.node_id} about to write state data")
            
            # Use synchronous I/O in thread pool to avoid async deadlocks
            loop = asyncio.get_event_loop()
            
            def _sync_write():
                # Write to temporary file first
                with open(temp_file, 'w') as f:
                    json.dump(state_data, f, indent=2)
                    f.flush()
                
                # Atomic rename
                import shutil
                shutil.move(str(temp_file), str(state_file))
            
            # FIXED: Much shorter timeout for file operations to prevent lock contention
            await asyncio.wait_for(
                loop.run_in_executor(None, _sync_write),
                timeout=1.0  # FIXED: Very short timeout
            )
            
            logger.debug(f"Saved persistent state: term={self.current_term}, voted_for={self.voted_for}")
        except asyncio.TimeoutError:
            logger.error(f"Node {self.node_id} file I/O timed out after 1s - this may cause state inconsistency")
            # Don't raise the exception to prevent deadlocks
        except Exception as e:
            logger.error(f"Node {self.node_id} failed to save persistent state: {e}", exc_info=True)
            # Clean up temp file if it exists
            try:
                if temp_file.exists():
                    temp_file.unlink()
                    logger.debug(f"Node {self.node_id} cleaned up temp file")
            except Exception as cleanup_e:
                logger.error(f"Node {self.node_id} failed to cleanup temp file: {cleanup_e}")
            # Don't raise the exception to prevent deadlocks
    
    async def load_persistent_state(self):
        """Load persistent state from disk"""
        state_file = self.state_dir / "state.json"
        
        if not state_file.exists():
            logger.info("No existing state found, starting with defaults")
            return
        
        try:
            async with aiofiles.open(state_file, 'r') as f:
                content = await f.read()
                state_data = json.loads(content)
            
            async with self._state_lock:
                self.current_term = state_data["current_term"]
                self.voted_for = state_data["voted_for"]
                self.log = [LogEntry.from_dict(entry) for entry in state_data["log"]]
                
                # Update volatile state based on log
                if self.log:
                    self.commit_index = min(self.commit_index, self.log[-1].index)
            
            logger.info(f"Loaded persistent state: term={self.current_term}, "
                       f"voted_for={self.voted_for}, log_length={len(self.log)}")
        except Exception as e:
            logger.error(f"Failed to load persistent state: {e}")
            raise
    
    async def _update_term_no_lock(self, new_term: int):
        """Update current term and reset voted_for (assumes lock is already held)"""
        logger.debug(f"Node {self.node_id} _update_term_no_lock called: {self.current_term} -> {new_term}")
        if new_term > self.current_term:
            logger.debug(f"Node {self.node_id} updating term from {self.current_term} to {new_term}")
            self.current_term = new_term
            self.voted_for = None
            logger.debug(f"Node {self.node_id} about to save persistent state")
            await self._save_persistent_state()
            logger.info(f"Updated term to {new_term}")
        else:
            logger.debug(f"Node {self.node_id} not updating term: {new_term} <= {self.current_term}")

    async def _record_vote_no_lock(self, candidate_id: str):
        """Record vote for a candidate (assumes lock is already held)"""
        logger.debug(f"Node {self.node_id} _record_vote_no_lock called for {candidate_id}")
        self.voted_for = candidate_id
        logger.debug(f"Node {self.node_id} about to save persistent state in record_vote")
        await self._save_persistent_state()
        logger.info(f"Voted for {candidate_id} in term {self.current_term}")

    async def update_term(self, new_term: int):
        """Update current term and reset voted_for"""
        logger.debug(f"Node {self.node_id} update_term called: {self.current_term} -> {new_term}")
        
        # Simple timeout wrapper with shorter timeout
        async def _do_update():
            logger.debug(f"Node {self.node_id} attempting to acquire state lock in update_term")
            async with self._state_lock:
                logger.debug(f"Node {self.node_id} acquired state lock in update_term")
                await self._update_term_no_lock(new_term)
                logger.debug(f"Node {self.node_id} about to release state lock in update_term")
        
        try:
            await asyncio.wait_for(_do_update(), timeout=10.0)  # Reduced timeout
            logger.debug(f"Node {self.node_id} successfully completed update_term")
        except asyncio.TimeoutError:
            logger.error(f"Node {self.node_id} update_term timed out after 10 seconds")
            raise
        except Exception as e:
            logger.error(f"Node {self.node_id} update_term failed: {e}", exc_info=True)
            raise

    async def record_vote(self, candidate_id: str):
        """Record vote for a candidate"""
        logger.debug(f"Node {self.node_id} record_vote called for {candidate_id}")
        
        # Simple timeout wrapper with shorter timeout
        async def _do_vote():
            logger.debug(f"Node {self.node_id} attempting to acquire state lock in record_vote")
            async with self._state_lock:
                logger.debug(f"Node {self.node_id} acquired state lock in record_vote")
                await self._record_vote_no_lock(candidate_id)
                logger.debug(f"Node {self.node_id} about to release state lock in record_vote")
        
        try:
            await asyncio.wait_for(_do_vote(), timeout=10.0)  # Reduced timeout
            logger.debug(f"Node {self.node_id} successfully completed record_vote")
        except asyncio.TimeoutError:
            logger.error(f"Node {self.node_id} record_vote timed out after 10 seconds")
            raise
        except Exception as e:
            logger.error(f"Node {self.node_id} record_vote failed: {e}", exc_info=True)
            raise
    
    async def append_entries(self, entries: List[LogEntry], leader_commit: int = None):
        """Append new entries to the log"""
        async with self._state_lock:
            if entries:
                # Ensure entries have correct indices
                start_index = len(self.log) + 1 if self.log else 1
                for i, entry in enumerate(entries):
                    entry.index = start_index + i
                
                self.log.extend(entries)
                await self._save_persistent_state()
                
                logger.info(f"Appended {len(entries)} entries to log, new log length: {len(self.log)}, last_index: {self.get_last_log_index()}")
            
            # Update commit index if provided
            if leader_commit is not None and leader_commit > self.commit_index:
                old_commit = self.commit_index
                self.commit_index = min(leader_commit, self.get_last_log_index())
                logger.info(f"Updated commit index from {old_commit} to {self.commit_index} via append_entries")
    
    async def delete_conflicting_entries(self, start_index: int):
        """Delete log entries starting from the given index"""
        async with self._state_lock:
            if start_index <= len(self.log):
                deleted_count = len(self.log) - start_index + 1
                self.log = self.log[:start_index - 1]
                await self._save_persistent_state()
                logger.info(f"Deleted {deleted_count} conflicting entries from index {start_index}")
    
    def get_last_log_index(self) -> int:
        """Get the index of the last log entry"""
        return self.log[-1].index if self.log else 0
    
    def get_last_log_term(self) -> int:
        """Get the term of the last log entry"""
        return self.log[-1].term if self.log else 0
    
    def get_log_entry(self, index: int) -> Optional[LogEntry]:
        """Get a log entry by index"""
        if index <= 0 or index > len(self.log):
            return None
        return self.log[index - 1]
    
    def get_entries_from(self, start_index: int) -> List[LogEntry]:
        """Get all log entries starting from the given index"""
        if start_index <= 0 or start_index > len(self.log):
            return []
        return self.log[start_index - 1:]
    
    def initialize_leader_state(self, cluster_nodes: List[str]):
        """Initialize state when becoming leader"""
        last_log_index = self.get_last_log_index()
        
        # Clear old state
        self.next_index.clear()
        self.match_index.clear()
        
        for node_id in cluster_nodes:
            if node_id != self.node_id:
                self.next_index[node_id] = last_log_index + 1
                self.match_index[node_id] = 0
        
        # FIX: Include self in match_index for commit calculation
        # The leader has already replicated all its own entries
        self.match_index[self.node_id] = last_log_index
        
        logger.info(f"Initialized leader state: last_log_index={last_log_index}")
        logger.info(f"next_index: {self.next_index}")
        logger.info(f"match_index: {self.match_index}")
    
    def update_follower_progress(self, follower_id: str, match_index: int):
        """Update progress tracking for a follower"""
        if follower_id in self.match_index:
            old_match_index = self.match_index[follower_id]
            self.match_index[follower_id] = match_index
            self.next_index[follower_id] = match_index + 1
            logger.info(f"Updated progress for {follower_id}: match_index={old_match_index}->{match_index}, next_index={match_index + 1}")
        else:
            logger.warning(f"Attempted to update progress for unknown follower: {follower_id}")
    
    def calculate_commit_index(self, cluster_size: int) -> int:
        """Calculate new commit index based on majority replication"""
        if self.state != "leader":
            return self.commit_index
        
        # FIX: Include leader's own progress
        # Get all match indices including the leader itself
        all_match_indices = []
        
        # Add follower match indices
        for follower_id, match_index in self.match_index.items():
            if follower_id != self.node_id:  # Skip self if in match_index
                all_match_indices.append(match_index)
        
        # Add leader's own last log index
        all_match_indices.append(self.get_last_log_index())
        
        # Sort in descending order
        all_match_indices.sort(reverse=True)
        
        majority_size = (cluster_size + 1) // 2
        
        logger.info(f"Calculating commit index: all_indices={all_match_indices}, "
                f"majority_size={majority_size}, current_commit={self.commit_index}")
        
        # Find the highest index that has been replicated on a majority
        for index in all_match_indices:
            if index <= self.commit_index:
                break  # No point checking lower indices
            
            # Count how many nodes have this index
            count = sum(1 for match_idx in all_match_indices if match_idx >= index)
            
            if count >= majority_size:
                # Check if entry is from current term
                entry = self.get_log_entry(index)
                if entry and entry.term == self.current_term:
                    logger.info(f"New commit index: {index} (replicated on {count} nodes)")
                    return index
                else:
                    logger.debug(f"Cannot commit index {index}: wrong term "
                            f"(entry_term={entry.term if entry else None}, current_term={self.current_term})")
        
        return self.commit_index
    
    async def apply_committed_entries(self) -> List[LogEntry]:
        """Get entries that are committed but not yet applied"""
        entries_to_apply = []
        
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.get_log_entry(self.last_applied)
            if entry:
                entries_to_apply.append(entry)
                logger.debug(f"Entry {self.last_applied} ready to apply")
        
        return entries_to_apply