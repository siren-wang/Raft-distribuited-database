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
            state_data = {
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "log": [entry.to_dict() for entry in self.log]
            }
            
            state_file = self.state_dir / "state.json"
            temp_file = self.state_dir / "state.json.tmp"
            
            try:
                # Write to temporary file first
                async with aiofiles.open(temp_file, 'w') as f:
                    await f.write(json.dumps(state_data, indent=2))
                
                # Atomic rename
                os.replace(temp_file, state_file)
                
                logger.debug(f"Saved persistent state: term={self.current_term}, voted_for={self.voted_for}")
            except Exception as e:
                logger.error(f"Failed to save persistent state: {e}")
                raise
    
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
    
    async def update_term(self, new_term: int):
        """Update current term and reset voted_for"""
        async with self._state_lock:
            if new_term > self.current_term:
                self.current_term = new_term
                self.voted_for = None
                await self.save_persistent_state()
                logger.info(f"Updated term to {new_term}")
    
    async def record_vote(self, candidate_id: str):
        """Record vote for a candidate"""
        async with self._state_lock:
            self.voted_for = candidate_id
            await self.save_persistent_state()
            logger.info(f"Voted for {candidate_id} in term {self.current_term}")
    
    async def append_entries(self, entries: List[LogEntry], leader_commit: int = None):
        """Append new entries to the log"""
        async with self._state_lock:
            if entries:
                # Ensure entries have correct indices
                start_index = len(self.log) + 1 if self.log else 1
                for i, entry in enumerate(entries):
                    entry.index = start_index + i
                
                self.log.extend(entries)
                await self.save_persistent_state()
                
                logger.info(f"Appended {len(entries)} entries to log")
            
            # Update commit index if provided
            if leader_commit is not None and leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.get_last_log_index())
                logger.debug(f"Updated commit index to {self.commit_index}")
    
    async def delete_conflicting_entries(self, start_index: int):
        """Delete log entries starting from the given index"""
        async with self._state_lock:
            if start_index <= len(self.log):
                deleted_count = len(self.log) - start_index + 1
                self.log = self.log[:start_index - 1]
                await self.save_persistent_state()
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
        for node_id in cluster_nodes:
            if node_id != self.node_id:
                self.next_index[node_id] = last_log_index + 1
                self.match_index[node_id] = 0
        logger.info(f"Initialized leader state for {len(cluster_nodes) - 1} followers")
    
    def update_follower_progress(self, follower_id: str, match_index: int):
        """Update progress tracking for a follower"""
        if follower_id in self.match_index:
            self.match_index[follower_id] = match_index
            self.next_index[follower_id] = match_index + 1
            logger.debug(f"Updated progress for {follower_id}: match_index={match_index}")
    
    def calculate_commit_index(self, cluster_size: int) -> int:
        """Calculate new commit index based on majority replication"""
        if self.state != "leader":
            return self.commit_index
        
        # Find the highest index replicated on majority of servers
        match_indices = list(self.match_index.values()) + [self.get_last_log_index()]
        match_indices.sort(reverse=True)
        
        majority_size = (cluster_size + 1) // 2
        
        for i in range(len(match_indices)):
            if i + 1 >= majority_size:
                candidate_index = match_indices[i]
                # Only commit entries from current term
                if candidate_index > self.commit_index:
                    entry = self.get_log_entry(candidate_index)
                    if entry and entry.term == self.current_term:
                        return candidate_index
        
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