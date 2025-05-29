"""
Tests for Raft state management
"""

import pytest
import asyncio
import tempfile
import shutil
from datetime import datetime
from pathlib import Path

from src.raft.state import RaftState, LogEntry


@pytest.fixture
async def temp_state_dir():
    """Create temporary directory for state storage"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
async def raft_state(temp_state_dir):
    """Create RaftState instance"""
    state = RaftState("test_node", temp_state_dir)
    yield state


class TestRaftState:
    """Test RaftState functionality"""
    
    async def test_initial_state(self, raft_state):
        """Test initial state values"""
        assert raft_state.node_id == "test_node"
        assert raft_state.current_term == 0
        assert raft_state.voted_for is None
        assert len(raft_state.log) == 0
        assert raft_state.commit_index == 0
        assert raft_state.last_applied == 0
        assert raft_state.state == "follower"
    
    async def test_update_term(self, raft_state):
        """Test term updates"""
        # Update to higher term
        await raft_state.update_term(5)
        assert raft_state.current_term == 5
        assert raft_state.voted_for is None
        
        # Lower term should not update
        await raft_state.update_term(3)
        assert raft_state.current_term == 5
        
        # Same term should not reset voted_for
        await raft_state.record_vote("node2")
        await raft_state.update_term(5)
        assert raft_state.voted_for == "node2"
    
    async def test_record_vote(self, raft_state):
        """Test vote recording"""
        await raft_state.record_vote("candidate1")
        assert raft_state.voted_for == "candidate1"
        
        # Can change vote in same term (though Raft wouldn't do this)
        await raft_state.record_vote("candidate2")
        assert raft_state.voted_for == "candidate2"
    
    async def test_append_entries(self, raft_state):
        """Test log entry appending"""
        # Create test entries
        entries = [
            LogEntry(term=1, index=0, command={"op": "set", "key": "a", "value": 1}),
            LogEntry(term=1, index=0, command={"op": "set", "key": "b", "value": 2}),
            LogEntry(term=2, index=0, command={"op": "set", "key": "c", "value": 3})
        ]
        
        # Append entries
        await raft_state.append_entries(entries)
        
        # Check log
        assert len(raft_state.log) == 3
        assert raft_state.log[0].index == 1
        assert raft_state.log[1].index == 2
        assert raft_state.log[2].index == 3
        
        # Check last log index/term
        assert raft_state.get_last_log_index() == 3
        assert raft_state.get_last_log_term() == 2
    
    async def test_delete_conflicting_entries(self, raft_state):
        """Test deletion of conflicting entries"""
        # Add initial entries
        entries = [
            LogEntry(term=1, index=1, command={"op": "set", "key": "a", "value": 1}),
            LogEntry(term=1, index=2, command={"op": "set", "key": "b", "value": 2}),
            LogEntry(term=2, index=3, command={"op": "set", "key": "c", "value": 3}),
            LogEntry(term=2, index=4, command={"op": "set", "key": "d", "value": 4})
        ]
        raft_state.log = entries
        
        # Delete from index 3
        await raft_state.delete_conflicting_entries(3)
        
        # Check remaining entries
        assert len(raft_state.log) == 2
        assert raft_state.get_last_log_index() == 2
        assert raft_state.log[-1].command["key"] == "b"
    
    async def test_get_log_entry(self, raft_state):
        """Test retrieving log entries by index"""
        # Add test entries
        entries = [
            LogEntry(term=1, index=1, command={"op": "set", "key": "a", "value": 1}),
            LogEntry(term=2, index=2, command={"op": "set", "key": "b", "value": 2})
        ]
        raft_state.log = entries
        
        # Test valid indices
        entry1 = raft_state.get_log_entry(1)
        assert entry1 is not None
        assert entry1.term == 1
        assert entry1.command["key"] == "a"
        
        entry2 = raft_state.get_log_entry(2)
        assert entry2 is not None
        assert entry2.term == 2
        assert entry2.command["key"] == "b"
        
        # Test invalid indices
        assert raft_state.get_log_entry(0) is None
        assert raft_state.get_log_entry(3) is None
        assert raft_state.get_log_entry(-1) is None
    
    async def test_get_entries_from(self, raft_state):
        """Test retrieving entries from index"""
        # Add test entries
        entries = [
            LogEntry(term=1, index=1, command={"op": "set", "key": "a", "value": 1}),
            LogEntry(term=1, index=2, command={"op": "set", "key": "b", "value": 2}),
            LogEntry(term=2, index=3, command={"op": "set", "key": "c", "value": 3})
        ]
        raft_state.log = entries
        
        # Get from index 2
        entries_from_2 = raft_state.get_entries_from(2)
        assert len(entries_from_2) == 2
        assert entries_from_2[0].index == 2
        assert entries_from_2[1].index == 3
        
        # Get from index 1 (all entries)
        entries_from_1 = raft_state.get_entries_from(1)
        assert len(entries_from_1) == 3
        
        # Get from invalid index
        assert len(raft_state.get_entries_from(4)) == 0
        assert len(raft_state.get_entries_from(0)) == 0
    
    async def test_leader_state_initialization(self, raft_state):
        """Test leader state initialization"""
        cluster_nodes = ["node1", "node2", "node3"]
        raft_state.node_id = "node1"
        
        # Add some log entries
        entries = [
            LogEntry(term=1, index=1, command={"op": "set", "key": "a", "value": 1}),
            LogEntry(term=2, index=2, command={"op": "set", "key": "b", "value": 2})
        ]
        raft_state.log = entries
        
        # Initialize leader state
        raft_state.initialize_leader_state(cluster_nodes)
        
        # Check next_index (should be last_log_index + 1 for all followers)
        assert raft_state.next_index["node2"] == 3
        assert raft_state.next_index["node3"] == 3
        assert "node1" not in raft_state.next_index
        
        # Check match_index (should be 0 for all followers)
        assert raft_state.match_index["node2"] == 0
        assert raft_state.match_index["node3"] == 0
        assert "node1" not in raft_state.match_index
    
    async def test_update_follower_progress(self, raft_state):
        """Test updating follower progress"""
        # Initialize leader state
        cluster_nodes = ["node1", "node2", "node3"]
        raft_state.node_id = "node1"
        raft_state.initialize_leader_state(cluster_nodes)
        
        # Update follower progress
        raft_state.update_follower_progress("node2", 5)
        
        # Check updates
        assert raft_state.match_index["node2"] == 5
        assert raft_state.next_index["node2"] == 6
        
        # Update non-existent follower (should not crash)
        raft_state.update_follower_progress("node4", 10)
    
    async def test_calculate_commit_index(self, raft_state):
        """Test commit index calculation"""
        # Setup as leader
        raft_state.state = "leader"
        raft_state.current_term = 2
        raft_state.node_id = "node1"
        
        # Add log entries
        entries = [
            LogEntry(term=1, index=1, command={"op": "set", "key": "a", "value": 1}),
            LogEntry(term=2, index=2, command={"op": "set", "key": "b", "value": 2}),
            LogEntry(term=2, index=3, command={"op": "set", "key": "c", "value": 3}),
            LogEntry(term=2, index=4, command={"op": "set", "key": "d", "value": 4})
        ]
        raft_state.log = entries
        
        # Setup match indices (3-node cluster)
        raft_state.match_index = {"node2": 3, "node3": 2}
        
        # Calculate commit index (majority is 2 nodes)
        # node1: 4, node2: 3, node3: 2
        # Majority has at least index 3
        new_commit = raft_state.calculate_commit_index(3)
        assert new_commit == 3
        
        # Update match indices
        raft_state.match_index = {"node2": 4, "node3": 4}
        
        # Now majority has index 4
        new_commit = raft_state.calculate_commit_index(3)
        assert new_commit == 4
    
    async def test_apply_committed_entries(self, raft_state):
        """Test getting entries to apply"""
        # Add log entries
        entries = [
            LogEntry(term=1, index=1, command={"op": "set", "key": "a", "value": 1}),
            LogEntry(term=1, index=2, command={"op": "set", "key": "b", "value": 2}),
            LogEntry(term=2, index=3, command={"op": "set", "key": "c", "value": 3})
        ]
        raft_state.log = entries
        
        # Set commit index
        raft_state.commit_index = 2
        raft_state.last_applied = 0
        
        # Get entries to apply
        to_apply = await raft_state.apply_committed_entries()
        
        # Should get entries 1 and 2
        assert len(to_apply) == 2
        assert to_apply[0].index == 1
        assert to_apply[1].index == 2
        assert raft_state.last_applied == 2
        
        # Call again - should get nothing
        to_apply = await raft_state.apply_committed_entries()
        assert len(to_apply) == 0
        assert raft_state.last_applied == 2
    
    async def test_persistence(self, raft_state, temp_state_dir):
        """Test state persistence and recovery"""
        # Set some state
        await raft_state.update_term(5)
        await raft_state.record_vote("node2")
        
        # Add log entries
        entries = [
            LogEntry(term=1, index=0, command={"op": "set", "key": "a", "value": 1}),
            LogEntry(term=2, index=0, command={"op": "set", "key": "b", "value": 2})
        ]
        await raft_state.append_entries(entries)
        
        # Create new state instance and load
        new_state = RaftState("test_node", temp_state_dir)
        await new_state.load_persistent_state()
        
        # Verify state was recovered
        assert new_state.current_term == 5
        assert new_state.voted_for == "node2"
        assert len(new_state.log) == 2
        assert new_state.log[0].command["key"] == "a"
        assert new_state.log[1].command["key"] == "b"
    
    async def test_concurrent_modifications(self, raft_state):
        """Test concurrent state modifications"""
        # Run multiple concurrent operations
        tasks = []
        
        # Multiple term updates
        for i in range(10):
            tasks.append(raft_state.update_term(i))
        
        # Multiple log appends
        for i in range(5):
            entry = LogEntry(term=1, index=0, command={"op": "set", "key": f"k{i}", "value": i})
            tasks.append(raft_state.append_entries([entry]))
        
        # Wait for all tasks
        await asyncio.gather(*tasks)
        
        # Verify final state
        assert raft_state.current_term == 9  # Highest term
        assert len(raft_state.log) == 5  # All appends succeeded


@pytest.mark.asyncio
async def test_log_entry_serialization():
    """Test LogEntry serialization/deserialization"""
    # Create entry
    original = LogEntry(
        term=5,
        index=10,
        command={"op": "set", "key": "test", "value": {"nested": True}},
        timestamp=datetime.utcnow()
    )
    
    # Serialize to dict
    entry_dict = original.to_dict()
    assert entry_dict["term"] == 5
    assert entry_dict["index"] == 10
    assert entry_dict["command"]["key"] == "test"
    
    # Deserialize from dict
    restored = LogEntry.from_dict(entry_dict)
    assert restored.term == original.term
    assert restored.index == original.index
    assert restored.command == original.command
    assert abs((restored.timestamp - original.timestamp).total_seconds()) < 1