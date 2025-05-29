"""
Tests for Raft node implementation
"""

import pytest
import asyncio
import tempfile
import shutil
from unittest.mock import Mock, AsyncMock, patch

from src.raft.node import RaftNode, RaftNodeState
from src.raft.state import LogEntry
from src.raft.rpc import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse


@pytest.fixture
async def temp_dir():
    """Create temporary directory"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
async def cluster_config():
    """Test cluster configuration"""
    return {
        "node1": "localhost:8001",
        "node2": "localhost:8002",
        "node3": "localhost:8003"
    }


@pytest.fixture
async def raft_node(cluster_config, temp_dir):
    """Create RaftNode instance"""
    node = RaftNode(
        node_id="node1",
        cluster_config=cluster_config,
        state_dir=temp_dir,
        election_timeout_range=(0.05, 0.1),  # Short timeouts for testing
        heartbeat_interval=0.02
    )
    
    # Mock RPC client
    node.rpc_client = AsyncMock()
    
    yield node
    
    # Cleanup
    node.running = False
    if node.election_timer_task:
        node.election_timer_task.cancel()
    if node.heartbeat_task:
        node.heartbeat_task.cancel()


class TestRaftNode:
    """Test RaftNode functionality"""
    
    async def test_initial_state(self, raft_node):
        """Test initial node state"""
        assert raft_node.node_id == "node1"
        assert raft_node.state.state == "follower"
        assert raft_node.current_leader is None
        assert not raft_node.running
    
    async def test_start_stop(self, raft_node):
        """Test node start/stop"""
        # Mock state loading
        raft_node.state.load_persistent_state = AsyncMock()
        
        # Start node
        await raft_node.start()
        assert raft_node.running
        assert raft_node.state.state == RaftNodeState.FOLLOWER.value
        
        # Stop node
        await raft_node.stop()
        assert not raft_node.running
    
    async def test_follower_to_candidate_transition(self, raft_node):
        """Test transition from follower to candidate"""
        raft_node.running = True
        raft_node.state.state = RaftNodeState.FOLLOWER.value
        
        # Transition to candidate
        raft_node.transition_to_candidate()
        
        assert raft_node.state.state == RaftNodeState.CANDIDATE.value
        assert raft_node.current_leader is None
    
    async def test_candidate_to_leader_transition(self, raft_node):
        """Test transition from candidate to leader"""
        raft_node.running = True
        raft_node.state.state = RaftNodeState.CANDIDATE.value
        
        # Transition to leader
        raft_node.transition_to_leader()
        
        assert raft_node.state.state == RaftNodeState.LEADER.value
        assert raft_node.current_leader == "node1"
        assert raft_node.heartbeat_task is not None
    
    async def test_leader_to_follower_transition(self, raft_node):
        """Test transition from leader to follower"""
        raft_node.running = True
        raft_node.state.state = RaftNodeState.LEADER.value
        raft_node.current_leader = "node1"
        
        # Create mock heartbeat task
        raft_node.heartbeat_task = AsyncMock()
        
        # Transition to follower
        raft_node.transition_to_follower()
        
        assert raft_node.state.state == RaftNodeState.FOLLOWER.value
        assert raft_node.current_leader is None
        assert raft_node.heartbeat_task.cancel.called
    
    async def test_election_timeout(self, raft_node):
        """Test election timeout triggers candidate transition"""
        raft_node.running = True
        raft_node.state.state = RaftNodeState.FOLLOWER.value
        
        # Mock transition
        raft_node.transition_to_candidate = Mock()
        
        # Trigger election timeout
        await raft_node._election_timeout(0.01)
        
        # Should transition to candidate
        raft_node.transition_to_candidate.assert_called_once()
    
    @patch('src.raft.node.asyncio.create_task')
    async def test_run_election_win(self, mock_create_task, raft_node):
        """Test winning an election"""
        raft_node.running = True
        raft_node.state.state = RaftNodeState.CANDIDATE.value
        raft_node.state.current_term = 1
        
        # Mock state operations
        raft_node.state.update_term = AsyncMock()
        raft_node.state.record_vote = AsyncMock()
        
        # Mock RPC responses - both nodes grant votes
        raft_node.rpc_client.request_vote = AsyncMock(
            side_effect=[
                RequestVoteResponse(term=1, vote_granted=True),
                RequestVoteResponse(term=1, vote_granted=True)
            ]
        )
        
        # Mock transition
        raft_node.transition_to_leader = Mock()
        
        # Run election
        await raft_node._run_election()
        
        # Should win with 3 votes (self + 2 others)
        raft_node.transition_to_leader.assert_called_once()
    
    async def test_run_election_lose(self, raft_node):
        """Test losing an election"""
        raft_node.running = True
        raft_node.state.state = RaftNodeState.CANDIDATE.value
        raft_node.state.current_term = 1
        
        # Mock state operations
        raft_node.state.update_term = AsyncMock()
        raft_node.state.record_vote = AsyncMock()
        
        # Mock RPC responses - only one node grants vote
        raft_node.rpc_client.request_vote = AsyncMock(
            side_effect=[
                RequestVoteResponse(term=1, vote_granted=True),
                RequestVoteResponse(term=1, vote_granted=False)
            ]
        )
        
        # Mock transition
        raft_node.transition_to_leader = Mock()
        raft_node.reset_election_timer = Mock()
        
        # Run election
        await raft_node._run_election()
        
        # Should not win (only 2 votes out of 3 needed)
        raft_node.transition_to_leader.assert_not_called()
        raft_node.reset_election_timer.assert_called_once()
    
    async def test_run_election_discover_higher_term(self, raft_node):
        """Test discovering higher term during election"""
        raft_node.running = True
        raft_node.state.state = RaftNodeState.CANDIDATE.value
        raft_node.state.current_term = 1
        
        # Mock state operations
        raft_node.state.update_term = AsyncMock()
        raft_node.state.record_vote = AsyncMock()
        
        # Mock RPC response with higher term
        raft_node.rpc_client.request_vote = AsyncMock(
            return_value=RequestVoteResponse(term=5, vote_granted=False)
        )
        
        # Mock transition
        raft_node.transition_to_follower = Mock()
        
        # Run election
        await raft_node._run_election()
        
        # Should update term and become follower
        raft_node.state.update_term.assert_called_with(5)
        raft_node.transition_to_follower.assert_called_once()
    
    async def test_propose_command_as_leader(self, raft_node):
        """Test proposing command as leader"""
        raft_node.state.state = RaftNodeState.LEADER.value
        raft_node.state.current_term = 2
        
        # Mock log operations
        raft_node.state.get_last_log_index = Mock(return_value=5)
        raft_node.state.append_entries = AsyncMock()
        
        # Mock heartbeat trigger
        raft_node._send_heartbeats = AsyncMock()
        
        # Propose command
        command = {"op": "set", "key": "test", "value": "data"}
        result = await raft_node.propose_command(command)
        
        # Should accept command
        assert result is True
        raft_node.state.append_entries.assert_called_once()
        
        # Check log entry
        call_args = raft_node.state.append_entries.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0].term == 2
        assert call_args[0].index == 6
        assert call_args[0].command == command
    
    async def test_propose_command_as_follower(self, raft_node):
        """Test proposing command as follower"""
        raft_node.state.state = RaftNodeState.FOLLOWER.value
        raft_node.current_leader = "node2"
        
        # Propose command
        command = {"op": "set", "key": "test", "value": "data"}
        result = await raft_node.propose_command(command)
        
        # Should reject command
        assert result is False
    
    async def test_append_noop_entry(self, raft_node):
        """Test appending no-op entry when becoming leader"""
        raft_node.state.state = RaftNodeState.LEADER.value
        raft_node.state.current_term = 3
        raft_node.node_id = "node1"
        
        # Mock log operations
        raft_node.state.get_last_log_index = Mock(return_value=10)
        raft_node.state.append_entries = AsyncMock()
        
        # Append no-op
        await raft_node._append_noop_entry()
        
        # Check no-op entry
        raft_node.state.append_entries.assert_called_once()
        call_args = raft_node.state.append_entries.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0].term == 3
        assert call_args[0].index == 11
        assert call_args[0].command["type"] == "noop"
        assert call_args[0].command["leader_id"] == "node1"
    
    async def test_apply_command_callback(self, raft_node):
        """Test applying committed commands"""
        # Set up callback
        callback = AsyncMock()
        raft_node.set_apply_command_callback(callback)
        
        # Create committed entries
        raft_node.state.apply_committed_entries = AsyncMock(
            return_value=[
                LogEntry(term=1, index=1, command={"op": "set", "key": "a", "value": 1}),
                LogEntry(term=1, index=2, command={"type": "noop"}),  # Should skip
                LogEntry(term=2, index=3, command={"op": "set", "key": "b", "value": 2})
            ]
        )
        
        # Apply entries
        await raft_node.apply_committed_entries()
        
        # Should call callback for non-noop entries
        assert callback.call_count == 2
        callback.assert_any_call({"op": "set", "key": "a", "value": 1})
        callback.assert_any_call({"op": "set", "key": "b", "value": 2})


class TestRaftRPCHandler:
    """Test RPC handler functionality"""
    
    async def test_handle_request_vote_grant(self, raft_node):
        """Test granting vote request"""
        raft_node.state.current_term = 1
        raft_node.state.voted_for = None
        raft_node.state.get_last_log_index = Mock(return_value=5)
        raft_node.state.get_last_log_term = Mock(return_value=1)
        
        # Mock state operations
        raft_node.state.record_vote = AsyncMock()
        raft_node.reset_election_timer = Mock()
        
        # Create request
        request = RequestVoteRequest(
            term=2,
            candidate_id="node2",
            last_log_index=5,
            last_log_term=1
        )
        
        # Handle request
        response = await raft_node.rpc_handler.handle_request_vote(request)
        
        # Should grant vote
        assert response.vote_granted is True
        assert response.term == 2
        raft_node.state.record_vote.assert_called_with("node2")
        raft_node.reset_election_timer.assert_called_once()
    
    async def test_handle_request_vote_deny_old_term(self, raft_node):
        """Test denying vote for old term"""
        raft_node.state.current_term = 5
        
        # Create request with old term
        request = RequestVoteRequest(
            term=3,
            candidate_id="node2",
            last_log_index=5,
            last_log_term=1
        )
        
        # Handle request
        response = await raft_node.rpc_handler.handle_request_vote(request)
        
        # Should deny vote
        assert response.vote_granted is False
        assert response.term == 5
    
    async def test_handle_append_entries_success(self, raft_node):
        """Test successful append entries"""
        raft_node.state.current_term = 2
        raft_node.state.get_log_entry = Mock(return_value=LogEntry(term=1, index=5, command={}))
        raft_node.state.get_last_log_index = Mock(return_value=5)
        
        # Mock state operations
        raft_node.state.append_entries = AsyncMock()
        raft_node.transition_to_follower = Mock()
        raft_node.reset_election_timer = Mock()
        
        # Create request
        entries = [
            {"term": 2, "index": 6, "command": {"op": "set", "key": "a", "value": 1}}
        ]
        request = AppendEntriesRequest(
            term=2,
            leader_id="node2",
            prev_log_index=5,
            prev_log_term=1,
            entries=entries,
            leader_commit=4
        )
        
        # Handle request
        response = await raft_node.rpc_handler.handle_append_entries(request)
        
        # Should succeed
        assert response.success is True
        assert response.term == 2
        raft_node.reset_election_timer.assert_called_once()
        assert raft_node.current_leader == "node2"
    
    async def test_handle_append_entries_log_inconsistency(self, raft_node):
        """Test append entries with log inconsistency"""
        raft_node.state.current_term = 2
        raft_node.state.get_log_entry = Mock(return_value=LogEntry(term=2, index=5, command={}))
        raft_node.state.get_last_log_index = Mock(return_value=5)
        
        # Create request with mismatched prev_log_term
        request = AppendEntriesRequest(
            term=2,
            leader_id="node2",
            prev_log_index=5,
            prev_log_term=1,  # Mismatch - actual is 2
            entries=[],
            leader_commit=4
        )
        
        # Handle request
        response = await raft_node.rpc_handler.handle_append_entries(request)
        
        # Should fail
        assert response.success is False
        assert response.term == 2
        assert response.match_index == 5