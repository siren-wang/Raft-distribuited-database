"""
Raft Consensus Module
Provides distributed consensus for the key-value store
"""

from .node import RaftNode, RaftNodeState
from .state import RaftState, LogEntry
from .rpc import (
    RaftRPCClient,
    RaftRPCHandler,
    RequestVoteRequest,
    RequestVoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse
)
from .log import WriteAheadLog, WALEntry
from .server import RaftKVStore, RaftCommand, create_raft_api

__all__ = [
    # Node
    "RaftNode",
    "RaftNodeState",
    
    # State
    "RaftState",
    "LogEntry",
    
    # RPC
    "RaftRPCClient",
    "RaftRPCHandler",
    "RequestVoteRequest",
    "RequestVoteResponse",
    "AppendEntriesRequest",
    "AppendEntriesResponse",
    
    # WAL
    "WriteAheadLog",
    "WALEntry",
    
    # Server
    "RaftKVStore",
    "RaftCommand",
    "create_raft_api"
]

__version__ = "1.0.0"