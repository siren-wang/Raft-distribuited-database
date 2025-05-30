syntax = "proto3";

package raft;

// Raft RPC service definition
service RaftService {
    // RequestVote RPC
    rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
    
    // AppendEntries RPC
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
    
    // InstallSnapshot RPC for log compaction
    rpc InstallSnapshot(InstallSnapshotRequest) returns (InstallSnapshotResponse);
    
    // Health check
    rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}

// RequestVote messages
message RequestVoteRequest {
    uint64 term = 1;
    string candidate_id = 2;
    uint64 last_log_index = 3;
    uint64 last_log_term = 4;
}

message RequestVoteResponse {
    uint64 term = 1;
    bool vote_granted = 2;
}

// AppendEntries messages
message AppendEntriesRequest {
    uint64 term = 1;
    string leader_id = 2;
    uint64 prev_log_index = 3;
    uint64 prev_log_term = 4;
    repeated LogEntry entries = 5;
    uint64 leader_commit = 6;
}

message AppendEntriesResponse {
    uint64 term = 1;
    bool success = 2;
    uint64 match_index = 3;
    
    // Additional fields for optimization
    uint64 conflict_index = 4;  // First index of conflicting entry
    uint64 conflict_term = 5;   // Term of conflicting entry
}

// Log entry
message LogEntry {
    uint64 term = 1;
    uint64 index = 2;
    bytes command = 3;  // Serialized command
    int64 timestamp = 4;  // Unix timestamp in microseconds
}

// InstallSnapshot messages for log compaction
message InstallSnapshotRequest {
    uint64 term = 1;
    string leader_id = 2;
    uint64 last_included_index = 3;
    uint64 last_included_term = 4;
    uint64 offset = 5;
    bytes data = 6;
    bool done = 7;
}

message InstallSnapshotResponse {
    uint64 term = 1;
}

// Health check messages
message HealthCheckRequest {
    string node_id = 1;
}

message HealthCheckResponse {
    bool healthy = 1;
    string status = 2;
    uint64 current_term = 3;
    string current_leader = 4;
}

// Batch message for improved throughput
message BatchedRequest {
    repeated RequestVoteRequest vote_requests = 1;
    repeated AppendEntriesRequest append_requests = 2;
}

message BatchedResponse {
    repeated RequestVoteResponse vote_responses = 1;
    repeated AppendEntriesResponse append_responses = 2;
}