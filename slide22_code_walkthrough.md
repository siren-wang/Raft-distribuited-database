# Slide 22: Code Walkthrough
## 💻 Implementation Highlights

### What We Built Successfully
- **8,000+ lines of clean, modular code**
- **Async/await patterns throughout**
- **Comprehensive monitoring and logging**
- **Working leader election (100% success)**
- **Perfect state consistency**

### Key Code Patterns

#### 1. Clean Async RPC Implementation
```python
async def request_vote(self, target_node: str, request: RequestVoteRequest):
    """Clean, readable async RPC implementation"""
    try:
        response = await self.clients[target_node].post(
            "/raft/request_vote",
            json=asdict(request)
        )
        if response.status_code == 200:
            return RequestVoteResponse(**response.json())
    except httpx.TimeoutException:
        logger.debug(f"Vote request timeout to {target_node}")
    except Exception as e:
        logger.error(f"Vote request failed: {e}")
    return None
```

#### 2. The Problem: Single Lock Bottleneck
```python
# PROBLEMATIC PATTERN - causes 100% write failures
async def append_entries(self, entries):
    async with self._state_lock:  # ← This lock blocks everything
        # All operations wait here:
        # - Log writes (slow disk I/O)
        # - Term updates
        # - Vote recording
        # - Leader elections
        await self.write_to_disk(entries)  # 8+ seconds!
```

#### 3. The Solution: Granular Locking
```python
# BETTER PATTERN - what we should implement
async def append_entries(self, entries):
    async with self.log_lock:      # Only lock log operations
        await self.write_to_disk(entries)
    
    async with self.term_lock:     # Separate lock for term updates
        self.update_commit_index()
    
    # Vote operations use their own lock
    # Elections don't block writes
```

### Architecture Visualization

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   HTTP API      │────│   KV Store       │────│   Raft Node     │
│ (FastAPI/Async) │    │ (Business Logic) │    │ (Consensus)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                       ┌──────────────────┐    ┌─────────────────┐
                       │   RPC Client     │────│  State Machine  │
                       │ (Network Layer)  │    │ (Core Algorithm)│
                       └──────────────────┘    └─────────────────┘
                                                         │
                                               ┌─────────────────┐
                                               │   Persistence   │
                                               │ (PostgreSQL)    │
                                               └─────────────────┘
```

### Performance Metrics
- ✅ **Leader Election**: 100% success, ~500ms
- ✅ **API Response**: 6.45ms average
- ✅ **State Consistency**: 100% across all nodes
- ❌ **Write Operations**: 0% success (lock contention)
- ❌ **Commit Progress**: Stuck at index 0

### Code Quality Achievements
- **Modular Design**: Each component independently testable
- **Error Handling**: Comprehensive try/catch patterns
- **Logging**: Detailed debugging information
- **Type Safety**: Full type hints throughout
- **Async Patterns**: Modern Python async/await usage

### The Learning: Code Quality vs. Architectural Decisions
**High code quality doesn't guarantee system success.** Our code is clean, well-structured, and follows best practices. But one architectural decision (single vs. granular locking) determined whether the system worked at all.

**Key Insight**: In distributed systems, small design choices have massive consequences. 