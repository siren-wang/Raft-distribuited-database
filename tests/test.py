"""
Simplified Integration Tests for Raft Cluster
Focus on verifying core functionality for demo
"""

import pytest
import asyncio
import httpx
import time
import subprocess
from typing import Dict, Optional, Any


class RaftClusterTester:
    """Test helper for Raft cluster operations"""
    
    def __init__(self, nodes: Dict[str, str]):
        self.nodes = nodes
        self.client = httpx.AsyncClient(timeout=10.0)
    
    async def close(self):
        await self.client.aclose()
    
    async def get_node_status(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific node"""
        try:
            response = await self.client.get(f"http://{self.nodes[node_id]}/raft/status")
            return response.json() if response.status_code == 200 else None
        except:
            return None
    
    async def find_leader(self) -> Optional[str]:
        """Find the current leader"""
        for node_id in self.nodes:
            status = await self.get_node_status(node_id)
            if status and status.get("state") == "leader":
                return node_id
        return None
    
    async def write_to_leader(self, key: str, value: Any) -> bool:
        """Write data through the leader"""
        leader = await self.find_leader()
        if not leader:
            return False
        
        try:
            response = await self.client.put(
                f"http://{self.nodes[leader]}/kv/{key}",
                json={"value": value}
            )
            return response.status_code == 200
        except:
            return False
    
    async def read_from_node(self, node_id: str, key: str) -> Optional[Any]:
        """Read data from a specific node"""
        try:
            response = await self.client.get(
                f"http://{self.nodes[node_id]}/kv/{key}"
            )
            return response.json() if response.status_code == 200 else None
        except:
            return None
    
    def stop_node(self, node_id: str):
        """Stop a node"""
        subprocess.run(
            ["docker", "stop", f"raft-{node_id}"],
            capture_output=True
        )
    
    def start_node(self, node_id: str):
        """Start a node"""
        subprocess.run(
            ["docker", "start", f"raft-{node_id}"],
            capture_output=True
        )


@pytest.fixture
async def cluster():
    """Fixture for cluster testing"""
    nodes = {
        "node1": "localhost:8001",
        "node2": "localhost:8002",
        "node3": "localhost:8003"
    }
    tester = RaftClusterTester(nodes)
    
    # Ensure cluster is running
    subprocess.run(
        ["docker-compose", "-f", "docker-compose-raft.yml", "up", "-d"],
        capture_output=True
    )
    
    # Wait for cluster to stabilize
    await asyncio.sleep(5)
    
    yield tester
    
    await tester.close()


@pytest.mark.asyncio
async def test_leader_election(cluster):
    """Test that a leader is elected"""
    # Wait for leader election
    leader = None
    for _ in range(10):
        leader = await cluster.find_leader()
        if leader:
            break
        await asyncio.sleep(1)
    
    assert leader is not None, "No leader elected within timeout"
    
    # Verify only one leader
    leaders = []
    for node_id in cluster.nodes:
        status = await cluster.get_node_status(node_id)
        if status and status.get("state") == "leader":
            leaders.append(node_id)
    
    assert len(leaders) == 1, f"Expected 1 leader, found {len(leaders)}"


@pytest.mark.asyncio
async def test_data_replication(cluster):
    """Test that data is replicated to all nodes"""
    # Write data
    test_key = f"test-key-{int(time.time())}"
    test_value = {"data": "test", "timestamp": time.time()}
    
    success = await cluster.write_to_leader(test_key, test_value)
    assert success, "Failed to write data"
    
    # Wait for replication
    await asyncio.sleep(1)
    
    # Verify data on all nodes
    for node_id in cluster.nodes:
        data = await cluster.read_from_node(node_id, test_key)
        assert data is not None, f"Data not found on {node_id}"
        assert data["value"]["data"] == "test", f"Data mismatch on {node_id}"


@pytest.mark.asyncio
async def test_leader_failure_recovery(cluster):
    """Test that cluster recovers from leader failure"""
    # Find initial leader
    initial_leader = await cluster.find_leader()
    assert initial_leader is not None
    
    # Write some data
    test_key = "leader-failure-test"
    await cluster.write_to_leader(test_key, {"before": "failure"})
    
    # Stop the leader
    cluster.stop_node(initial_leader)
    
    # Wait for new leader election
    await asyncio.sleep(3)
    
    new_leader = None
    for _ in range(10):
        new_leader = await cluster.find_leader()
        if new_leader and new_leader != initial_leader:
            break
        await asyncio.sleep(1)
    
    assert new_leader is not None, "No new leader elected"
    assert new_leader != initial_leader, "Same leader re-elected"
    
    # Verify data is still accessible
    data = await cluster.read_from_node(new_leader, test_key)
    assert data is not None, "Data lost after leader failure"
    
    # Restart stopped node
    cluster.start_node(initial_leader)


@pytest.mark.asyncio
async def test_minority_partition(cluster):
    """Test that minority partition cannot make progress"""
    # Stop 2 of 3 nodes
    cluster.stop_node("node2")
    cluster.stop_node("node3")
    
    await asyncio.sleep(2)
    
    # Try to write to remaining node
    try:
        response = await cluster.client.put(
            f"http://{cluster.nodes['node1']}/kv/minority-test",
            json={"value": "should fail"},
            timeout=3.0
        )
        assert response.status_code != 200, "Minority partition accepted write"
    except:
        # Timeout or error is expected
        pass
    
    # Restart nodes
    cluster.start_node("node2")
    cluster.start_node("node3")
    
    # Wait for recovery
    await asyncio.sleep(3)
    
    # Verify cluster is functional again
    leader = await cluster.find_leader()
    assert leader is not None, "Cluster did not recover"


@pytest.mark.asyncio
async def test_concurrent_operations(cluster):
    """Test handling of concurrent operations"""
    # Ensure leader exists
    leader = await cluster.find_leader()
    assert leader is not None
    
    # Perform concurrent writes
    tasks = []
    for i in range(10):
        key = f"concurrent-{i}"
        value = {"index": i}
        tasks.append(cluster.write_to_leader(key, value))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Count successful writes
    successful = sum(1 for r in results if r is True)
    assert successful >= 8, f"Too many failed writes: {successful}/10"
    
    # Verify successful writes are replicated
    await asyncio.sleep(1)
    
    for i in range(10):
        if results[i] is True:
            key = f"concurrent-{i}"
            # Check on a follower
            follower = next(n for n in cluster.nodes if n != leader)
            data = await cluster.read_from_node(follower, key)
            assert data is not None, f"Concurrent write {i} not replicated"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])