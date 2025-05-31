"""
Raft Consensus Demo
Demonstrates the distributed key-value store with Raft consensus
"""

import asyncio
import httpx
import json
import time
import random
from typing import Dict, Any, Optional

# Cluster configuration
NODES = {
    "node1": "http://localhost:8001",
    "node2": "http://localhost:8002",
    "node3": "http://localhost:8003"
}


class RaftClient:
    """Client for interacting with Raft cluster"""
    
    def __init__(self, nodes: Dict[str, str]):
        self.nodes = nodes
        self.current_leader = None
        self.client = httpx.AsyncClient(timeout=5.0)
    
    async def close(self):
        await self.client.aclose()
    
    async def find_leader(self) -> Optional[str]:
        """Find the current leader node"""
        for node_id, url in self.nodes.items():
            try:
                response = await self.client.get(f"{url}/raft/status")
                if response.status_code == 200:
                    status = response.json()
                    if status["state"] == "leader":
                        self.current_leader = node_id
                        return node_id
            except Exception:
                pass
        return None
    
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get status of all nodes in the cluster"""
        status_map = {}
        for node_id, url in self.nodes.items():
            try:
                response = await self.client.get(f"{url}/raft/status")
                if response.status_code == 200:
                    status_map[node_id] = response.json()
                else:
                    status_map[node_id] = {"error": f"HTTP {response.status_code}"}
            except Exception as e:
                status_map[node_id] = {"error": str(e)}
        return status_map
    
    async def put(self, key: str, value: Any) -> Dict[str, Any]:
        """Put a value into the distributed store"""
        if not self.current_leader:
            await self.find_leader()
        
        if not self.current_leader:
            raise Exception("No leader found")
        
        url = f"{self.nodes[self.current_leader]}/kv/{key}"
        response = await self.client.put(url, json=value)
        
        if response.status_code == 421:  # Misdirected request
            # Leader changed, retry
            self.current_leader = None
            return await self.put(key, value)
        
        response.raise_for_status()
        return response.json()
    
    async def get(self, key: str) -> Dict[str, Any]:
        """Get a value from the distributed store"""
        if not self.current_leader:
            await self.find_leader()
        
        if not self.current_leader:
            raise Exception("No leader found")
        
        url = f"{self.nodes[self.current_leader]}/kv/{key}"
        response = await self.client.get(url)
        
        if response.status_code == 421:  # Misdirected request
            # Leader changed, retry
            self.current_leader = None
            return await self.get(key)
        
        response.raise_for_status()
        return response.json()
    
    async def delete(self, key: str) -> Optional[Dict[str, Any]]:
        """Delete a key from the distributed store"""
        if not self.current_leader:
            await self.find_leader()
        
        if not self.current_leader:
            raise Exception("No leader found")
        
        url = f"{self.nodes[self.current_leader]}/kv/{key}"
        response = await self.client.delete(url)
        
        if response.status_code == 421:  # Misdirected request
            # Leader changed, retry
            self.current_leader = None
            return await self.delete(key)
        
        response.raise_for_status()
        return response.json() if response.status_code == 200 else None


async def demo_basic_operations():
    """Demonstrate basic Raft operations"""
    print("=== Raft Consensus Demo ===\n")
    
    client = RaftClient(NODES)
    
    try:
        # Wait for cluster to elect a leader
        print("1. Waiting for leader election...")
        for i in range(20):
            leader = await client.find_leader()
            if leader:
                print(f"   Leader elected: {leader}")
                break
            await asyncio.sleep(1)
        else:
            print("   No leader elected after 20 seconds")
            return
        
        # Show cluster status
        print("\n2. Cluster Status:")
        status = await client.get_cluster_status()
        for node_id, node_status in status.items():
            if "error" not in node_status:
                print(f"   {node_id}: state={node_status['state']}, "
                      f"term={node_status['current_term']}, "
                      f"log_length={node_status['log_length']}")
            else:
                print(f"   {node_id}: {node_status['error']}")
        
        # Write some data
        print("\n3. Writing data to the cluster:")
        data = {
            "user:alice": {"name": "Alice", "age": 30, "city": "New York"},
            "user:bob": {"name": "Bob", "age": 25, "city": "San Francisco"},
            "user:charlie": {"name": "Charlie", "age": 35, "city": "Seattle"}
        }
        
        for key, value in data.items():
            result = await client.put(key, value)
            print(f"   PUT {key} -> version {result['version']}")
        
        # Read data back
        print("\n4. Reading data from the cluster:")
        for key in data.keys():
            result = await client.get(key)
            print(f"   GET {key} -> {result['value']}")
        
        # Show replication status
        print("\n5. Checking replication:")
        await asyncio.sleep(1)  # Wait for replication
        status = await client.get_cluster_status()
        commit_indices = {}
        for node_id, node_status in status.items():
            if "error" not in node_status:
                commit_indices[node_id] = node_status['commit_index']
        
        if len(set(commit_indices.values())) == 1:
            print(f"   ✓ All nodes have same commit index: {list(commit_indices.values())[0]}")
        else:
            print(f"   ✗ Commit indices differ: {commit_indices}")
        
    finally:
        await client.close()


async def demo_fault_tolerance():
    """Demonstrate fault tolerance"""
    print("\n=== Fault Tolerance Demo ===\n")
    
    client = RaftClient(NODES)
    
    try:
        # Find leader
        leader = await client.find_leader()
        print(f"1. Current leader: {leader}")
        
        # Write some data
        print("\n2. Writing test data...")
        await client.put("test:fault", {"value": "initial"})
        print("   Data written successfully")
        
        # Simulate leader failure
        print(f"\n3. Simulating leader failure (stop {leader})...")
        print("   Stop the leader node manually or using docker-compose stop")
        print("   Waiting for new leader election...")
        
        # Wait for new leader
        await asyncio.sleep(3)
        old_leader = leader
        new_leader = None
        
        for i in range(10):
            client.current_leader = None  # Force re-discovery
            new_leader = await client.find_leader()
            if new_leader and new_leader != old_leader:
                print(f"   New leader elected: {new_leader}")
                break
            await asyncio.sleep(1)
        
        if new_leader:
            # Try to read/write with new leader
            print("\n4. Testing operations with new leader...")
            
            # Read previous data
            result = await client.get("test:fault")
            print(f"   Read previous data: {result['value']}")
            
            # Write new data
            await client.put("test:fault", {"value": "after_failure"})
            print("   Written new data successfully")
            
            # Verify
            result = await client.get("test:fault")
            print(f"   Verified new data: {result['value']}")
            
            print("\n   ✓ Cluster continues operating despite leader failure!")
        else:
            print("   ✗ Could not elect new leader")
            
    finally:
        await client.close()


async def demo_concurrent_writes():
    """Demonstrate concurrent write handling"""
    print("\n=== Concurrent Writes Demo ===\n")
    
    # Create multiple clients
    clients = [RaftClient(NODES) for _ in range(5)]
    
    try:
        print("1. Performing concurrent writes from 5 clients...")
        
        # Define write tasks
        async def write_task(client_id: int, client: RaftClient):
            results = []
            for i in range(10):
                key = f"concurrent:client{client_id}:item{i}"
                value = {"client": client_id, "item": i, "timestamp": time.time()}
                try:
                    result = await client.put(key, value)
                    results.append((key, "success"))
                except Exception as e:
                    results.append((key, f"error: {e}"))
                
                # Small random delay
                await asyncio.sleep(random.uniform(0.01, 0.05))
            
            return results
        
        # Run concurrent writes
        tasks = [write_task(i, client) for i, client in enumerate(clients)]
        all_results = await asyncio.gather(*tasks)
        
        # Count successes
        total_writes = sum(len(results) for results in all_results)
        successful_writes = sum(1 for results in all_results 
                              for _, status in results if status == "success")
        
        print(f"   Total write attempts: {total_writes}")
        print(f"   Successful writes: {successful_writes}")
        print(f"   Success rate: {successful_writes/total_writes*100:.1f}%")
        
        # Verify data consistency
        print("\n2. Verifying data consistency...")
        
        # Pick a random client to read
        reader = clients[0]
        sample_keys = [f"concurrent:client0:item{i}" for i in range(5)]
        
        for key in sample_keys:
            try:
                result = await reader.get(key)
                print(f"   {key}: ✓ (version {result['version']})")
            except Exception as e:
                print(f"   {key}: ✗ ({e})")
        
    finally:
        for client in clients:
            await client.close()


async def main():
    """Run all demos"""
    
    # Wait for cluster to start
    print("Waiting for cluster to start...")
    await asyncio.sleep(5)
    
    # Run demos
    await demo_basic_operations()
    
    print("\n" + "="*50 + "\n")
    
    await demo_concurrent_writes()
    
    print("\n" + "="*50 + "\n")
    
    print("To test fault tolerance:")
    print("1. Run this demo")
    print("2. In another terminal, stop one of the nodes:")
    print("   docker-compose -f docker-compose-raft.yml stop raft-node1")
    print("3. Run: python examples/raft_demo.py --fault-tolerance")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--fault-tolerance":
        asyncio.run(demo_fault_tolerance())
    else:
        asyncio.run(main())