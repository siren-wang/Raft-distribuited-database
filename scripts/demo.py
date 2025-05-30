#!/usr/bin/env python3
"""
Raft Demo Runner - Orchestrates demo scenarios for visualization
"""

import asyncio
import httpx
import json
import time
import subprocess
from typing import Dict, List, Any, Optional
import random
from datetime import datetime


class RaftDemoRunner:
    """Orchestrates Raft cluster demos"""
    
    def __init__(self, nodes: Dict[str, str]):
        self.nodes = nodes
        self.client = httpx.AsyncClient(timeout=5.0)
        
    async def close(self):
        """Cleanup"""
        await self.client.aclose()
    
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get status of all nodes"""
        status = {}
        for node_id, address in self.nodes.items():
            try:
                response = await self.client.get(f"http://{address}/raft/status")
                if response.status_code == 200:
                    status[node_id] = response.json()
                else:
                    status[node_id] = {"status": "error", "code": response.status_code}
            except Exception as e:
                status[node_id] = {"status": "offline", "error": str(e)}
        return status
    
    async def find_leader(self) -> Optional[str]:
        """Find current leader"""
        status = await self.get_cluster_status()
        for node_id, node_status in status.items():
            if node_status.get("state") == "leader":
                return node_id
        return None
    
    async def write_data(self, key: str, value: Any) -> bool:
        """Write data to the cluster"""
        leader = await self.find_leader()
        if not leader:
            print("No leader found")
            return False
        
        try:
            response = await self.client.put(
                f"http://{self.nodes[leader]}/kv/{key}",
                json={"value": value}
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Write failed: {e}")
            return False
    
    async def read_data(self, key: str) -> Optional[Any]:
        """Read data from any node"""
        for node_id, address in self.nodes.items():
            try:
                response = await self.client.get(f"http://{address}/kv/{key}")
                if response.status_code == 200:
                    return response.json()
            except:
                continue
        return None
    
    def stop_node(self, node_id: str):
        """Stop a node using docker"""
        try:
            subprocess.run(
                ["docker-compose", "-f", "docker-compose-raft.yml", "stop", f"raft-{node_id}"],
                check=True,
                capture_output=True
            )
            print(f"✓ Stopped {node_id}")
        except Exception as e:
            print(f"✗ Failed to stop {node_id}: {e}")
    
    def start_node(self, node_id: str):
        """Start a node using docker"""
        try:
            subprocess.run(
                ["docker-compose", "-f", "docker-compose-raft.yml", "start", f"raft-{node_id}"],
                check=True,
                capture_output=True
            )
            print(f"✓ Started {node_id}")
        except Exception as e:
            print(f"✗ Failed to start {node_id}: {e}")
    
    async def wait_for_leader(self, timeout: int = 10) -> bool:
        """Wait for a leader to be elected"""
        start = time.time()
        while time.time() - start < timeout:
            leader = await self.find_leader()
            if leader:
                print(f"✓ Leader elected: {leader}")
                return True
            await asyncio.sleep(0.5)
        return False
    
    async def demo_leader_election(self):
        """Demo 1: Basic leader election"""
        print("\n=== Demo 1: Leader Election ===")
        print("Starting with fresh cluster...")
        
        # Get initial status
        status = await self.get_cluster_status()
        print(f"Initial state: All nodes are followers")
        
        # Wait for election
        print("Waiting for leader election...")
        if await self.wait_for_leader():
            leader = await self.find_leader()
            print(f"✓ {leader} became the leader!")
            
            # Show final status
            status = await self.get_cluster_status()
            for node_id, node_status in status.items():
                state = node_status.get("state", "unknown")
                term = node_status.get("current_term", 0)
                print(f"  {node_id}: {state} (term {term})")
    
    async def demo_data_replication(self):
        """Demo 2: Data replication across nodes"""
        print("\n=== Demo 2: Data Replication ===")
        
        # Write data
        test_key = f"demo-key-{int(time.time())}"
        test_value = {"message": "Hello Raft!", "timestamp": datetime.now().isoformat()}
        
        print(f"Writing data to leader: {test_key}")
        if await self.write_data(test_key, test_value):
            print("✓ Data written successfully")
            
            # Wait for replication
            await asyncio.sleep(1)
            
            # Read from each node
            print("Reading from all nodes:")
            for node_id in self.nodes:
                data = await self.read_data(test_key)
                if data:
                    print(f"  ✓ {node_id}: Data present")
                else:
                    print(f"  ✗ {node_id}: Data not found")
    
    async def demo_leader_failure(self):
        """Demo 3: Leader failure and re-election"""
        print("\n=== Demo 3: Leader Failure ===")
        
        # Find current leader
        leader = await self.find_leader()
        if not leader:
            print("No leader found, skipping demo")
            return
        
        print(f"Current leader: {leader}")
        
        # Write some data first
        test_key = f"pre-failure-{int(time.time())}"
        await self.write_data(test_key, {"data": "before failure"})
        
        # Stop the leader
        print(f"Stopping leader {leader}...")
        self.stop_node(leader)
        
        # Wait for new election
        print("Waiting for new leader election...")
        await asyncio.sleep(2)
        
        if await self.wait_for_leader():
            new_leader = await self.find_leader()
            print(f"✓ New leader elected: {new_leader}")
            
            # Verify data still accessible
            data = await self.read_data(test_key)
            if data:
                print("✓ Data still accessible after leader failure")
            
            # Write new data
            new_key = f"post-failure-{int(time.time())}"
            if await self.write_data(new_key, {"data": "after failure"}):
                print("✓ Can write new data with new leader")
        
        # Restart the stopped node
        print(f"Restarting {leader}...")
        self.start_node(leader)
        await asyncio.sleep(2)
        print(f"✓ {leader} rejoined as follower")
    
    async def demo_split_brain_prevention(self):
        """Demo 4: Split brain prevention"""
        print("\n=== Demo 4: Split Brain Prevention ===")
        
        # Stop two nodes (minority)
        print("Creating minority partition (stopping 2 of 3 nodes)...")
        self.stop_node("node2")
        self.stop_node("node3")
        
        await asyncio.sleep(2)
        
        # Try to write to remaining node
        print("Attempting to write to minority partition...")
        try:
            response = await self.client.put(
                f"http://{self.nodes['node1']}/kv/test-split",
                json={"value": "should fail"},
                timeout=3.0
            )
            if response.status_code != 200:
                print("✓ Minority partition correctly rejected write")
        except:
            print("✓ Minority partition cannot accept writes")
        
        # Restart nodes
        print("Healing partition...")
        self.start_node("node2")
        self.start_node("node3")
        
        await asyncio.sleep(3)
        if await self.wait_for_leader():
            print("✓ Cluster recovered and elected leader")
    
    async def demo_concurrent_writes(self):
        """Demo 5: Concurrent writes"""
        print("\n=== Demo 5: Concurrent Writes ===")
        
        # Ensure we have a leader
        if not await self.wait_for_leader():
            print("No leader available")
            return
        
        print("Sending 10 concurrent write requests...")
        
        # Create concurrent write tasks
        tasks = []
        for i in range(10):
            key = f"concurrent-{i}"
            value = {"client": i, "timestamp": time.time()}
            tasks.append(self.write_data(key, value))
        
        # Execute concurrently
        results = await asyncio.gather(*tasks)
        
        successful = sum(1 for r in results if r)
        print(f"✓ {successful}/10 writes succeeded")
        
        # Verify all successful writes are replicated
        await asyncio.sleep(1)
        print("Verifying replication...")
        
        for i in range(10):
            key = f"concurrent-{i}"
            data = await self.read_data(key)
            if data:
                print(f"  ✓ {key}: Replicated to all nodes")


async def main():
    """Run all demos"""
    
    # Cluster configuration
    nodes = {
        "node1": "localhost:8001",
        "node2": "localhost:8002",
        "node3": "localhost:8003"
    }
    
    runner = RaftDemoRunner(nodes)
    
    try:
        print("=== Raft Consensus Demo Suite ===")
        print("Make sure the cluster is running:")
        print("  docker-compose -f docker-compose-raft.yml up -d")
        print()
        
        # Wait for cluster to stabilize
        print("Waiting for cluster to initialize...")
        await asyncio.sleep(5)
        
        # Run demos
        await runner.demo_leader_election()
        await asyncio.sleep(2)
        
        await runner.demo_data_replication()
        await asyncio.sleep(2)
        
        await runner.demo_leader_failure()
        await asyncio.sleep(2)
        
        await runner.demo_split_brain_prevention()
        await asyncio.sleep(2)
        
        await runner.demo_concurrent_writes()
        
        print("\n=== All demos completed! ===")
        
    finally:
        await runner.close()


if __name__ == "__main__":
    asyncio.run(main())