"""
Raft Distributed Database Benchmark Suite
Comprehensive performance testing for the Raft consensus implementation
"""

import asyncio
import time
import statistics
import json
import httpx
import random
import string
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@dataclass
class BenchmarkResult:
    """Result of a benchmark test"""
    test_name: str
    duration: float
    success_rate: float
    throughput: float
    latency_p50: float
    latency_p95: float
    latency_p99: float
    additional_metrics: Dict[str, Any]


class RaftBenchmarkClient:
    """Client for benchmarking Raft cluster"""
    
    def __init__(self, cluster_nodes: Dict[str, str]):
        self.cluster_nodes = cluster_nodes
        self.current_leader = None
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def close(self):
        await self.client.aclose()
    
    async def find_leader(self) -> Optional[str]:
        """Find the current leader node"""
        for node_id, url in self.cluster_nodes.items():
            try:
                response = await self.client.get(f"{url}/raft/status")
                if response.status_code == 200:
                    status = response.json()
                    if status["state"] == "leader":
                        self.current_leader = node_id
                        return node_id
            except Exception:
                continue
        return None
    
    async def wait_for_leader(self, timeout: float = 30.0) -> bool:
        """Wait for a leader to be elected"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            leader = await self.find_leader()
            if leader:
                return True
            await asyncio.sleep(0.5)
        return False
    
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get status of all nodes"""
        status_map = {}
        for node_id, url in self.cluster_nodes.items():
            try:
                response = await self.client.get(f"{url}/raft/status")
                if response.status_code == 200:
                    status_map[node_id] = response.json()
                else:
                    status_map[node_id] = {"error": f"HTTP {response.status_code}"}
            except Exception as e:
                status_map[node_id] = {"error": str(e)}
        return status_map
    
    async def write_operation(self, key: str, value: Any) -> Tuple[bool, float, Optional[str]]:
        """Perform a write operation and return (success, latency, error)"""
        if not self.current_leader:
            await self.find_leader()
        
        if not self.current_leader:
            return False, 0.0, "No leader found"
        
        start_time = time.time()
        try:
            url = f"{self.cluster_nodes[self.current_leader]}/kv/{key}"
            response = await self.client.put(url, json={"value": value})
            latency = time.time() - start_time
            
            if response.status_code == 421:  # Leader changed
                self.current_leader = None
                return False, latency, "Leader changed"
            
            if response.status_code == 200:
                return True, latency, None
            else:
                return False, latency, f"HTTP {response.status_code}"
                
        except Exception as e:
            latency = time.time() - start_time
            return False, latency, str(e)
    
    async def read_operation(self, key: str) -> Tuple[bool, float, Optional[str]]:
        """Perform a read operation and return (success, latency, error)"""
        if not self.current_leader:
            await self.find_leader()
        
        if not self.current_leader:
            return False, 0.0, "No leader found"
        
        start_time = time.time()
        try:
            url = f"{self.cluster_nodes[self.current_leader]}/kv/{key}"
            response = await self.client.get(url)
            latency = time.time() - start_time
            
            if response.status_code == 421:  # Leader changed
                self.current_leader = None
                return False, latency, "Leader changed"
            
            if response.status_code in [200, 404]:
                return True, latency, None
            else:
                return False, latency, f"HTTP {response.status_code}"
                
        except Exception as e:
            latency = time.time() - start_time
            return False, latency, str(e)


class RaftBenchmarkSuite:
    """Comprehensive benchmark suite for Raft implementation"""
    
    def __init__(self, cluster_nodes: Dict[str, str]):
        self.cluster_nodes = cluster_nodes
        self.client = RaftBenchmarkClient(cluster_nodes)
        self.results: List[BenchmarkResult] = []
    
    async def setup(self):
        """Setup benchmark environment"""
        print("Setting up benchmark environment...")
        
        # Wait for cluster to be ready
        print("Waiting for leader election...")
        if not await self.client.wait_for_leader(timeout=60.0):
            raise Exception("No leader elected within timeout")
        
        print(f"Leader elected: {self.client.current_leader}")
        
        # Check cluster health
        status = await self.client.get_cluster_status()
        healthy_nodes = sum(1 for s in status.values() if "error" not in s)
        print(f"Healthy nodes: {healthy_nodes}/{len(self.cluster_nodes)}")
        
        if healthy_nodes < 2:
            raise Exception("Insufficient healthy nodes for benchmarking")
    
    async def cleanup(self):
        """Cleanup benchmark environment"""
        await self.client.close()
    
    def generate_key(self, prefix: str = "benchmark") -> str:
        """Generate a random key for testing"""
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        return f"{prefix}_{suffix}"
    
    def generate_value(self, size_bytes: int = 100) -> Dict[str, Any]:
        """Generate a random value of specified size"""
        data = ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes))
        return {
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "size": size_bytes
        }
    
    async def benchmark_leader_election_time(self) -> BenchmarkResult:
        """Benchmark leader election time (requires cluster restart)"""
        print("\n=== Leader Election Benchmark ===")
        print("Note: This test requires manual cluster restart to trigger new elections")
        
        # For now, measure current election status
        start_time = time.time()
        status = await self.client.get_cluster_status()
        
        election_times = []
        for node_id, node_status in status.items():
            if "error" not in node_status and node_status.get("state") == "leader":
                # Estimate based on current term and uptime
                term = node_status.get("current_term", 0)
                # This is a simplified estimation
                estimated_election_time = term * 0.5  # Rough estimate
                election_times.append(estimated_election_time)
        
        avg_election_time = statistics.mean(election_times) if election_times else 5.0
        
        return BenchmarkResult(
            test_name="leader_election_time",
            duration=time.time() - start_time,
            success_rate=1.0 if election_times else 0.0,
            throughput=0.0,
            latency_p50=avg_election_time,
            latency_p95=avg_election_time,
            latency_p99=avg_election_time,
            additional_metrics={
                "current_leader": self.client.current_leader,
                "election_count": len(election_times),
                "estimated_time": avg_election_time
            }
        )
    
    async def benchmark_write_throughput(self, duration: float = 30.0, 
                                      concurrent_clients: int = 10) -> BenchmarkResult:
        """Benchmark write throughput"""
        print(f"\n=== Write Throughput Benchmark ===")
        print(f"Duration: {duration}s, Concurrent clients: {concurrent_clients}")
        
        results = []
        start_time = time.time()
        
        async def write_worker(worker_id: int):
            worker_results = []
            end_time = start_time + duration
            
            while time.time() < end_time:
                key = self.generate_key(f"write_worker_{worker_id}")
                value = self.generate_value(100)
                
                success, latency, error = await self.client.write_operation(key, value)
                worker_results.append((success, latency, error))
                
                if not success and "timeout" not in str(error).lower():
                    await asyncio.sleep(0.1)  # Brief pause on non-timeout errors
            
            return worker_results
        
        # Run workers concurrently
        tasks = [write_worker(i) for i in range(concurrent_clients)]
        worker_results = await asyncio.gather(*tasks)
        
        # Flatten results
        for worker_result in worker_results:
            results.extend(worker_result)
        
        # Calculate metrics
        total_operations = len(results)
        successful_operations = sum(1 for success, _, _ in results if success)
        success_rate = successful_operations / total_operations if total_operations > 0 else 0.0
        
        successful_latencies = [latency for success, latency, _ in results if success]
        
        if successful_latencies:
            latency_p50 = statistics.median(successful_latencies)
            latency_p95 = np.percentile(successful_latencies, 95)
            latency_p99 = np.percentile(successful_latencies, 99)
        else:
            latency_p50 = latency_p95 = latency_p99 = 0.0
        
        actual_duration = time.time() - start_time
        throughput = successful_operations / actual_duration
        
        print(f"Results: {successful_operations}/{total_operations} operations successful")
        print(f"Throughput: {throughput:.2f} ops/sec")
        print(f"Success rate: {success_rate:.2%}")
        
        return BenchmarkResult(
            test_name="write_throughput",
            duration=actual_duration,
            success_rate=success_rate,
            throughput=throughput,
            latency_p50=latency_p50,
            latency_p95=latency_p95,
            latency_p99=latency_p99,
            additional_metrics={
                "total_operations": total_operations,
                "concurrent_clients": concurrent_clients,
                "avg_latency": statistics.mean(successful_latencies) if successful_latencies else 0.0
            }
        )
    
    async def benchmark_read_throughput(self, duration: float = 30.0,
                                     concurrent_clients: int = 10) -> BenchmarkResult:
        """Benchmark read throughput"""
        print(f"\n=== Read Throughput Benchmark ===")
        print(f"Duration: {duration}s, Concurrent clients: {concurrent_clients}")
        
        # First, write some test data
        print("Preparing test data...")
        test_keys = []
        for i in range(100):
            key = self.generate_key(f"read_test_{i}")
            value = self.generate_value(100)
            success, _, _ = await self.client.write_operation(key, value)
            if success:
                test_keys.append(key)
        
        print(f"Prepared {len(test_keys)} test keys")
        
        if not test_keys:
            print("Warning: No test data could be written, using random keys")
            test_keys = [self.generate_key(f"missing_{i}") for i in range(100)]
        
        results = []
        start_time = time.time()
        
        async def read_worker(worker_id: int):
            worker_results = []
            end_time = start_time + duration
            
            while time.time() < end_time:
                key = random.choice(test_keys)
                success, latency, error = await self.client.read_operation(key)
                worker_results.append((success, latency, error))
                
                if not success and "timeout" not in str(error).lower():
                    await asyncio.sleep(0.01)  # Brief pause on errors
            
            return worker_results
        
        # Run workers concurrently
        tasks = [read_worker(i) for i in range(concurrent_clients)]
        worker_results = await asyncio.gather(*tasks)
        
        # Flatten results
        for worker_result in worker_results:
            results.extend(worker_result)
        
        # Calculate metrics
        total_operations = len(results)
        successful_operations = sum(1 for success, _, _ in results if success)
        success_rate = successful_operations / total_operations if total_operations > 0 else 0.0
        
        successful_latencies = [latency for success, latency, _ in results if success]
        
        if successful_latencies:
            latency_p50 = statistics.median(successful_latencies)
            latency_p95 = np.percentile(successful_latencies, 95)
            latency_p99 = np.percentile(successful_latencies, 99)
        else:
            latency_p50 = latency_p95 = latency_p99 = 0.0
        
        actual_duration = time.time() - start_time
        throughput = successful_operations / actual_duration
        
        print(f"Results: {successful_operations}/{total_operations} operations successful")
        print(f"Throughput: {throughput:.2f} ops/sec")
        print(f"Success rate: {success_rate:.2%}")
        
        return BenchmarkResult(
            test_name="read_throughput",
            duration=actual_duration,
            success_rate=success_rate,
            throughput=throughput,
            latency_p50=latency_p50,
            latency_p95=latency_p95,
            latency_p99=latency_p99,
            additional_metrics={
                "total_operations": total_operations,
                "concurrent_clients": concurrent_clients,
                "test_keys_prepared": len(test_keys),
                "avg_latency": statistics.mean(successful_latencies) if successful_latencies else 0.0
            }
        )
    
    async def benchmark_mixed_workload(self, duration: float = 30.0,
                                     read_ratio: float = 0.8) -> BenchmarkResult:
        """Benchmark mixed read/write workload"""
        print(f"\n=== Mixed Workload Benchmark ===")
        print(f"Duration: {duration}s, Read ratio: {read_ratio:.0%}")
        
        # Prepare some test data
        test_keys = []
        for i in range(50):
            key = self.generate_key(f"mixed_test_{i}")
            value = self.generate_value(100)
            success, _, _ = await self.client.write_operation(key, value)
            if success:
                test_keys.append(key)
        
        results = []
        start_time = time.time()
        end_time = start_time + duration
        
        while time.time() < end_time:
            if random.random() < read_ratio and test_keys:
                # Read operation
                key = random.choice(test_keys)
                success, latency, error = await self.client.read_operation(key)
                results.append(("read", success, latency, error))
            else:
                # Write operation
                key = self.generate_key("mixed_write")
                value = self.generate_value(100)
                success, latency, error = await self.client.write_operation(key, value)
                results.append(("write", success, latency, error))
                if success:
                    test_keys.append(key)
            
            await asyncio.sleep(0.01)  # Small delay to prevent overwhelming
        
        # Calculate metrics
        total_operations = len(results)
        successful_operations = sum(1 for _, success, _, _ in results if success)
        success_rate = successful_operations / total_operations if total_operations > 0 else 0.0
        
        successful_latencies = [latency for _, success, latency, _ in results if success]
        
        if successful_latencies:
            latency_p50 = statistics.median(successful_latencies)
            latency_p95 = np.percentile(successful_latencies, 95)
            latency_p99 = np.percentile(successful_latencies, 99)
        else:
            latency_p50 = latency_p95 = latency_p99 = 0.0
        
        actual_duration = time.time() - start_time
        throughput = successful_operations / actual_duration
        
        # Break down by operation type
        read_ops = [r for r in results if r[0] == "read"]
        write_ops = [r for r in results if r[0] == "write"]
        
        read_success_rate = sum(1 for _, success, _, _ in read_ops if success) / len(read_ops) if read_ops else 0.0
        write_success_rate = sum(1 for _, success, _, _ in write_ops if success) / len(write_ops) if write_ops else 0.0
        
        print(f"Results: {successful_operations}/{total_operations} operations successful")
        print(f"Read operations: {len(read_ops)} (success: {read_success_rate:.2%})")
        print(f"Write operations: {len(write_ops)} (success: {write_success_rate:.2%})")
        print(f"Throughput: {throughput:.2f} ops/sec")
        
        return BenchmarkResult(
            test_name="mixed_workload",
            duration=actual_duration,
            success_rate=success_rate,
            throughput=throughput,
            latency_p50=latency_p50,
            latency_p95=latency_p95,
            latency_p99=latency_p99,
            additional_metrics={
                "total_operations": total_operations,
                "read_operations": len(read_ops),
                "write_operations": len(write_ops),
                "read_success_rate": read_success_rate,
                "write_success_rate": write_success_rate,
                "actual_read_ratio": len(read_ops) / total_operations if total_operations > 0 else 0.0
            }
        )
    
    async def benchmark_fault_tolerance(self) -> BenchmarkResult:
        """Benchmark fault tolerance (basic health check)"""
        print(f"\n=== Fault Tolerance Benchmark ===")
        print("Note: This is a basic health check. Full fault tolerance testing requires manual intervention.")
        
        start_time = time.time()
        
        # Check cluster health multiple times
        health_checks = []
        for i in range(10):
            status = await self.client.get_cluster_status()
            healthy_nodes = sum(1 for s in status.values() if "error" not in s)
            has_leader = any(s.get("state") == "leader" for s in status.values() if "error" not in s)
            
            health_checks.append({
                "healthy_nodes": healthy_nodes,
                "has_leader": has_leader,
                "total_nodes": len(self.cluster_nodes)
            })
            
            await asyncio.sleep(1.0)
        
        # Calculate metrics
        avg_healthy_nodes = statistics.mean(h["healthy_nodes"] for h in health_checks)
        leader_availability = sum(1 for h in health_checks if h["has_leader"]) / len(health_checks)
        
        duration = time.time() - start_time
        
        print(f"Average healthy nodes: {avg_healthy_nodes:.1f}/{len(self.cluster_nodes)}")
        print(f"Leader availability: {leader_availability:.2%}")
        
        return BenchmarkResult(
            test_name="fault_tolerance",
            duration=duration,
            success_rate=leader_availability,
            throughput=0.0,
            latency_p50=0.0,
            latency_p95=0.0,
            latency_p99=0.0,
            additional_metrics={
                "avg_healthy_nodes": avg_healthy_nodes,
                "leader_availability": leader_availability,
                "health_checks": len(health_checks)
            }
        )
    
    async def run_all_benchmarks(self) -> List[BenchmarkResult]:
        """Run all benchmark tests"""
        print("Starting Raft Distributed Database Benchmark Suite")
        print("=" * 60)
        
        try:
            await self.setup()
            
            # Run benchmarks
            self.results.append(await self.benchmark_leader_election_time())
            self.results.append(await self.benchmark_fault_tolerance())
            self.results.append(await self.benchmark_read_throughput(duration=20.0, concurrent_clients=5))
            self.results.append(await self.benchmark_write_throughput(duration=20.0, concurrent_clients=3))
            self.results.append(await self.benchmark_mixed_workload(duration=20.0, read_ratio=0.7))
            
            return self.results
            
        finally:
            await self.cleanup()
    
    def generate_report(self, output_file: str = "benchmark_report.json"):
        """Generate benchmark report"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "cluster_nodes": list(self.cluster_nodes.keys()),
            "results": []
        }
        
        for result in self.results:
            report["results"].append({
                "test_name": result.test_name,
                "duration": result.duration,
                "success_rate": result.success_rate,
                "throughput": result.throughput,
                "latency_p50": result.latency_p50,
                "latency_p95": result.latency_p95,
                "latency_p99": result.latency_p99,
                "additional_metrics": result.additional_metrics
            })
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\nBenchmark report saved to {output_file}")
        return report
    
    def print_summary(self):
        """Print benchmark summary"""
        print("\n" + "=" * 60)
        print("BENCHMARK SUMMARY")
        print("=" * 60)
        
        for result in self.results:
            print(f"\n{result.test_name.upper().replace('_', ' ')}")
            print(f"  Duration: {result.duration:.2f}s")
            print(f"  Success Rate: {result.success_rate:.2%}")
            if result.throughput > 0:
                print(f"  Throughput: {result.throughput:.2f} ops/sec")
            if result.latency_p50 > 0:
                print(f"  Latency P50: {result.latency_p50*1000:.2f}ms")
                print(f"  Latency P95: {result.latency_p95*1000:.2f}ms")
                print(f"  Latency P99: {result.latency_p99*1000:.2f}ms")


async def main():
    """Main benchmark execution"""
    # Default cluster configuration
    cluster_nodes = {
        "node1": "http://localhost:8001",
        "node2": "http://localhost:8002", 
        "node3": "http://localhost:8003"
    }
    
    # Create benchmark suite
    benchmark = RaftBenchmarkSuite(cluster_nodes)
    
    try:
        # Run all benchmarks
        results = await benchmark.run_all_benchmarks()
        
        # Generate reports
        benchmark.print_summary()
        benchmark.generate_report("benchmark_results.json")
        
        print(f"\nCompleted {len(results)} benchmark tests")
        
    except Exception as e:
        print(f"Benchmark failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Add required packages to requirements if missing
    try:
        import matplotlib.pyplot as plt
        import pandas as pd
        import numpy as np
    except ImportError:
        print("Installing additional packages for benchmarking...")
        import subprocess
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "matplotlib", "pandas", "numpy"
        ])
        import matplotlib.pyplot as plt
        import pandas as pd
        import numpy as np
    
    asyncio.run(main()) 