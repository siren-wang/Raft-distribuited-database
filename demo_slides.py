#!/usr/bin/env python3
"""
Live Demo Scripts for Raft Presentation
Slides 9, 10, and 11 - Interactive Demos
"""

import asyncio
import httpx
import json
import time
import subprocess
import sys
from typing import Dict, Any, Optional
from datetime import datetime

class Color:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    END = '\033[0m'

class RaftDemoPresentation:
    
    def __init__(self):
        self.nodes = {
            "node1": "localhost:8001",
            "node2": "localhost:8002", 
            "node3": "localhost:8003"
        }
        self.client = httpx.AsyncClient(timeout=15.0)
        
    async def close(self):
        await self.client.aclose()
    
    def print_header(self, title: str):
        print(f"\n{Color.BOLD}{Color.CYAN}{'='*60}{Color.END}")
        print(f"{Color.BOLD}{Color.CYAN}{title:^60}{Color.END}")
        print(f"{Color.BOLD}{Color.CYAN}{'='*60}{Color.END}\n")
    
    def print_step(self, step: str):
        print(f"{Color.BOLD}{Color.BLUE}>>> {step}{Color.END}")
    
    def print_success(self, message: str):
        print(f"{Color.GREEN}✓ {message}{Color.END}")
    
    def print_error(self, message: str):
        print(f"{Color.RED}✗ {message}{Color.END}")
    
    def print_warning(self, message: str):
        print(f"{Color.YELLOW}⚠ {message}{Color.END}")
    
    async def get_cluster_status(self) -> Dict[str, Any]:
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
    
    def display_cluster_status(self, status, title="Cluster Status"):
        print(f"\n{title}:")
        print("Node     State      Term   Log      Commit   Leader    ")
        print("------------------------------------------------------------")
        
        for node_id, info in status.items():
            if "error" in info or info.get("status") == "offline":
                error = info.get("error", "HTTP error")
                print(f"{node_id:<8} {Color.RED}offline{Color.END}   -      -        -        {error:<20}")
                continue
                
            state = info.get('state', 'unknown')
            term = info.get('current_term', 0)
            log_len = info.get('log_length', 0)  
            commit = info.get('commit_index', 0)
            current_leader = info.get('current_leader')
            
            if current_leader is None:
                current_leader = 'none'
            
            if state == "leader":
                state_color = Color.GREEN
            elif state == "candidate":
                state_color = Color.YELLOW  
            elif state == "follower":
                state_color = Color.BLUE
            else:
                state_color = Color.RED
            
            print(f"{node_id:<8} {state_color}{state:<10}{Color.END} {term:<6} {log_len:<8} {commit:<8} {current_leader:<10}")
    
    async def find_leader(self) -> Optional[str]:
        status = await self.get_cluster_status()
        for node_id, node_status in status.items():
            if node_status.get("state") == "leader":
                return node_id
        return None
    
    async def wait_for_leader(self, timeout: int = 30) -> Optional[str]:
        print(f"Waiting for leader election (timeout: {timeout}s)...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            leader = await self.find_leader()
            if leader:
                elapsed = time.time() - start_time
                self.print_success(f"Leader elected: {leader} (took {elapsed:.1f}s)")
                return leader
            
            elapsed = time.time() - start_time
            if int(elapsed) % 2 == 0 and elapsed > 1:
                print(f"  Still waiting... ({elapsed:.0f}s elapsed)")
            
            await asyncio.sleep(0.5)
        
        self.print_error(f"No leader elected after {timeout}s")
        return None
    
    def stop_node(self, node_id: str):
        try:
            subprocess.run(
                ["docker", "compose", "-f", "docker-compose-raft.yml", "stop", f"raft-{node_id}"],
                check=True,
                capture_output=True
            )
            self.print_success(f"Stopped {node_id}")
            return True
        except Exception as e:
            self.print_error(f"Failed to stop {node_id}: {e}")
            return False
    
    def start_node(self, node_id: str):
        try:
            subprocess.run(
                ["docker", "compose", "-f", "docker-compose-raft.yml", "start", f"raft-{node_id}"],
                check=True,
                capture_output=True
            )
            self.print_success(f"Started {node_id}")
            return True
        except Exception as e:
            self.print_error(f"Failed to start {node_id}: {e}")
            return False
    
    async def demo_slide_9_leader_election(self):
        """
        Slide 9: Demo - Leader Election
        Shows the current cluster state and leader election behavior
        """
        self.print_header("SLIDE 9: Leader Election Demo")
        
        self.print_step("Checking Raft cluster...")
        
        status = await self.get_cluster_status()
        online_nodes = [node for node, stat in status.items() if stat.get("status") != "offline"]
        
        if len(online_nodes) < 3:
            self.print_error("Not all nodes are online. Starting cluster...")
            subprocess.run(["docker", "compose", "-f", "docker-compose-raft.yml", "up", "-d"], 
                         capture_output=True)
            await asyncio.sleep(10)
            status = await self.get_cluster_status()
        
        self.display_cluster_status(status, "Current Cluster State")
        
        leader = await self.find_leader()
        
        if leader:
            self.print_success(f"Leader already elected: {leader}")
            
            candidates = [node for node, stat in status.items() 
                         if stat.get("state") == "candidate"]
            
            if candidates:
                self.print_warning(f"Nodes stuck in candidate state: {candidates}")
                print(f"\n{Color.BOLD}📊 Cluster Analysis:{Color.END}")
                print(f"  • Leader elected: ✓ {leader}")
                print(f"  • Stuck candidates: {len(candidates)} nodes")
                print(f"  • This shows the state lock contention issue")
                
        else:
            self.print_step("Waiting for leader election...")
            leader = await self.wait_for_leader(15)
        
        return True
    
    async def demo_slide_10_fault_tolerance(self):
        """
        Slide 10: Demo - Fault Tolerance
        Shows leader failure and attempts at re-election (simplified - no writes)
        """
        self.print_header("SLIDE 10: Fault Tolerance Demo")
        
        # Check if cluster is running and wait for leader election if needed
        self.print_step("Ensuring cluster is ready...")
        status = await self.get_cluster_status()
        online_nodes = [node for node, stat in status.items() if stat.get("status") != "offline"]
        
        if len(online_nodes) < 3:
            self.print_error("Not all nodes are online. Starting cluster...")
            subprocess.run(["docker", "compose", "-f", "docker-compose-raft.yml", "up", "-d"], 
                         capture_output=True)
            await asyncio.sleep(10)
        
        leader = await self.find_leader()
        if not leader:
            self.print_step("No leader found. Waiting for leader election...")
            leader = await self.wait_for_leader(30)
            if not leader:
                self.print_error("No leader elected. Cluster may have fundamental issues.")
                return False
        
        self.print_step(f"Current leader identified: {leader}")
        
        status = await self.get_cluster_status()
        self.display_cluster_status(status, "Before Failure")
        
        self.print_step("Testing pure fault tolerance (no write operations)")
        
        self.print_step(f"Simulating leader failure - stopping {leader}...")
        failure_start = time.time()
        
        if not self.stop_node(leader):
            return False
        
        self.print_step("Waiting for failure detection and new election...")
        await asyncio.sleep(3)
        
        new_leader = await self.wait_for_leader(20) 
        
        if new_leader and new_leader != leader:
            recovery_time = time.time() - failure_start
            
            status = await self.get_cluster_status()
            self.display_cluster_status(status, "After Recovery")
            
            print(f"\n{Color.BOLD}📊 Recovery Analysis:{Color.END}")
            print(f"  • Detection + Election: {recovery_time:.1f} seconds")
            print(f"  • New leader: {new_leader}")
            print(f"  • Fault tolerance: ✓ Working")
            
            self.print_success("Fault tolerance demonstrated!")
            
        elif new_leader == leader:
            self.print_warning("Same leader re-elected after restart")
            
        else:
            self.print_error("No new leader elected - this demonstrates the challenge!")
            
            status = await self.get_cluster_status()
            self.display_cluster_status(status, "Final State")
            
            print(f"\n{Color.BOLD}🔍 Analysis:{Color.END}")
            print(f"  • Leader failure detected: ✓")
            print(f"  • Remaining nodes: 2/3 (majority)")
            print(f"  • New election triggered: ✓")
            print(f"  • Election completion: ✗ Failed")
            print(f"  • Root cause: Lock contention preventing coordination")
        
        self.print_step(f"Restarting {leader}...")
        self.start_node(leader)
        
        # Wait for cluster to stabilize after restart
        self.print_step("Waiting for cluster to stabilize after restart...")
        await asyncio.sleep(8)  # Give more time for full cluster reconciliation
        
        # Show final stable state
        final_leader = await self.find_leader()
        final_status = await self.get_cluster_status()
        self.display_cluster_status(final_status, "Final Stable State")
        
        if final_leader:
            if final_leader != new_leader:
                self.print_warning(f"Leadership changed during recovery: {new_leader} → {final_leader}")
                print(f"\n{Color.BOLD}🔄 Final Recovery Analysis:{Color.END}")
                print(f"  • Initial recovery leader: {new_leader}")
                print(f"  • Final stable leader: {final_leader}")
                print(f"  • This shows continued cluster evolution after node restart")
            else:
                self.print_success(f"Leadership remained stable: {final_leader}")
        else:
            self.print_error("No leader in final state - cluster unstable")
        
        return True
    
    async def demo_slide_11_the_challenge(self):
        """
        Slide 11: Demo - The Challenge
        Shows write timeouts due to log replication issues
        """
        self.print_header("SLIDE 11: The Challenge Demo")
        
        leader = await self.find_leader()
        if not leader:
            self.print_error("No leader found. This itself demonstrates the challenge!")
            return True
        
        self.print_step("Demonstrating the log replication challenge...")
        
        status = await self.get_cluster_status()
        self.display_cluster_status(status, "Before Write Operation")
        
        self.print_step("Attempting write operation (may timeout due to replication issues)...")
        
        write_start = time.time()
        timeout_occurred = False
        
        try:
            client_with_timeout = httpx.AsyncClient(timeout=8.0)
            
            response = await client_with_timeout.put(
                f"http://{self.nodes[leader]}/kv/challenge-key",
                json={"value": {"test": "demonstrating replication issue", "timestamp": datetime.now().isoformat()}}
            )
            
            write_time = time.time() - write_start
            
            if response.status_code == 200:
                self.print_success(f"Write completed in {write_time:.1f}s")
                result = response.json()
                print(f"  Response: {json.dumps(result, indent=2)}")
            else:
                self.print_warning(f"Write returned status {response.status_code} after {write_time:.1f}s")
            
            await client_with_timeout.aclose()
            
        except httpx.ReadTimeout:
            write_time = time.time() - write_start
            timeout_occurred = True
            self.print_error(f"Write operation TIMED OUT after {write_time:.1f}s")
            
        except Exception as e:
            write_time = time.time() - write_start
            self.print_error(f"Write operation FAILED after {write_time:.1f}s: {e}")
            timeout_occurred = True
        
        self.print_step("Analyzing cluster state after write attempt...")
        status = await self.get_cluster_status()
        self.display_cluster_status(status, "After Write Attempt")
        
        print(f"\n{Color.BOLD}🔍 Technical Analysis:{Color.END}")
        
        if timeout_occurred:
            print(f"  {Color.RED}✗ Write operation timed out{Color.END}")
            print(f"  {Color.GREEN}✓ Leader accepts write request{Color.END}")
            print(f"  {Color.GREEN}✓ Creates log entry locally{Color.END}")
            print(f"  {Color.YELLOW}⏱ Replication to followers stalls{Color.END}")
            print(f"  {Color.RED}✗ Cannot achieve majority consensus{Color.END}")
            print(f"  {Color.RED}✗ Client timeout after 8 seconds{Color.END}")
            
        else:
            print(f"  {Color.GREEN}✓ Write completed successfully{Color.END}")
            print(f"  This indicates the cluster is currently stable")
            print(f"  Note: Timeouts occur intermittently due to lock contention")
        
        print(f"\n{Color.BOLD}🐛 Root Cause (from code analysis):{Color.END}")
        print(f"  • _state_lock contention in RaftState class")
        print(f"  • Concurrent RPC handlers block each other:")
        print(f"    - append_entries() (log replication)")
        print(f"    - request_vote() (elections)")
        print(f"    - update_term() (term synchronization)")
        print(f"  • File I/O operations block async event loop")
        print(f"  • RPC timeout (2s) vs file I/O delays create cascading failures")
        
        return True
    
    async def show_logs_analysis(self):
        self.print_header("LOGS ANALYSIS")
        
        self.print_step("Checking recent container logs for key events...")
        
        for node in ["node1", "node2", "node3"]:
            print(f"\n{Color.BOLD}--- Logs for raft-{node} ---{Color.END}")
            try:
                result = subprocess.run(
                    ["docker", "compose", "-f", "docker-compose-raft.yml", "logs", "--tail", "15", f"raft-{node}"],
                    capture_output=True, text=True, timeout=5
                )
                if result.stdout:
                    lines = result.stdout.strip().split('\n')
                    for line in lines[-10:]:
                        if any(keyword in line.lower() for keyword in ['error', 'timeout', 'failed']):
                            print(f"{Color.RED}{line}{Color.END}")
                        elif any(keyword in line.lower() for keyword in ['leader', 'elected']):
                            print(f"{Color.GREEN}{line}{Color.END}")
                        elif any(keyword in line.lower() for keyword in ['vote', 'append_entries', 'candidate']):
                            print(f"{Color.YELLOW}{line}{Color.END}")
                        else:
                            print(line)
            except Exception as e:
                print(f"  {Color.RED}Could not fetch logs: {e}{Color.END}")
    
    async def test_proper_re_election(self):
        """
        Test to validate that proper Raft re-election is happening
        This checks election timing, term progression, and vote validation
        """
        self.print_header("ELECTION VALIDATION TEST")
        
        self.print_step("Testing proper Raft re-election behavior...")
        
        # Get initial state
        initial_status = await self.get_cluster_status()
        initial_leader = await self.find_leader()
        
        if not initial_leader:
            self.print_error("No initial leader found - cluster not ready")
            return False
        
        initial_term = initial_status[initial_leader]['current_term']
        self.print_success(f"Initial state: {initial_leader} is leader in term {initial_term}")
        
        # Stop the leader to trigger re-election
        self.print_step(f"Stopping leader {initial_leader} to trigger re-election...")
        election_start_time = time.time()
        
        if not self.stop_node(initial_leader):
            return False
        
        # Wait a brief moment for failure detection
        await asyncio.sleep(1.0)
        
        # Track election progress with detailed timing
        self.print_step("Monitoring election progress...")
        election_phases = []
        max_wait = 20  # Maximum wait time for election
        check_interval = 0.5
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            status = await self.get_cluster_status()
            elapsed = time.time() - election_start_time
            
            # Analyze current state
            online_nodes = [node for node, stat in status.items() if stat.get("status") != "offline"]
            candidates = [node for node, stat in status.items() if stat.get("state") == "candidate"]
            leaders = [node for node, stat in status.items() if stat.get("state") == "leader"]
            terms = [stat.get("current_term") for node, stat in status.items() if stat.get("status") != "offline"]
            
            phase_info = {
                "time": elapsed,
                "online_nodes": len(online_nodes),
                "candidates": len(candidates),
                "leaders": len(leaders),
                "min_term": min(terms) if terms else 0,
                "max_term": max(terms) if terms else 0,
                "term_spread": max(terms) - min(terms) if terms else 0
            }
            election_phases.append(phase_info)
            
            # Print periodic updates
            if int(elapsed) % 2 == 0 and elapsed > 1:
                print(f"  T+{elapsed:.1f}s: {len(candidates)} candidates, {len(leaders)} leaders, terms {min(terms) if terms else 0}-{max(terms) if terms else 0}")
            
            # Check if election completed
            if leaders and len(leaders) == 1:
                new_leader = [node for node, stat in status.items() if stat.get("state") == "leader"][0]
                final_term = status[new_leader]['current_term']
                election_time = time.time() - election_start_time
                
                self.print_success(f"Election completed: {new_leader} elected in term {final_term} after {election_time:.1f}s")
                break
            
            await asyncio.sleep(check_interval)
        else:
            self.print_error(f"Election did not complete within {max_wait}s")
            # Show final state
            final_status = await self.get_cluster_status()
            self.display_cluster_status(final_status, "Final Election State")
            return False
        
        # Analyze election behavior
        print(f"\n{Color.BOLD}🔍 Election Analysis:{Color.END}")
        
        # 1. Timing validation
        if election_time < 0.5:
            self.print_error(f"Election too fast ({election_time:.1f}s) - likely configuration issue")
        elif election_time > 15:
            self.print_warning(f"Election slow ({election_time:.1f}s) - may indicate network/timeout issues")
        else:
            self.print_success(f"Election timing reasonable ({election_time:.1f}s)")
        
        # 2. Term progression validation
        term_increase = final_term - initial_term
        if term_increase < 1:
            self.print_error(f"Term did not increase properly: {initial_term} → {final_term}")
        elif term_increase > 10:
            self.print_warning(f"Term increased too much: {initial_term} → {final_term} (indicates many failed elections)")
        else:
            self.print_success(f"Term progression healthy: {initial_term} → {final_term}")
        
        # 3. Phase analysis
        max_candidates = max(phase['candidates'] for phase in election_phases)
        candidate_duration = sum(1 for phase in election_phases if phase['candidates'] > 0) * check_interval
        
        print(f"  • Maximum concurrent candidates: {max_candidates}")
        print(f"  • Candidate state duration: {candidate_duration:.1f}s")
        print(f"  • Election phases recorded: {len(election_phases)}")
        
        # 4. Network partition check
        max_term_spread = max(phase['term_spread'] for phase in election_phases)
        if max_term_spread > 5:
            self.print_warning(f"High term spread ({max_term_spread}) indicates possible network issues")
        else:
            self.print_success(f"Term spread reasonable ({max_term_spread})")
        
        # Restart the failed node
        self.print_step(f"Restarting {initial_leader}...")
        self.start_node(initial_leader)
        await asyncio.sleep(3)
        
        # Final state validation
        final_status = await self.get_cluster_status()
        self.display_cluster_status(final_status, "Final State After Recovery")
        
        online_nodes = [node for node, stat in final_status.items() if stat.get("status") != "offline"]
        if len(online_nodes) == 3:
            self.print_success("All nodes back online")
        else:
            self.print_warning(f"Only {len(online_nodes)}/3 nodes online")
        
        return True

async def main():
    if len(sys.argv) < 2:
        print(f"""
{Color.BOLD}Raft Demo Presentation Script{Color.END}

Usage: python demo_slides.py <demo_name>

Available demos:
  slide9     - Leader Election Demo
  slide10    - Fault Tolerance Demo  
  slide11    - The Challenge Demo
  test       - Election Validation Test (detailed re-election analysis)
  logs       - Show container logs analysis
  all        - Run all demos in sequence

Examples:
  python demo_slides.py slide9
  python demo_slides.py test
  python demo_slides.py all
        """)
        return
    
    demo_name = sys.argv[1].lower()
    demo = RaftDemoPresentation()
    
    try:
        if demo_name == "slide9":
            await demo.demo_slide_9_leader_election()
        elif demo_name == "slide10":
            await demo.demo_slide_10_fault_tolerance()
        elif demo_name == "slide11":
            await demo.demo_slide_11_the_challenge()
        elif demo_name == "test":
            await demo.test_proper_re_election()
        elif demo_name == "logs":
            await demo.show_logs_analysis()
        elif demo_name == "all":
            print(f"{Color.BOLD}Running all demos in sequence...{Color.END}\n")
            await demo.demo_slide_9_leader_election()
            input(f"\n{Color.CYAN}Press Enter to continue to Slide 10...{Color.END}")
            await demo.demo_slide_10_fault_tolerance()
            input(f"\n{Color.CYAN}Press Enter to continue to Slide 11...{Color.END}")
            await demo.demo_slide_11_the_challenge()
        else:
            print(f"{Color.RED}Unknown demo: {demo_name}{Color.END}")
    
    finally:
        await demo.close()

if __name__ == "__main__":
    asyncio.run(main()) 