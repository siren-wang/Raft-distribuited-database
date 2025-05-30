"""
Metrics Collection and Monitoring for Raft Consensus
Provides comprehensive metrics for cluster health and performance monitoring
"""

import time
import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from collections import defaultdict, deque
from prometheus_client import Counter, Histogram, Gauge, Summary, Info
from prometheus_client.core import CollectorRegistry
import json

logger = logging.getLogger(__name__)


class RaftMetrics:
    """
    Comprehensive metrics collection for Raft consensus
    """
    
    def __init__(self, node_id: str, registry: Optional[CollectorRegistry] = None):
        self.node_id = node_id
        self.registry = registry or CollectorRegistry()
        
        # Initialize Prometheus metrics
        self._init_prometheus_metrics()
        
        # Internal metrics storage for custom calculations
        self.rpc_latencies: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.election_history: deque = deque(maxlen=100)
        self.commit_history: deque = deque(maxlen=1000)
        self.last_metrics_reset = datetime.utcnow()
        
        # Performance tracking
        self.start_time = time.time()
        self.last_leader_change = None
        self.last_election_start = None
        
        logger.info(f"Initialized RaftMetrics for node {node_id}")
    
    def _init_prometheus_metrics(self):
        """Initialize Prometheus metrics"""
        # Node information
        self.node_info = Info(
            'raft_node_info',
            'Raft node information',
            ['node_id'],
            registry=self.registry
        )
        self.node_info.labels(node_id=self.node_id).info({
            'node_id': self.node_id,
            'start_time': str(datetime.utcnow())
        })
        
        # State metrics
        self.node_state = Gauge(
            'raft_node_state',
            'Current node state (0=follower, 1=candidate, 2=leader)',
            ['node_id'],
            registry=self.registry
        )
        
        self.current_term = Gauge(
            'raft_current_term',
            'Current term number',
            ['node_id'],
            registry=self.registry
        )
        
        self.commit_index = Gauge(
            'raft_commit_index',
            'Current commit index',
            ['node_id'],
            registry=self.registry
        )
        
        self.last_applied = Gauge(
            'raft_last_applied',
            'Last applied log index',
            ['node_id'],
            registry=self.registry
        )
        
        self.log_size = Gauge(
            'raft_log_size',
            'Number of entries in the log',
            ['node_id'],
            registry=self.registry
        )
        
        # Election metrics
        self.elections_started = Counter(
            'raft_elections_started_total',
            'Total number of elections started',
            ['node_id'],
            registry=self.registry
        )
        
        self.elections_won = Counter(
            'raft_elections_won_total',
            'Total number of elections won',
            ['node_id'],
            registry=self.registry
        )
        
        self.election_duration = Histogram(
            'raft_election_duration_seconds',
            'Duration of leader elections',
            ['node_id'],
            registry=self.registry,
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
        )
        
        # RPC metrics
        self.rpc_sent = Counter(
            'raft_rpc_sent_total',
            'Total number of RPCs sent',
            ['node_id', 'rpc_type', 'target_node'],
            registry=self.registry
        )
        
        self.rpc_received = Counter(
            'raft_rpc_received_total',
            'Total number of RPCs received',
            ['node_id', 'rpc_type', 'source_node'],
            registry=self.registry
        )
        
        self.rpc_latency = Histogram(
            'raft_rpc_latency_seconds',
            'RPC latency',
            ['node_id', 'rpc_type', 'target_node'],
            registry=self.registry,
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
        )
        
        self.rpc_errors = Counter(
            'raft_rpc_errors_total',
            'Total number of RPC errors',
            ['node_id', 'rpc_type', 'error_type'],
            registry=self.registry
        )
        
        # Log replication metrics
        self.log_entries_appended = Counter(
            'raft_log_entries_appended_total',
            'Total number of log entries appended',
            ['node_id'],
            registry=self.registry
        )
        
        self.log_entries_committed = Counter(
            'raft_log_entries_committed_total',
            'Total number of log entries committed',
            ['node_id'],
            registry=self.registry
        )
        
        self.log_entries_applied = Counter(
            'raft_log_entries_applied_total',
            'Total number of log entries applied',
            ['node_id'],
            registry=self.registry
        )
        
        self.replication_lag = Gauge(
            'raft_replication_lag',
            'Replication lag for each follower',
            ['node_id', 'follower_id'],
            registry=self.registry
        )
        
        # Performance metrics
        self.throughput = Gauge(
            'raft_throughput_ops_per_sec',
            'Operations per second',
            ['node_id', 'operation_type'],
            registry=self.registry
        )
        
        self.batch_size = Histogram(
            'raft_batch_size',
            'Size of batched operations',
            ['node_id'],
            registry=self.registry,
            buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000)
        )
        
        # Cluster health metrics
        self.cluster_size = Gauge(
            'raft_cluster_size',
            'Number of nodes in the cluster',
            ['node_id'],
            registry=self.registry
        )
        
        self.healthy_nodes = Gauge(
            'raft_healthy_nodes',
            'Number of healthy nodes',
            ['node_id'],
            registry=self.registry
        )
        
        self.leader_changes = Counter(
            'raft_leader_changes_total',
            'Total number of leader changes',
            ['node_id'],
            registry=self.registry
        )
        
        # Storage metrics
        self.wal_size = Gauge(
            'raft_wal_size_bytes',
            'Size of Write-Ahead Log in bytes',
            ['node_id'],
            registry=self.registry
        )
        
        self.snapshot_size = Gauge(
            'raft_snapshot_size_bytes',
            'Size of latest snapshot in bytes',
            ['node_id'],
            registry=self.registry
        )
        
        self.compaction_duration = Histogram(
            'raft_compaction_duration_seconds',
            'Duration of log compaction',
            ['node_id'],
            registry=self.registry,
            buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0)
        )
    
    def update_state(self, state: str, term: int):
        """Update node state metrics"""
        state_map = {"follower": 0, "candidate": 1, "leader": 2}
        self.node_state.labels(node_id=self.node_id).set(state_map.get(state, 0))
        self.current_term.labels(node_id=self.node_id).set(term)
        
        if state == "leader" and self.last_leader_change != state:
            self.leader_changes.labels(node_id=self.node_id).inc()
            self.last_leader_change = state
    
    def update_log_indices(self, commit_index: int, last_applied: int, log_size: int):
        """Update log-related metrics"""
        self.commit_index.labels(node_id=self.node_id).set(commit_index)
        self.last_applied.labels(node_id=self.node_id).set(last_applied)
        self.log_size.labels(node_id=self.node_id).set(log_size)
    
    def record_election_started(self):
        """Record that an election has started"""
        self.elections_started.labels(node_id=self.node_id).inc()
        self.last_election_start = time.time()
        self.election_history.append({
            "timestamp": datetime.utcnow(),
            "type": "started"
        })
    
    def record_election_won(self):
        """Record that an election was won"""
        self.elections_won.labels(node_id=self.node_id).inc()
        
        if self.last_election_start:
            duration = time.time() - self.last_election_start
            self.election_duration.labels(node_id=self.node_id).observe(duration)
            
            self.election_history.append({
                "timestamp": datetime.utcnow(),
                "type": "won",
                "duration": duration
            })
    
    def record_rpc_sent(self, rpc_type: str, target_node: str):
        """Record an outgoing RPC"""
        self.rpc_sent.labels(
            node_id=self.node_id,
            rpc_type=rpc_type,
            target_node=target_node
        ).inc()
    
    def record_rpc_received(self, rpc_type: str, source_node: str):
        """Record an incoming RPC"""
        self.rpc_received.labels(
            node_id=self.node_id,
            rpc_type=rpc_type,
            source_node=source_node
        ).inc()
    
    def record_rpc_latency(self, rpc_type: str, target_node: str, latency: float):
        """Record RPC latency"""
        self.rpc_latency.labels(
            node_id=self.node_id,
            rpc_type=rpc_type,
            target_node=target_node
        ).observe(latency)
        
        # Store for custom calculations
        key = f"{rpc_type}:{target_node}"
        self.rpc_latencies[key].append(latency)
    
    def record_rpc_error(self, rpc_type: str, error_type: str):
        """Record an RPC error"""
        self.rpc_errors.labels(
            node_id=self.node_id,
            rpc_type=rpc_type,
            error_type=error_type
        ).inc()
    
    def record_log_append(self, count: int = 1):
        """Record log entries appended"""
        self.log_entries_appended.labels(node_id=self.node_id).inc(count)
    
    def record_log_commit(self, count: int = 1):
        """Record log entries committed"""
        self.log_entries_committed.labels(node_id=self.node_id).inc(count)
        self.commit_history.append({
            "timestamp": datetime.utcnow(),
            "count": count
        })
    
    def record_log_apply(self, count: int = 1):
        """Record log entries applied"""
        self.log_entries_applied.labels(node_id=self.node_id).inc(count)
    
    def update_replication_lag(self, follower_id: str, lag: int):
        """Update replication lag for a follower"""
        self.replication_lag.labels(
            node_id=self.node_id,
            follower_id=follower_id
        ).set(lag)
    
    def record_batch_size(self, size: int):
        """Record batch size"""
        self.batch_size.labels(node_id=self.node_id).observe(size)
    
    def update_cluster_health(self, total_nodes: int, healthy_nodes: int):
        """Update cluster health metrics"""
        self.cluster_size.labels(node_id=self.node_id).set(total_nodes)
        self.healthy_nodes.labels(node_id=self.node_id).set(healthy_nodes)
    
    def update_storage_metrics(self, wal_size: int, snapshot_size: int):
        """Update storage-related metrics"""
        self.wal_size.labels(node_id=self.node_id).set(wal_size)
        self.snapshot_size.labels(node_id=self.node_id).set(snapshot_size)
    
    def record_compaction(self, duration: float):
        """Record log compaction duration"""
        self.compaction_duration.labels(node_id=self.node_id).observe(duration)
    
    def calculate_throughput(self) -> Dict[str, float]:
        """Calculate current throughput metrics"""
        now = datetime.utcnow()
        window = timedelta(seconds=60)  # 1-minute window
        
        # Calculate commit throughput
        recent_commits = [
            c for c in self.commit_history
            if now - c["timestamp"] < window
        ]
        
        commit_count = sum(c["count"] for c in recent_commits)
        commit_throughput = commit_count / 60.0  # ops/sec
        
        self.throughput.labels(
            node_id=self.node_id,
            operation_type="commit"
        ).set(commit_throughput)
        
        return {
            "commit_throughput": commit_throughput
        }
    
    def get_rpc_statistics(self) -> Dict[str, Any]:
        """Get RPC latency statistics"""
        stats = {}
        
        for key, latencies in self.rpc_latencies.items():
            if latencies:
                sorted_latencies = sorted(latencies)
                stats[key] = {
                    "count": len(latencies),
                    "mean": sum(latencies) / len(latencies),
                    "min": sorted_latencies[0],
                    "max": sorted_latencies[-1],
                    "p50": sorted_latencies[len(latencies) // 2],
                    "p95": sorted_latencies[int(len(latencies) * 0.95)],
                    "p99": sorted_latencies[int(len(latencies) * 0.99)]
                }
        
        return stats
    
    def get_election_statistics(self) -> Dict[str, Any]:
        """Get election statistics"""
        won_elections = [e for e in self.election_history if e["type"] == "won"]
        
        if won_elections:
            durations = [e["duration"] for e in won_elections if "duration" in e]
            if durations:
                return {
                    "total_elections": len(self.election_history),
                    "won_elections": len(won_elections),
                    "avg_duration": sum(durations) / len(durations),
                    "min_duration": min(durations),
                    "max_duration": max(durations)
                }
        
        return {
            "total_elections": len(self.election_history),
            "won_elections": 0
        }
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get a comprehensive health summary"""
        uptime = time.time() - self.start_time
        throughput = self.calculate_throughput()
        
        return {
            "node_id": self.node_id,
            "uptime_seconds": uptime,
            "throughput": throughput,
            "rpc_statistics": self.get_rpc_statistics(),
            "election_statistics": self.get_election_statistics(),
            "last_metrics_reset": self.last_metrics_reset.isoformat()
        }
    
    def reset_metrics(self):
        """Reset internal metrics (not Prometheus counters)"""
        self.rpc_latencies.clear()
        self.election_history.clear()
        self.commit_history.clear()
        self.last_metrics_reset = datetime.utcnow()
        logger.info("Reset internal metrics")


class MetricsCollector:
    """
    Async metrics collector that periodically updates metrics
    """
    
    def __init__(self, metrics: RaftMetrics, collection_interval: float = 5.0):
        self.metrics = metrics
        self.collection_interval = collection_interval
        self.running = False
        self._collection_task: Optional[asyncio.Task] = None
        
        # Callbacks for collecting metrics
        self.state_callback = None
        self.log_callback = None
        self.cluster_callback = None
        self.storage_callback = None
    
    def set_callbacks(self, 
                     state_callback=None,
                     log_callback=None,
                     cluster_callback=None,
                     storage_callback=None):
        """Set callbacks for collecting metrics from various sources"""
        self.state_callback = state_callback
        self.log_callback = log_callback
        self.cluster_callback = cluster_callback
        self.storage_callback = storage_callback
    
    async def start(self):
        """Start the metrics collector"""
        self.running = True
        self._collection_task = asyncio.create_task(self._collection_loop())
        logger.info("Started metrics collector")
    
    async def stop(self):
        """Stop the metrics collector"""
        self.running = False
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped metrics collector")
    
    async def _collection_loop(self):
        """Main collection loop"""
        while self.running:
            try:
                await self._collect_metrics()
                await asyncio.sleep(self.collection_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
    
    async def _collect_metrics(self):
        """Collect metrics from all sources"""
        try:
            # Collect state metrics
            if self.state_callback:
                state_data = await self.state_callback()
                self.metrics.update_state(
                    state_data["state"],
                    state_data["term"]
                )
            
            # Collect log metrics
            if self.log_callback:
                log_data = await self.log_callback()
                self.metrics.update_log_indices(
                    log_data["commit_index"],
                    log_data["last_applied"],
                    log_data["log_size"]
                )
            
            # Collect cluster metrics
            if self.cluster_callback:
                cluster_data = await self.cluster_callback()
                self.metrics.update_cluster_health(
                    cluster_data["total_nodes"],
                    cluster_data["healthy_nodes"]
                )
            
            # Collect storage metrics
            if self.storage_callback:
                storage_data = await self.storage_callback()
                self.metrics.update_storage_metrics(
                    storage_data["wal_size"],
                    storage_data["snapshot_size"]
                )
            
            # Calculate derived metrics
            self.metrics.calculate_throughput()
            
        except Exception as e:
            logger.error(f"Error in metrics collection: {e}")