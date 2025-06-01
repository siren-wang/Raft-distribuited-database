import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { RefreshCw, Zap, AlertCircle, CheckCircle, XCircle, Send, Power } from 'lucide-react';

const RaftVisualDashboard = () => {
  const [nodes, setNodes] = useState([
    { id: 'node1', port: 8001, state: 'follower', term: 0, leader: null, status: 'healthy', logs: 0 },
    { id: 'node2', port: 8002, state: 'follower', term: 0, leader: null, status: 'healthy', logs: 0 },
    { id: 'node3', port: 8003, state: 'follower', term: 0, leader: null, status: 'healthy', logs: 0 }
  ]);
  
  const [operations, setOperations] = useState([]);
  const [demoState, setDemoState] = useState('idle');
  const [currentDemo, setCurrentDemo] = useState(null);
  const [kvData, setKvData] = useState({});

  // Fetch cluster status
  const fetchClusterStatus = async () => {
    try {
      const promises = nodes.map(async (node) => {
        try {
          const response = await fetch(`http://localhost:${node.port}/raft/status`);
          if (response.ok) {
            const data = await response.json();
            return {
              ...node,
              state: data.state,
              term: data.current_term,
              leader: data.current_leader,
              logs: data.log_length,
              commitIndex: data.commit_index,
              status: 'healthy'
            };
          }
        } catch (error) {
          return { ...node, status: 'offline' };
        }
      });
      
      const updatedNodes = await Promise.all(promises);
      setNodes(updatedNodes);
    } catch (error) {
      console.error('Error fetching cluster status:', error);
    }
  };

  // Auto-refresh status
  useEffect(() => {
    fetchClusterStatus();
    const interval = setInterval(fetchClusterStatus, 1000);
    return () => clearInterval(interval);
  }, []);

  // Demo scenarios
  const demos = [
    {
      id: 'leader-election',
      name: 'Leader Election',
      description: 'Watch how nodes elect a leader when starting up',
      steps: [
        { action: 'Starting cluster...', delay: 1000 },
        { action: 'Nodes exchanging votes...', delay: 2000 },
        { action: 'Leader elected!', delay: 1000 }
      ]
    },
    {
      id: 'data-replication',
      name: 'Data Replication',
      description: 'See how data is replicated across all nodes',
      steps: [
        { action: 'Writing data to leader...', delay: 1000 },
        { action: 'Replicating to followers...', delay: 1500 },
        { action: 'Data committed on majority!', delay: 1000 }
      ]
    },
    {
      id: 'leader-failure',
      name: 'Leader Failure',
      description: 'Simulate leader failure and watch re-election',
      steps: [
        { action: 'Stopping current leader...', delay: 1000 },
        { action: 'Followers detect leader failure...', delay: 2000 },
        { action: 'New election started...', delay: 1500 },
        { action: 'New leader elected!', delay: 1000 }
      ]
    },
    {
      id: 'network-partition',
      name: 'Network Partition',
      description: 'Simulate network split and recovery',
      steps: [
        { action: 'Creating network partition...', delay: 1000 },
        { action: 'Minority partition cannot elect leader...', delay: 2000 },
        { action: 'Healing partition...', delay: 1500 },
        { action: 'Cluster synchronized!', delay: 1000 }
      ]
    },
    {
      id: 'concurrent-writes',
      name: 'Concurrent Writes',
      description: 'Multiple clients writing simultaneously',
      steps: [
        { action: 'Client 1 writing...', delay: 500 },
        { action: 'Client 2 writing...', delay: 500 },
        { action: 'Client 3 writing...', delay: 500 },
        { action: 'All writes replicated!', delay: 1000 }
      ]
    }
  ];

  const runDemo = async (demo) => {
    setDemoState('running');
    setCurrentDemo(demo.id);
    setOperations([]);
    
    for (const step of demo.steps) {
      setOperations(prev => [...prev, { text: step.action, timestamp: new Date() }]);
      await new Promise(resolve => setTimeout(resolve, step.delay));
    }
    
    setDemoState('completed');
    setTimeout(() => {
      setDemoState('idle');
      setCurrentDemo(null);
    }, 2000);
  };

  const writeData = async () => {
    const leaderNode = nodes.find(n => n.state === 'leader');
    if (!leaderNode) {
      alert('No leader elected yet!');
      return;
    }
    
    const key = `key-${Date.now()}`;
    const value = { data: `value-${Math.random()}`, timestamp: new Date() };
    
    try {
      const response = await fetch(`http://localhost:${leaderNode.port}/kv/${key}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value })
      });
      
      if (response.ok) {
        setOperations(prev => [...prev, { 
          text: `Wrote ${key} to leader ${leaderNode.id}`, 
          timestamp: new Date() 
        }]);
        setKvData(prev => ({ ...prev, [key]: value }));
      }
    } catch (error) {
      console.error('Write failed:', error);
    }
  };

  const stopNode = async (nodeId) => {
    // In real implementation, would call docker stop
    setNodes(prev => prev.map(n => 
      n.id === nodeId ? { ...n, status: 'offline' } : n
    ));
    setOperations(prev => [...prev, { 
      text: `Stopped ${nodeId}`, 
      timestamp: new Date() 
    }]);
  };

  const startNode = async (nodeId) => {
    // In real implementation, would call docker start
    setNodes(prev => prev.map(n => 
      n.id === nodeId ? { ...n, status: 'healthy' } : n
    ));
    setOperations(prev => [...prev, { 
      text: `Started ${nodeId}`, 
      timestamp: new Date() 
    }]);
  };

  const getNodeColor = (node) => {
    if (node.status === 'offline') return 'bg-gray-100 border-gray-300';
    if (node.state === 'leader') return 'bg-green-50 border-green-500';
    if (node.state === 'candidate') return 'bg-yellow-50 border-yellow-500';
    return 'bg-blue-50 border-blue-300';
  };

  const getStateIcon = (state) => {
    if (state === 'leader') return <Zap className="w-5 h-5 text-green-600" />;
    if (state === 'candidate') return <AlertCircle className="w-5 h-5 text-yellow-600" />;
    return <CheckCircle className="w-5 h-5 text-blue-600" />;
  };

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">
      <div className="text-center mb-8">
        <h1 className="text-4xl font-bold mb-2">Raft Consensus Visualization</h1>
        <p className="text-gray-600">Interactive demonstration of distributed consensus</p>
      </div>

      {/* Cluster Status */}
      <div className="grid grid-cols-3 gap-4">
        {nodes.map(node => (
          <Card key={node.id} className={`${getNodeColor(node)} border-2 transition-all duration-300`}>
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center justify-between">
                <span className="flex items-center gap-2">
                  {getStateIcon(node.state)}
                  {node.id}
                </span>
                <Badge variant={node.status === 'healthy' ? 'success' : 'secondary'}>
                  {node.status}
                </Badge>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="font-medium">State:</span>
                  <span className="capitalize">{node.state}</span>
                </div>
                <div className="flex justify-between">
                  <span className="font-medium">Term:</span>
                  <span>{node.term}</span>
                </div>
                <div className="flex justify-between">
                  <span className="font-medium">Logs:</span>
                  <span>{node.logs}</span>
                </div>
                {node.leader && (
                  <div className="flex justify-between">
                    <span className="font-medium">Leader:</span>
                    <span>{node.leader}</span>
                  </div>
                )}
                <div className="flex gap-2 mt-3">
                  <Button 
                    size="sm" 
                    variant={node.status === 'healthy' ? 'destructive' : 'default'}
                    onClick={() => node.status === 'healthy' ? stopNode(node.id) : startNode(node.id)}
                  >
                    <Power className="w-4 h-4" />
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Demo Scenarios */}
      <Card>
        <CardHeader>
          <CardTitle>Demo Scenarios</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {demos.map(demo => (
              <Button
                key={demo.id}
                variant="outline"
                onClick={() => runDemo(demo)}
                disabled={demoState === 'running'}
                className={currentDemo === demo.id ? 'ring-2 ring-blue-500' : ''}
              >
                {demo.name}
              </Button>
            ))}
          </div>
          
          {currentDemo && (
            <Alert className="mt-4">
              <AlertDescription>
                {demos.find(d => d.id === currentDemo)?.description}
              </AlertDescription>
            </Alert>
          )}
        </CardContent>
      </Card>

      {/* Actions */}
      <Card>
        <CardHeader>
          <CardTitle>Actions</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-3">
            <Button onClick={writeData} className="flex items-center gap-2">
              <Send className="w-4 h-4" />
              Write Data
            </Button>
            <Button onClick={fetchClusterStatus} variant="outline" className="flex items-center gap-2">
              <RefreshCw className="w-4 h-4" />
              Refresh Status
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Operations Log */}
      <Card className="max-h-64 overflow-hidden">
        <CardHeader>
          <CardTitle>Operations Log</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-1 overflow-y-auto max-h-40">
            {operations.length === 0 ? (
              <p className="text-gray-500 text-sm">No operations yet...</p>
            ) : (
              operations.slice(-10).reverse().map((op, idx) => (
                <div key={idx} className="text-sm flex justify-between">
                  <span>{op.text}</span>
                  <span className="text-gray-400">
                    {op.timestamp.toLocaleTimeString()}
                  </span>
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default RaftVisualDashboard;