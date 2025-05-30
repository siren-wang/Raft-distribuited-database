#!/bin/bash
# Script to generate gRPC Python code from protobuf definitions

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo -e "${RED}Error: protoc is not installed${NC}"
    echo "Please install protobuf compiler:"
    echo "  Ubuntu/Debian: sudo apt-get install protobuf-compiler"
    echo "  MacOS: brew install protobuf"
    echo "  Or download from: https://github.com/protocolbuffers/protobuf/releases"
    exit 1
fi

# Check if grpc tools are installed
if ! python -c "import grpc_tools" 2>/dev/null; then
    echo -e "${YELLOW}Installing grpcio-tools...${NC}"
    pip install grpcio-tools
fi

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Paths
PROTO_DIR="$PROJECT_ROOT/src/raft"
PROTO_FILE="$PROTO_DIR/raft.proto"
OUTPUT_DIR="$PROJECT_ROOT/src/raft/generated"

# Create output directory
mkdir -p "$OUTPUT_DIR"
touch "$OUTPUT_DIR/__init__.py"

# Check if proto file exists
if [ ! -f "$PROTO_FILE" ]; then
    echo -e "${RED}Error: Proto file not found at $PROTO_FILE${NC}"
    exit 1
fi

echo -e "${GREEN}Generating gRPC code from $PROTO_FILE...${NC}"

# Generate Python code
python -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$OUTPUT_DIR" \
    --grpc_python_out="$OUTPUT_DIR" \
    "$PROTO_FILE"

# Fix imports in generated files (grpc tools generate absolute imports)
echo -e "${YELLOW}Fixing imports in generated files...${NC}"

# Update imports to be relative
if [ -f "$OUTPUT_DIR/raft_pb2_grpc.py" ]; then
    # On macOS, use -i '' for in-place editing, on Linux use -i
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' 's/import raft_pb2/from . import raft_pb2/' "$OUTPUT_DIR/raft_pb2_grpc.py"
    else
        sed -i 's/import raft_pb2/from . import raft_pb2/' "$OUTPUT_DIR/raft_pb2_grpc.py"
    fi
fi

# Create __init__.py for easy imports
cat > "$OUTPUT_DIR/__init__.py" << EOF
"""
Generated gRPC code for Raft consensus protocol
"""

from .raft_pb2 import *
from .raft_pb2_grpc import *

__all__ = [
    # Service
    'RaftServiceStub',
    'RaftServiceServicer',
    'add_RaftServiceServicer_to_server',
    
    # Core messages
    'RequestVoteRequest',
    'RequestVoteResponse',
    'AppendEntriesRequest',
    'AppendEntriesResponse',
    'LogEntry',
    
    # Snapshot
    'InstallSnapshotRequest',
    'InstallSnapshotResponse',
    'SnapshotMetadata',
    
    # Cluster management
    'NodeInfo',
    'NodeState',
    'AddNodeRequest',
    'AddNodeResponse',
    'RemoveNodeRequest',
    'RemoveNodeResponse',
    'GetClusterInfoRequest',
    'GetClusterInfoResponse',
    'ClusterConfig',
    'ClusterStats',
    
    # Health and metrics
    'HealthCheckRequest',
    'HealthCheckResponse',
    'HealthDetails',
    'GetMetricsRequest',
    'GetMetricsResponse',
    'MetricValue',
    'Histogram',
    
    # Client operations
    'ForwardRequestMsg',
    'ForwardResponseMsg',
    'GetRequest',
    'GetResponse',
    'PutRequest',
    'PutResponse',
    'DeleteRequest',
    'DeleteResponse',
    'CompareAndSwapRequest',
    'CompareAndSwapResponse',
    'BatchRequest',
    'BatchResponse',
]
EOF

echo -e "${GREEN}✓ gRPC code generation complete!${NC}"
echo -e "Generated files in: $OUTPUT_DIR"
echo -e "  - raft_pb2.py (message definitions)"
echo -e "  - raft_pb2_grpc.py (service definitions)"

# Verify generation
if [ -f "$OUTPUT_DIR/raft_pb2.py" ] && [ -f "$OUTPUT_DIR/raft_pb2_grpc.py" ]; then
    echo -e "${GREEN}✓ All files generated successfully${NC}"
else
    echo -e "${RED}✗ Some files were not generated${NC}"
    exit 1
fi