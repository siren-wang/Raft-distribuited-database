#!/bin/bash
# Script to run and manage Raft cluster

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
COMPOSE_FILE="docker-compose-raft.yml"
CLUSTER_SIZE=3

# Functions
print_help() {
    echo "Raft Cluster Management Script"
    echo ""
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  start       Start the Raft cluster"
    echo "  stop        Stop the Raft cluster"
    echo "  restart     Restart the Raft cluster"
    echo "  status      Show cluster status"
    echo "  logs        Show cluster logs"
    echo "  demo        Run the demo script"
    echo "  clean       Clean up all data"
    echo "  test        Run tests"
    echo ""
    echo "Options:"
    echo "  -h, --help  Show this help message"
}

start_cluster() {
    echo -e "${GREEN}Starting Raft cluster...${NC}"
    
    # Build images
    docker-compose -f $COMPOSE_FILE build
    
    # Start services
    docker-compose -f $COMPOSE_FILE up -d
    
    echo -e "${GREEN}Waiting for cluster to initialize...${NC}"
    sleep 5
    
    # Check cluster health
    check_health
    
    echo -e "${GREEN}Raft cluster started successfully!${NC}"
    echo ""
    echo "Nodes:"
    echo "  - Node 1: http://localhost:8001"
    echo "  - Node 2: http://localhost:8002"
    echo "  - Node 3: http://localhost:8003"
    echo "  - Load Balancer: http://localhost:8000"
}

stop_cluster() {
    echo -e "${YELLOW}Stopping Raft cluster...${NC}"
    docker-compose -f $COMPOSE_FILE down
    echo -e "${GREEN}Raft cluster stopped.${NC}"
}

restart_cluster() {
    stop_cluster
    start_cluster
}

show_status() {
    echo -e "${GREEN}Raft Cluster Status${NC}"
    echo "===================="
    
    for i in 1 2 3; do
        echo ""
        echo "Node $i (port 800$i):"
        
        # Get status
        response=$(curl -s http://localhost:800$i/raft/status 2>/dev/null || echo "OFFLINE")
        
        if [ "$response" != "OFFLINE" ]; then
            # Parse JSON response
            state=$(echo $response | jq -r '.state' 2>/dev/null || echo "unknown")
            term=$(echo $response | jq -r '.current_term' 2>/dev/null || echo "?")
            leader=$(echo $response | jq -r '.current_leader' 2>/dev/null || echo "none")
            log_length=$(echo $response | jq -r '.log_length' 2>/dev/null || echo "?")
            commit_index=$(echo $response | jq -r '.commit_index' 2>/dev/null || echo "?")
            
            # Color code based on state
            if [ "$state" = "leader" ]; then
                echo -e "  State: ${GREEN}LEADER${NC}"
            elif [ "$state" = "follower" ]; then
                echo -e "  State: ${YELLOW}FOLLOWER${NC}"
            elif [ "$state" = "candidate" ]; then
                echo -e "  State: ${RED}CANDIDATE${NC}"
            else
                echo -e "  State: $state"
            fi
            
            echo "  Term: $term"
            echo "  Current Leader: $leader"
            echo "  Log Length: $log_length"
            echo "  Commit Index: $commit_index"
        else
            echo -e "  State: ${RED}OFFLINE${NC}"
        fi
    done
    
    echo ""
}

show_logs() {
    echo -e "${GREEN}Showing cluster logs (Ctrl+C to exit)...${NC}"
    docker-compose -f $COMPOSE_FILE logs -f
}

run_demo() {
    echo -e "${GREEN}Running Raft demo...${NC}"
    
    # Check if cluster is running
    if ! docker-compose -f $COMPOSE_FILE ps | grep -q "Up"; then
        echo -e "${RED}Error: Cluster is not running. Start it first with '$0 start'${NC}"
        exit 1
    fi
    
    # Run demo script
    python examples/raft_demo.py
}

clean_data() {
    echo -e "${YELLOW}Cleaning up all data...${NC}"
    
    # Stop cluster
    docker-compose -f $COMPOSE_FILE down -v
    
    # Remove local directories
    rm -rf raft_state raft_wal
    
    echo -e "${GREEN}Cleanup complete.${NC}"
}

run_tests() {
    echo -e "${GREEN}Running Raft tests...${NC}"
    
    # Run unit tests
    pytest tests/test_raft_state.py -v
    pytest tests/test_raft_node.py -v
    
    echo -e "${GREEN}Tests complete.${NC}"
}

check_health() {
    # Wait for at least one node to be healthy
    for attempt in {1..30}; do
        for i in 1 2 3; do
            if curl -s http://localhost:800$i/raft/status >/dev/null 2>&1; then
                return 0
            fi
        done
        echo -n "."
        sleep 1
    done
    
    echo ""
    echo -e "${RED}Warning: Cluster may not be fully initialized${NC}"
    return 1
}

simulate_failure() {
    node=$1
    if [ -z "$node" ]; then
        echo "Usage: $0 fail <node_number>"
        echo "Example: $0 fail 1"
        exit 1
    fi
    
    echo -e "${YELLOW}Simulating failure of node $node...${NC}"
    docker-compose -f $COMPOSE_FILE stop raft-node$node
    echo -e "${GREEN}Node $node stopped. Check cluster status to see failover.${NC}"
}

recover_node() {
    node=$1
    if [ -z "$node" ]; then
        echo "Usage: $0 recover <node_number>"
        echo "Example: $0 recover 1"
        exit 1
    fi
    
    echo -e "${GREEN}Recovering node $node...${NC}"
    docker-compose -f $COMPOSE_FILE start raft-node$node
    echo -e "${GREEN}Node $node started. It will rejoin the cluster.${NC}"
}

# Main script logic
case "$1" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    restart)
        restart_cluster
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    demo)
        run_demo
        ;;
    clean)
        clean_data
        ;;
    test)
        run_tests
        ;;
    fail)
        simulate_failure $2
        ;;
    recover)
        recover_node $2
        ;;
    -h|--help|help)
        print_help
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo ""
        print_help
        exit 1
        ;;
esac