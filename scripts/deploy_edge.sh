#!/bin/bash

set -e

echo "=== AI Robot Memory System - Edge Deployment Script ==="
echo "Target: NVIDIA Jetson (Edge Device)"

PROJECT_NAME="ai-robot-memory-system"
TARGET_HOST="${JETSON_HOST:-jetson-nano.local}"
TARGET_USER="${JETSON_USER:-nvidia}"
TARGET_DIR="/home/${TARGET_USER}/ai-robot-memory-system"

CORTEX_VERSION="${CORTEX_VERSION:-latest}"
OLLAMA_VERSION="${OLLAMA_VERSION:-latest}"

echo "Configuration:"
echo "  Target Host: ${TARGET_HOST}"
echo "  Target User: ${TARGET_USER}"
echo "  Target Dir:  ${TARGET_DIR}"
echo ""

echo "[1/7] Checking prerequisites..."
if ! command -v rsync &> /dev/null; then
    echo "Error: rsync not found. Install with: sudo apt install rsync"
    exit 1
fi

echo "[2/7] Creating deployment package..."
mkdir -p deploy_${PROJECT_NAME}
rsync -av --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' --exclude='.pytest_cache' \
    --exclude='node_modules' --exclude='*.egg-info' \
    ./ai-robot-memory-system/ deploy_${PROJECT_NAME}/

echo "[3/7] Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "[4/7] Syncing to edge device..."
rsync -avz --delete \
    --exclude='.git' \
    --exclude='venv' \
    --exclude='*.log' \
    deploy_${PROJECT_NAME}/ ${TARGET_USER}@${TARGET_HOST}:${TARGET_DIR}/

echo "[5/7] Installing dependencies on edge device..."
ssh ${TARGET_USER}@${TARGET_HOST} << 'ENDSSH'
    set -e
    cd /home/${USER}/ai-robot-memory-system

    if ! command -v python3 &> /dev/null; then
        echo "Installing Python 3..."
        sudo apt-get update
        sudo apt-get install -y python3 python3-pip python3-venv
    fi

    if [ ! -d "venv" ]; then
        echo "Creating virtual environment..."
        python3 -m venv venv
    fi

    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt

    if ! command -v cortexdb &> /dev/null; then
        echo "Installing CortexDB..."
        pip install cortexdb-client
    fi

    if ! command -v ollama &> /dev/null; then
        echo "Warning: Ollama not installed. Install manually for LLM support."
    fi

    echo "Installation complete!"
ENDSSH

echo "[6/7] Configuring CortexDB connection..."
ssh ${TARGET_USER}@${TARGET_HOST} << 'ENDSSH'
    cd /home/${USER}/ai-robot-memory-system

    if [ -f "config/cortex_config.yaml" ]; then
        sed -i 's/host: localhost/host: ${CORTEX_HOST:-localhost}/' config/cortex_config.yaml
        sed -i 's/port: 5000/port: ${CORTEX_PORT:-5000}/' config/cortex_config.yaml
    fi
ENDSSH

echo "[7/7] Starting memory node on edge device..."
ssh ${TARGET_USER}@${TARGET_HOST} << 'ENDSSH'
    cd /home/${USER}/ai-robot-memory-system
    source venv/bin/activate

    export ROS_DOMAIN_ID=42

    nohup python3 -m ros2_integration.memory_node > memory_node.log 2>&1 &
    echo $! > memory_node.pid

    sleep 2

    if ps -p $(cat memory_node.pid) > /dev/null; then
        echo "Memory node started successfully (PID: $(cat memory_node.pid))"
    else
        echo "Failed to start memory node. Check memory_node.log"
        exit 1
    fi
ENDSSH

echo ""
echo "=== Deployment Complete ==="
echo "To connect to the edge device:"
echo "  ssh ${TARGET_USER}@${TARGET_HOST}"
echo ""
echo "To check memory node status:"
echo "  ssh ${TARGET_USER}@${TARGET_HOST} 'ps -p \$(cat ${TARGET_DIR}/memory_node.pid)'"
echo ""
echo "To view logs:"
echo "  ssh ${TARGET_USER}@${TARGET_HOST} 'tail -f ${TARGET_DIR}/memory_node.log'"
