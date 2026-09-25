# CoreTexDB Python Examples

This directory contains examples for using CoreTexDB with Python.

## Quick Start

### Option 1: Connect to Running Server

```python
from coretexdb import CortexDBGrpcClient

# Connect to a running CoreTexDB server
with CortexDBGrpcClient(host="localhost", port=50051) as client:
    # Check server health
    print(client.health_check())
    
    # Create a collection
    client.create_collection("my_collection", dimension=128, metric="cosine")
    
    # Insert vectors
    import numpy as np
    vectors = [np.random.rand(128).astype(np.float32) for _ in range(10)]
    client.insert_vectors("my_collection", vectors)
    
    # Search
    query = np.random.rand(128).astype(np.float32)
    results = client.search("my_collection", query, k=3)
    print(results)
```

### Option 2: Start Server and Connect

First, start the Rust server (从 V0.2.4 起是**单二进制**，只有 `coretex` 一个可执行文件)：

```bash
# Build the server
cargo build --release

# Run the server: REST 默认 5000，gRPC 默认 50051
./target/release/coretex server

# 或指定端口：
./target/release/coretex server -p 8080 --grpc-port 50052
```

Then connect using the Python client (see Option 1).

## Available Clients

| Client | Description |
|--------|-------------|
| `CortexDBGrpcClient` | Synchronous gRPC client |
| `AsyncCortexDBGrpcClient` | Asynchronous gRPC client |
| `CortexDBClient` | Synchronous HTTP/REST client |
| `AsyncCortexDBClient` | Async HTTP/REST client |

## Environment Variables

You can configure the client using environment variables:

```bash
# REST 客户端默认端口 5000，gRPC 客户端默认端口 50051
export CORTEXDB_HOST=localhost
export CORTEXDB_PORT=5000
export CORTEXDB_API_KEY=your_api_key
export CORTEXDB_TIMEOUT=30.0
```
