# CortexDB Python Examples

This directory contains examples for using CortexDB with Python.

## Quick Start

### Option 1: Connect to Running Server

```python
from coretexdb import CortexDBGrpcClient

# Connect to a running CortexDB server
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

First, start the Rust server:

```bash
# Build the server
cargo build --release

# Run the server
cargo run --release --bin coretex-server -- --host 0.0.0.0 --port 50051
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
export CORTEXDB_HOST=localhost
export CORTEXDB_PORT=50051
export CORTEXDB_API_KEY=your_api_key
export CORTEXDB_TIMEOUT=30.0
```
