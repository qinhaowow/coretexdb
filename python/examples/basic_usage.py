"""
CortexDB Python Usage Examples

This module demonstrates how to use CortexDB with different connection methods.
"""

import numpy as np
from typing import List, Dict, Any

try:
    from coretexdb import CortexDBGrpcClient, AsyncCortexDBGrpcClient
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

try:
    from coretexdb import CortexDBClient, AsyncCortexDBClient
    HTTP_AVAILABLE = True
except ImportError:
    HTTP_AVAILABLE = False


def example_grpc_client():
    """Example: Using gRPC client to connect to CortexDB server"""
    if not GRPC_AVAILABLE:
        print("gRPC client not available. Install with: pip install coretexdb[grpc]")
        return

    print("=== CortexDB gRPC Client Example ===\n")

    with CortexDBGrpcClient(host="localhost", port=50051) as client:
        health = client.health_check()
        print(f"Server status: {health['status']}")
        print(f"Server version: {health['version']}\n")

        client.create_collection("test_collection", dimension=128, metric="cosine")
        print("Created collection: test_collection\n")

        vectors = [
            np.random.rand(128).astype(np.float32) for _ in range(10)
        ]
        metadata = [{"text": f"Document {i}"} for i in range(10)]

        result = client.insert_vectors("test_collection", vectors, metadata)
        print(f"Inserted {result['count']} vectors\n")

        query = np.random.rand(128).astype(np.float32)
        results = client.search("test_collection", query, k=3)

        print("Search results:")
        for r in results:
            print(f"  ID: {r['id']}, Score: {r['score']:.4f}")

        collections = client.list_collections()
        print(f"\nCollections: {collections}")


async def example_async_grpc_client():
    """Example: Using async gRPC client"""
    if not GRPC_AVAILABLE:
        print("gRPC client not available. Install with: pip install coretexdb[grpc]")
        return

    print("\n=== CortexDB Async gRPC Client Example ===\n")

    async with AsyncCortexDBGrpcClient(host="localhost", port=50051) as client:
        health = await client.health_check()
        print(f"Server status: {health['status']}")

        await client.create_collection("async_test", dimension=256)
        print("Created collection via async client")

        vectors = [np.random.rand(256).astype(np.float32) for _ in range(5)]
        result = await client.insert_vectors("async_test", vectors)
        print(f"Inserted {result['count']} vectors")


def example_http_client():
    """Example: Using HTTP REST client"""
    if not HTTP_AVAILABLE:
        print("HTTP client not available")
        return

    print("\n=== CortexDB HTTP Client Example ===\n")

    client = CortexDBClient(host="localhost", port=8000)

    client.create_collection("http_collection", dimension=128)
    print("Created collection via HTTP")

    vectors = np.random.rand(5, 128).astype(np.float32)
    result = client.insert("http_collection", vectors)
    print(f"Inserted vectors: {result}")

    query = np.random.rand(128).astype(np.float32)
    results = client.search("http_collection", query, k=3)
    print(f"Search results: {results}")


def example_local_mode():
    """Example: Using local in-memory mode (no server required)"""
    print("\n=== CortexDB Local Mode Example ===\n")
    print("Note: This requires the Rust backend to be compiled as a Python extension")
    print("For now, use the gRPC or HTTP client to connect to a running server\n")

    print("To start a CortexDB server:")
    print("  cargo run --release --bin coretex-server -- --host 0.0.0.0 --port 50051")
    print("\nThen connect using the gRPC or HTTP client examples above")


def run_all_examples():
    """Run all usage examples"""
    print("CortexDB Python SDK - Usage Examples")
    print("=" * 50)

    example_grpc_client()
    example_local_mode()

    import asyncio
    asyncio.run(example_async_grpc_client())


if __name__ == "__main__":
    run_all_examples()
