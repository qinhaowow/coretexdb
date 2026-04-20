import time
import json
import statistics
from typing import Any, Dict, List, Optional
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class Benchmark:
    def __init__(
        self,
        cortex_client,
        embedding_service,
        memory_manager=None,
        test_collection: str = "benchmark_test"
    ):
        self.cortex_client = cortex_client
        self.embedding_service = embedding_service
        self.memory_manager = memory_manager
        self.test_collection = test_collection
        self.results: Dict[str, List[float]] = {}

    def run_all(self) -> Dict[str, Any]:
        print("=" * 60)
        print("AI Robot Memory System - Performance Benchmark")
        print("=" * 60)
        print()

        self.cortex_client.create_collection(
            name=self.test_collection,
            vector_size=self.embedding_service.get_dimension()
        )

        results = {}

        results["embedding_generation"] = self.benchmark_embedding_generation()
        results["vector_insertion"] = self.benchmark_vector_insertion()
        results["vector_search"] = self.benchmark_vector_search()
        results["hybrid_search"] = self.benchmark_hybrid_search()
        results["batch_operations"] = self.benchmark_batch_operations()

        if self.memory_manager:
            results["memory_management"] = self.benchmark_memory_management()

        self.print_results(results)

        self.cortex_client.delete_collection(self.test_collection)

        return results

    def benchmark_embedding_generation(self) -> Dict[str, Any]:
        print("[1/5] Benchmarking Embedding Generation...")
        test_texts = [f"sample text {i}" for i in range(100)]

        start_time = time.time()
        embeddings = self.embedding_service.batch_embed(test_texts)
        elapsed = time.time() - start_time

        return {
            "total_time": elapsed,
            "avg_time": elapsed / len(test_texts),
            "throughput": len(test_texts) / elapsed,
            "embedding_dim": len(embeddings[0]) if embeddings else 0
        }

    def benchmark_vector_insertion(self) -> Dict[str, Any]:
        print("[2/5] Benchmarking Vector Insertion...")

        num_vectors = 1000
        vectors = [[0.1] * self.embedding_service.get_dimension() for _ in range(num_vectors)]
        payloads = [{"text": f"doc_{i}"} for i in range(num_vectors)]
        ids = [f"insert_{i}" for i in range(num_vectors)]

        start_time = time.time()
        result = self.cortex_client.insert(
            collection=self.test_collection,
            vectors=vectors,
            payloads=payloads,
            ids=ids
        )
        elapsed = time.time() - start_time

        return {
            "total_time": elapsed,
            "avg_time": elapsed / num_vectors,
            "throughput": num_vectors / elapsed,
            "inserted_count": result.get("count", num_vectors)
        }

    def benchmark_vector_search(self) -> Dict[str, Any]:
        print("[3/5] Benchmarking Vector Search...")

        query_vector = [0.1] * self.embedding_service.get_dimension()
        num_queries = 100
        limit = 10

        times = []
        for _ in range(num_queries):
            start_time = time.time()
            results = self.cortex_client.search(
                collection=self.test_collection,
                query_vector=query_vector,
                limit=limit
            )
            elapsed = time.time() - start_time
            times.append(elapsed)

        return {
            "total_time": sum(times),
            "avg_time": statistics.mean(times),
            "median_time": statistics.median(times),
            "min_time": min(times),
            "max_time": max(times),
            "p95_time": sorted(times)[int(len(times) * 0.95)],
            "queries_per_second": num_queries / sum(times)
        }

    def benchmark_hybrid_search(self) -> Dict[str, Any]:
        print("[4/5] Benchmarking Hybrid Search...")

        query_vector = [0.1] * self.embedding_service.get_dimension()
        filters = {"category": "test"}

        num_queries = 50

        times = []
        for _ in range(num_queries):
            start_time = time.time()
            results = self.cortex_client.search(
                collection=self.test_collection,
                query_vector=query_vector,
                limit=10,
                filter_dict=filters
            )
            elapsed = time.time() - start_time
            times.append(elapsed)

        return {
            "total_time": sum(times),
            "avg_time": statistics.mean(times),
            "median_time": statistics.median(times),
            "queries_per_second": num_queries / sum(times)
        }

    def benchmark_batch_operations(self) -> Dict[str, Any]:
        print("[5/5] Benchmarking Batch Operations...")

        batch_sizes = [10, 50, 100, 500]
        results = {}

        for batch_size in batch_sizes:
            vectors = [[0.1] * self.embedding_service.get_dimension() for _ in range(batch_size)]
            payloads = [{"text": f"batch_doc_{i}"} for i in range(batch_size)]
            ids = [f"batch_{i}" for i in range(batch_size)]

            start_time = time.time()
            self.cortex_client.insert(
                collection=self.test_collection,
                vectors=vectors,
                payloads=payloads,
                ids=ids
            )
            elapsed = time.time() - start_time

            results[f"batch_{batch_size}"] = {
                "time": elapsed,
                "throughput": batch_size / elapsed
            }

        return results

    def benchmark_memory_management(self) -> Dict[str, Any]:
        print("[Extra] Benchmarking Memory Management...")

        num_memories = 100
        for i in range(num_memories):
            self.memory_manager.add_memory(
                content=f"memory content {i}",
                memory_type="episodic",
                importance=0.5
            )

        start_time = time.time()
        results = self.memory_manager.retrieve("memory", limit=10)
        retrieval_time = time.time() - start_time

        return {
            "total_memories": num_memories,
            "retrieval_time": retrieval_time,
            "retrieval_rate": len(results) / retrieval_time if retrieval_time > 0 else 0,
            "stats": self.memory_manager.get_stats()
        }

    def print_results(self, results: Dict[str, Any]):
        print()
        print("=" * 60)
        print("BENCHMARK RESULTS")
        print("=" * 60)

        for category, metrics in results.items():
            print(f"\n{category.upper().replace('_', ' ')}:")
            print("-" * 40)
            if isinstance(metrics, dict):
                for key, value in metrics.items():
                    if isinstance(value, float):
                        print(f"  {key}: {value:.4f}")
                    else:
                        print(f"  {key}: {value}")
            else:
                print(f"  {metrics}")

        print()
        print("=" * 60)

    def export_results(self, results: Dict[str, Any], output_file: str):
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results exported to {output_file}")


def main():
    from core.cortex_client import CortexClient
    from core.embedding_service import EmbeddingService
    from core.memory_manager import MemoryManager

    cortex_client = CortexClient(host="localhost", port=5000)
    cortex_client.connect()

    embedding_service = EmbeddingService()
    embedding_service.load_model()

    memory_manager = MemoryManager(
        cortex_client=cortex_client,
        embedding_service=embedding_service
    )

    benchmark = Benchmark(
        cortex_client=cortex_client,
        embedding_service=embedding_service,
        memory_manager=memory_manager
    )

    results = benchmark.run_all()

    benchmark.export_results(results, "benchmark_results.json")


if __name__ == "__main__":
    main()
