"""
CortexDB gRPC Python Client
"""

import os
import grpc
from typing import List, Dict, Any, Optional, Union
import numpy as np

try:
    import coretexdb_pb2
    import coretexdb_pb2_grpc
except ImportError:
    coretexdb_pb2 = None
    coretexdb_pb2_grpc = None


class CortexDBGrpcClient:
    """
    gRPC client for CortexDB
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        timeout: float = 30.0,
    ):
        """
        Initialize CortexDB gRPC client

        Args:
            host: Hostname of the CortexDB server
            port: Port of the CortexDB gRPC server
            timeout: Request timeout in seconds
        """
        if coretexdb_pb2 is None:
            raise ImportError(
                "gRPC protobuf modules not found. "
                "Please install coretexdb with gRPC support: pip install coretexdb[grpc]"
            )

        self.host = host
        self.port = port
        self.timeout = timeout
        self.channel = None
        self.stub = None

    def connect(self):
        """Establish connection to the gRPC server"""
        address = f"{self.host}:{self.port}"
        self.channel = grpc.insecure_channel(address)
        self.stub = coretexdb_pb2_grpc.CoretexServiceStub(self.channel)

    def close(self):
        """Close the connection"""
        if self.channel:
            self.channel.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _to_vector_data(self, vectors: List[np.ndarray], metadata: Optional[List[Dict]] = None) -> List[coretexdb_pb2.VectorData]:
        """Convert numpy arrays to VectorData protobuf messages"""
        result = []
        for i, vec in enumerate(vectors):
            vec_list = vec.tolist() if isinstance(vec, np.ndarray) else list(vec)
            meta = metadata[i] if metadata and i < len(metadata) else {}
            result.append(coretexdb_pb2.VectorData(
                id=f"vec_{i}",
                vector=vec_list,
                metadata=meta
            ))
        return result

    def health_check(self) -> Dict[str, str]:
        """Check if the server is healthy"""
        request = coretexdb_pb2.Empty()
        response = self.stub.HealthCheck(request, timeout=self.timeout)
        return {
            "status": response.status,
            "version": response.version
        }

    def create_collection(
        self,
        name: str,
        dimension: int,
        metric: str = "cosine"
    ) -> Dict[str, Any]:
        """
        Create a new collection

        Args:
            name: Collection name
            dimension: Vector dimension
            metric: Distance metric (cosine, euclidean, dot)

        Returns:
            Dict with success status and message
        """
        request = coretexdb_pb2.CreateCollectionRequest(
            name=name,
            dimension=dimension,
            metric=metric
        )
        response = self.stub.CreateCollection(request, timeout=self.timeout)
        return {
            "success": response.success,
            "message": response.message
        }

    def delete_collection(self, name: str) -> Dict[str, Any]:
        """
        Delete a collection

        Args:
            name: Collection name

        Returns:
            Dict with success status and message
        """
        request = coretexdb_pb2.DeleteCollectionRequest(name=name)
        response = self.stub.DeleteCollection(request, timeout=self.timeout)
        return {
            "success": response.success,
            "message": response.message
        }

    def list_collections(self) -> List[str]:
        """
        List all collections

        Returns:
            List of collection names
        """
        request = coretexdb_pb2.Empty()
        response = self.stub.ListCollections(request, timeout=self.timeout)
        return list(response.collections)

    def insert_vectors(
        self,
        collection: str,
        vectors: Union[List[np.ndarray], np.ndarray],
        metadata: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        """
        Insert vectors into a collection

        Args:
            collection: Collection name
            vectors: List of vectors or 2D numpy array
            metadata: Optional list of metadata dicts

        Returns:
            Dict with inserted IDs and count
        """
        if isinstance(vectors, np.ndarray):
            vectors = [vectors[i] for i in range(len(vectors))]

        vector_data = self._to_vector_data(vectors, metadata)

        request = coretexdb_pb2.InsertVectorsRequest(
            collection=collection,
            vectors=vector_data
        )
        response = self.stub.InsertVectors(request, timeout=self.timeout)
        return {
            "ids": list(response.ids),
            "count": response.count
        }

    def search(
        self,
        collection: str,
        query_vector: Union[np.ndarray, List[float]],
        k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search for similar vectors

        Args:
            collection: Collection name
            query_vector: Query vector
            k: Number of results to return

        Returns:
            List of search results with id, score, and distance
        """
        query_list = query_vector.tolist() if isinstance(query_vector, np.ndarray) else query_vector

        request = coretexdb_pb2.SearchRequest(
            collection=collection,
            query_vector=query_list,
            k=k
        )
        response = self.stub.SearchVectors(request, timeout=self.timeout)

        return [
            {
                "id": result.id,
                "score": result.score,
                "distance": result.distance
            }
            for result in response.results
        ]

    def get_vector(
        self,
        collection: str,
        vector_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get a vector by ID

        Args:
            collection: Collection name
            vector_id: Vector ID

        Returns:
            Dict with vector data or None if not found
        """
        request = coretexdb_pb2.GetVectorRequest(
            collection=collection,
            id=vector_id
        )
        try:
            response = self.stub.GetVector(request, timeout=self.timeout)
            return {
                "id": response.id,
                "vector": response.vector,
                "metadata": response.metadata
            }
        except grpc.RpcError:
            return None

    def delete_vectors(
        self,
        collection: str,
        vector_ids: List[str]
    ) -> int:
        """
        Delete vectors by IDs

        Args:
            collection: Collection name
            vector_ids: List of vector IDs to delete

        Returns:
            Number of deleted vectors
        """
        request = coretexdb_pb2.DeleteVectorsRequest(
            collection=collection,
            ids=vector_ids
        )
        response = self.stub.DeleteVectors(request, timeout=self.timeout)
        return response.deleted_count

    def get_collection_info(self, name: str) -> Dict[str, Any]:
        """
        Get collection information

        Args:
            name: Collection name

        Returns:
            Dict with collection info
        """
        request = coretexdb_pb2.GetCollectionInfoRequest(name=name)
        response = self.stub.GetCollectionInfo(request, timeout=self.timeout)
        return {
            "name": response.name,
            "dimension": response.dimension,
            "metric": response.metric,
            "vector_count": response.vector_count
        }


class AsyncCortexDBGrpcClient:
    """
    Async gRPC client for CortexDB
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        timeout: float = 30.0,
    ):
        if coretexdb_pb2 is None:
            raise ImportError(
                "gRPC protobuf modules not found. "
                "Please install coretexdb with gRPC support: pip install coretexdb[grpc]"
            )

        self.host = host
        self.port = port
        self.timeout = timeout
        self.channel = None
        self.stub = None

    async def connect(self):
        """Establish connection to the gRPC server"""
        address = f"{self.host}:{self.port}"
        self.channel = grpc.aio.insecure_channel(address)
        self.stub = coretexdb_pb2_grpc.CoretexServiceStub(self.channel)

    async def close(self):
        """Close the connection"""
        if self.channel:
            await self.channel.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def health_check(self) -> Dict[str, str]:
        request = coretexdb_pb2.Empty()
        response = await self.stub.HealthCheck(request, timeout=self.timeout)
        return {"status": response.status, "version": response.version}

    async def create_collection(self, name: str, dimension: int, metric: str = "cosine") -> Dict[str, Any]:
        request = coretexdb_pb2.CreateCollectionRequest(name=name, dimension=dimension, metric=metric)
        response = await self.stub.CreateCollection(request, timeout=self.timeout)
        return {"success": response.success, "message": response.message}

    async def delete_collection(self, name: str) -> Dict[str, Any]:
        request = coretexdb_pb2.DeleteCollectionRequest(name=name)
        response = await self.stub.DeleteCollection(request, timeout=self.timeout)
        return {"success": response.success, "message": response.message}

    async def list_collections(self) -> List[str]:
        request = coretexdb_pb2.Empty()
        response = await self.stub.ListCollections(request, timeout=self.timeout)
        return list(response.collections)

    async def insert_vectors(
        self,
        collection: str,
        vectors: Union[List[np.ndarray], np.ndarray],
        metadata: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        if isinstance(vectors, np.ndarray):
            vectors = [vectors[i] for i in range(len(vectors))]

        vector_data = []
        for i, vec in enumerate(vectors):
            vec_list = vec.tolist() if isinstance(vec, np.ndarray) else list(vec)
            meta = metadata[i] if metadata and i < len(metadata) else {}
            vector_data.append(coretexdb_pb2.VectorData(id=f"vec_{i}", vector=vec_list, metadata=meta))

        request = coretexdb_pb2.InsertVectorsRequest(collection=collection, vectors=vector_data)
        response = await self.stub.InsertVectors(request, timeout=self.timeout)
        return {"ids": list(response.ids), "count": response.count}

    async def search(
        self,
        collection: str,
        query_vector: Union[np.ndarray, List[float]],
        k: int = 10
    ) -> List[Dict[str, Any]]:
        query_list = query_vector.tolist() if isinstance(query_vector, np.ndarray) else query_vector
        request = coretexdb_pb2.SearchRequest(collection=collection, query_vector=query_list, k=k)
        response = await self.stub.SearchVectors(request, timeout=self.timeout)
        return [{"id": r.id, "score": r.score, "distance": r.distance} for r in response.results]
