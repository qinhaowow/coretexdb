"""
CortexDB Python Client
"""

import os
from typing import List, Dict, Any, Optional, Union
import numpy as np
import requests
import aiohttp
from pydantic import BaseModel, Field
from cortexdb.protocol import (
    CollectionConfig,
    VectorInsert,
    SearchQuery,
    SearchResult,
    BatchSearchQuery,
    BatchSearchResult,
    DeleteRequest,
    UpdateRequest,
    CollectionStats,
    HealthCheckResponse,
    ErrorResponse,
    MetricType,
)


class CortexDBClient:
    """
    Synchronous client for CortexDB
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ):
        """
        Initialize CortexDB client

        Args:
            host: Hostname of the CortexDB server
            port: Port of the CortexDB server
            api_key: API key for authentication
            timeout: Request timeout in seconds
        """
        # Use environment variables if not provided
        self.host = host or os.environ.get("CORTEXDB_HOST", "localhost")
        self.port = port or int(os.environ.get("CORTEXDB_PORT", "8080"))
        self.api_key = api_key or os.environ.get("CORTEXDB_API_KEY")
        self.timeout = timeout or float(os.environ.get("CORTEXDB_TIMEOUT", "30.0"))

        # Base URL
        self.base_url = f"http://{self.host}:{self.port}"

        # Headers
        self.headers = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            self.headers["Authorization"] = f"Bearer {self.api_key}"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make a request to the CortexDB API

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request data

        Returns:
            Response data

        Raises:
            Exception: If the request fails
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=self.timeout,
                )
            elif method == "POST":
                response = requests.post(
                    url,
                    headers=self.headers,
                    json=data,
                    timeout=self.timeout,
                )
            elif method == "PUT":
                response = requests.put(
                    url,
                    headers=self.headers,
                    json=data,
                    timeout=self.timeout,
                )
            elif method == "DELETE":
                response = requests.delete(
                    url,
                    headers=self.headers,
                    json=data,
                    timeout=self.timeout,
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Check response status
            response.raise_for_status()

            # Return response data
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"API request failed: {e}")

    def create_collection(
        self,
        name: str,
        dimension: int,
        metric: Union[str, MetricType] = MetricType.COSINE,
        index_type: str = "hnsw",
        index_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new collection

        Args:
            name: Collection name
            dimension: Vector dimension
            metric: Similarity metric
            index_type: Index type
            index_params: Index parameters

        Returns:
            Response data
        """
        config = CollectionConfig(
            name=name,
            dimension=dimension,
            metric=MetricType(metric) if isinstance(metric, str) else metric,
            index_type=index_type,
            index_params=index_params,
        )

        return self._make_request(
            "POST",
            "/api/collections",
            config.model_dump(),
        )

    def list_collections(self) -> List[str]:
        """
        List all collections

        Returns:
            List of collection names
        """
        response = self._make_request("GET", "/api/collections")
        return response.get("collections", [])

    def delete_collection(self, name: str) -> Dict[str, Any]:
        """
        Delete a collection

        Args:
            name: Collection name

        Returns:
            Response data
        """
        return self._make_request("DELETE", f"/api/collections/{name}")

    def get_collection_stats(self, name: str) -> CollectionStats:
        """
        Get collection statistics

        Args:
            name: Collection name

        Returns:
            Collection statistics
        """
        response = self._make_request("GET", f"/api/collections/{name}/stats")
        return CollectionStats(**response)

    def insert(
        self,
        collection: str,
        vectors: Union[List[List[float]], np.ndarray],
        metadata: Optional[List[Dict[str, Any]]] = None,
        ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Insert vectors into a collection

        Args:
            collection: Collection name
            vectors: List of vectors or numpy array
            metadata: Optional list of metadata
            ids: Optional list of vector IDs

        Returns:
            Response data
        """
        # Convert numpy array to list if needed
        if isinstance(vectors, np.ndarray):
            vectors = vectors.tolist()

        insert_data = VectorInsert(
            vectors=vectors,
            ids=ids,
            metadata=metadata,
        )

        return self._make_request(
            "POST",
            f"/api/collections/{collection}/vectors",
            insert_data.model_dump(),
        )

    def search(
        self,
        collection: str,
        query: Union[List[float], np.ndarray],
        k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
        include_metadata: bool = True,
    ) -> List[SearchResult]:
        """
        Search for similar vectors

        Args:
            collection: Collection name
            query: Query vector
            k: Number of results to return
            filter: Optional filter
            include_metadata: Include metadata in results

        Returns:
            List of search results
        """
        # Convert numpy array to list if needed
        if isinstance(query, np.ndarray):
            query = query.tolist()

        search_data = SearchQuery(
            query=query,
            k=k,
            filter=filter,
            include_metadata=include_metadata,
        )

        response = self._make_request(
            "POST",
            f"/api/collections/{collection}/search",
            search_data.model_dump(),
        )

        return [SearchResult(**result) for result in response.get("results", [])]

    def batch_search(
        self,
        collection: str,
        queries: Union[List[List[float]], np.ndarray],
        k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
        include_metadata: bool = True,
    ) -> BatchSearchResult:
        """
        Search with multiple queries

        Args:
            collection: Collection name
            queries: List of query vectors
            k: Number of results per query
            filter: Optional filter
            include_metadata: Include metadata in results

        Returns:
            Batch search result
        """
        # Convert numpy array to list if needed
        if isinstance(queries, np.ndarray):
            queries = queries.tolist()

        batch_search_data = BatchSearchQuery(
            queries=queries,
            k=k,
            filter=filter,
            include_metadata=include_metadata,
        )

        response = self._make_request(
            "POST",
            f"/api/collections/{collection}/batch-search",
            batch_search_data.model_dump(),
        )

        return BatchSearchResult(**response)

    def delete(self, collection: str, ids: List[str]) -> Dict[str, Any]:
        """
        Delete vectors by ID

        Args:
            collection: Collection name
            ids: List of vector IDs

        Returns:
            Response data
        """
        delete_data = DeleteRequest(ids=ids)

        return self._make_request(
            "DELETE",
            f"/api/collections/{collection}/vectors",
            delete_data.model_dump(),
        )

    def update(
        self,
        collection: str,
        ids: List[str],
        vectors: Optional[Union[List[List[float]], np.ndarray]] = None,
        metadata: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Update vectors and/or metadata

        Args:
            collection: Collection name
            ids: List of vector IDs
            vectors: Optional list of new vectors
            metadata: Optional list of new metadata

        Returns:
            Response data
        """
        # Convert numpy array to list if needed
        if isinstance(vectors, np.ndarray):
            vectors = vectors.tolist()

        update_data = UpdateRequest(
            ids=ids,
            vectors=vectors,
            metadata=metadata,
        )

        return self._make_request(
            "PUT",
            f"/api/collections/{collection}/vectors",
            update_data.model_dump(),
        )

    def health_check(self) -> HealthCheckResponse:
        """
        Check server health

        Returns:
            Health check response
        """
        response = self._make_request("GET", "/api/health")
        return HealthCheckResponse(**response)


class AsyncCortexDBClient:
    """
    Asynchronous client for CortexDB
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ):
        """
        Initialize asynchronous CortexDB client

        Args:
            host: Hostname of the CortexDB server
            port: Port of the CortexDB server
            api_key: API key for authentication
            timeout: Request timeout in seconds
        """
        # Use environment variables if not provided
        self.host = host or os.environ.get("CORTEXDB_HOST", "localhost")
        self.port = port or int(os.environ.get("CORTEXDB_PORT", "8080"))
        self.api_key = api_key or os.environ.get("CORTEXDB_API_KEY")
        self.timeout = timeout or float(os.environ.get("CORTEXDB_TIMEOUT", "30.0"))

        # Base URL
        self.base_url = f"http://{self.host}:{self.port}"

        # Headers
        self.headers = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            self.headers["Authorization"] = f"Bearer {self.api_key}"

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make an asynchronous request to the CortexDB API

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request data

        Returns:
            Response data

        Raises:
            Exception: If the request fails
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                if method == "GET":
                    async with session.get(
                        url,
                        headers=self.headers,
                    ) as response:
                        response.raise_for_status()
                        return await response.json()
                elif method == "POST":
                    async with session.post(
                        url,
                        headers=self.headers,
                        json=data,
                    ) as response:
                        response.raise_for_status()
                        return await response.json()
                elif method == "PUT":
                    async with session.put(
                        url,
                        headers=self.headers,
                        json=data,
                    ) as response:
                        response.raise_for_status()
                        return await response.json()
                elif method == "DELETE":
                    async with session.delete(
                        url,
                        headers=self.headers,
                        json=data,
                    ) as response:
                        response.raise_for_status()
                        return await response.json()
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
        except aiohttp.ClientError as e:
            raise Exception(f"API request failed: {e}")

    async def create_collection(
        self,
        name: str,
        dimension: int,
        metric: Union[str, MetricType] = MetricType.COSINE,
        index_type: str = "hnsw",
        index_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new collection

        Args:
            name: Collection name
            dimension: Vector dimension
            metric: Similarity metric
            index_type: Index type
            index_params: Index parameters

        Returns:
            Response data
        """
        config = CollectionConfig(
            name=name,
            dimension=dimension,
            metric=MetricType(metric) if isinstance(metric, str) else metric,
            index_type=index_type,
            index_params=index_params,
        )

        return await self._make_request(
            "POST",
            "/api/collections",
            config.model_dump(),
        )

    async def list_collections(self) -> List[str]:
        """
        List all collections

        Returns:
            List of collection names
        """
        response = await self._make_request("GET", "/api/collections")
        return response.get("collections", [])

    async def delete_collection(self, name: str) -> Dict[str, Any]:
        """
        Delete a collection

        Args:
            name: Collection name

        Returns:
            Response data
        """
        return await self._make_request("DELETE", f"/api/collections/{name}")

    async def get_collection_stats(self, name: str) -> CollectionStats:
        """
        Get collection statistics

        Args:
            name: Collection name

        Returns:
            Collection statistics
        """
        response = await self._make_request("GET", f"/api/collections/{name}/stats")
        return CollectionStats(**response)

    async def insert(
        self,
        collection: str,
        vectors: Union[List[List[float]], np.ndarray],
        metadata: Optional[List[Dict[str, Any]]] = None,
        ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Insert vectors into a collection

        Args:
            collection: Collection name
            vectors: List of vectors or numpy array
            metadata: Optional list of metadata
            ids: Optional list of vector IDs

        Returns:
            Response data
        """
        # Convert numpy array to list if needed
        if isinstance(vectors, np.ndarray):
            vectors = vectors.tolist()

        insert_data = VectorInsert(
            vectors=vectors,
            ids=ids,
            metadata=metadata,
        )

        return await self._make_request(
            "POST",
            f"/api/collections/{collection}/vectors",
            insert_data.model_dump(),
        )

    async def search(
        self,
        collection: str,
        query: Union[List[float], np.ndarray],
        k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
        include_metadata: bool = True,
    ) -> List[SearchResult]:
        """
        Search for similar vectors

        Args:
            collection: Collection name
            query: Query vector
            k: Number of results to return
            filter: Optional filter
            include_metadata: Include metadata in results

        Returns:
            List of search results
        """
        # Convert numpy array to list if needed
        if isinstance(query, np.ndarray):
            query = query.tolist()

        search_data = SearchQuery(
            query=query,
            k=k,
            filter=filter,
            include_metadata=include_metadata,
        )

        response = await self._make_request(
            "POST",
            f"/api/collections/{collection}/search",
            search_data.model_dump(),
        )

        return [SearchResult(**result) for result in response.get("results", [])]

    async def batch_search(
        self,
        collection: str,
        queries: Union[List[List[float]], np.ndarray],
        k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
        include_metadata: bool = True,
    ) -> BatchSearchResult:
        """
        Search with multiple queries

        Args:
            collection: Collection name
            queries: List of query vectors
            k: Number of results per query
            filter: Optional filter
            include_metadata: Include metadata in results

        Returns:
            Batch search result
        """
        # Convert numpy array to list if needed
        if isinstance(queries, np.ndarray):
            queries = queries.tolist()

        batch_search_data = BatchSearchQuery(
            queries=queries,
            k=k,
            filter=filter,
            include_metadata=include_metadata,
        )

        response = await self._make_request(
            "POST",
            f"/api/collections/{collection}/batch-search",
            batch_search_data.model_dump(),
        )

        return BatchSearchResult(**response)

    async def delete(self, collection: str, ids: List[str]) -> Dict[str, Any]:
        """
        Delete vectors by ID

        Args:
            collection: Collection name
            ids: List of vector IDs

        Returns:
            Response data
        """
        delete_data = DeleteRequest(ids=ids)

        return await self._make_request(
            "DELETE",
            f"/api/collections/{collection}/vectors",
            delete_data.model_dump(),
        )

    async def update(
        self,
        collection: str,
        ids: List[str],
        vectors: Optional[Union[List[List[float]], np.ndarray]] = None,
        metadata: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Update vectors and/or metadata

        Args:
            collection: Collection name
            ids: List of vector IDs
            vectors: Optional list of new vectors
            metadata: Optional list of new metadata

        Returns:
            Response data
        """
        # Convert numpy array to list if needed
        if isinstance(vectors, np.ndarray):
            vectors = vectors.tolist()

        update_data = UpdateRequest(
            ids=ids,
            vectors=vectors,
            metadata=metadata,
        )

        return await self._make_request(
            "PUT",
            f"/api/collections/{collection}/vectors",
            update_data.model_dump(),
        )

    async def health_check(self) -> HealthCheckResponse:
        """
        Check server health

        Returns:
            Health check response
        """
        response = await self._make_request("GET", "/api/health")
        return HealthCheckResponse(**response)