"""
Core Python bindings for CortexDB
This module provides the main Python API for interacting with CortexDB
via the REST API server.
"""

import json
import os
from typing import Any, Dict, List, Optional

import numpy as np

try:
    import requests
except ImportError:
    requests = None


class CortexDB:
    """
    Main CortexDB class for Python.

    This class communicates with a running CortexDB server via its REST API.
    For direct Rust-backed access, use the PyO3 bindings (install with
    ``pip install coretexdb[python]``).

    Example:
    --------
    import coretexdb
    import numpy as np

    db = coretexdb.CortexDB("localhost", port=5000)

    db.create_collection("my_vectors", dimension=128)

    vectors = np.random.randn(100, 128).astype(np.float32)
    db.insert("my_vectors", vectors)

    query = np.random.randn(128).astype(np.float32)
    results = db.search("my_vectors", query, k=10)
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5000,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ):
        """
        Initialize a new CortexDB instance.

        Args:
            host: Hostname of the CortexDB server.
            port: Port of the CortexDB server.
            api_key: Optional API key for authentication.
            timeout: HTTP request timeout in seconds.
        """
        if requests is None:
            raise ImportError(
                "The 'requests' library is required. Install it with: pip install requests"
            )

        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self.timeout = timeout

        self.headers: Dict[str, str] = {"Content-Type": "application/json"}
        resolved_key = api_key or os.environ.get("CORTEXDB_API_KEY")
        if resolved_key:
            self.headers["Authorization"] = f"Bearer {resolved_key}"

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make an HTTP request to the CortexDB server.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            endpoint: API endpoint path.
            data: Optional JSON body.

        Returns:
            Parsed JSON response.

        Raises:
            ConnectionError: If the server is unreachable.
            RuntimeError: If the server returns a non-2xx status.
        """
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.request(
                method,
                url,
                headers=self.headers,
                json=data,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.ConnectionError:
            raise ConnectionError(
                f"Cannot connect to CortexDB server at {self.base_url}. "
                "Make sure the server is running."
            )
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"HTTP {response.status_code}: {response.text}"
            ) from exc

    def insert(
        self,
        collection: str,
        vectors: np.ndarray,
        metadata: Optional[List[Dict[str, Any]]] = None,
        ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Insert vectors into a collection.

        Args:
            collection: Name of the collection.
            vectors: 2D numpy array of vectors (shape: [n_vectors, dimension]).
            metadata: Optional list of metadata dicts, one per vector.
            ids: Optional list of vector IDs.

        Returns:
            Server response containing inserted IDs and count.

        Raises:
            ValueError: If vectors is not a 2D numpy array.
        """
        if not isinstance(vectors, np.ndarray):
            raise ValueError("vectors must be a numpy ndarray")
        if vectors.ndim != 2:
            raise ValueError(
                f"vectors must be a 2D array, got {vectors.ndim}D"
            )

        payload: Dict[str, Any] = {
            "vectors": vectors.tolist(),
        }
        if metadata is not None:
            payload["metadata"] = metadata
        if ids is not None:
            payload["ids"] = ids

        return self._request(
            "POST",
            f"/api/collections/{collection}/vectors",
            payload,
        )

    def search(
        self,
        collection: str,
        query: np.ndarray,
        k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for similar vectors in a collection.

        Args:
            collection: Name of the collection.
            query: 1D numpy array query vector.
            k: Number of results to return.
            filter: Optional metadata filter.

        Returns:
            List of result dicts with 'id', 'score', and 'metadata' keys.

        Raises:
            ValueError: If query is not a 1D numpy array.
        """
        if not isinstance(query, np.ndarray):
            raise ValueError("query must be a numpy ndarray")
        if query.ndim != 1:
            raise ValueError(
                f"query must be a 1D array, got {query.ndim}D"
            )

        payload: Dict[str, Any] = {
            "query": query.tolist(),
            "k": k,
            "include_metadata": True,
        }
        if filter is not None:
            payload["filter"] = filter

        response = self._request(
            "POST",
            f"/api/collections/{collection}/search",
            payload,
        )
        return response.get("results", [])

    def create_collection(
        self,
        collection: str,
        dimension: int,
        metric: str = "cosine",
        index_type: str = "hnsw",
    ) -> Dict[str, Any]:
        """
        Create a new collection.

        Args:
            collection: Name of the collection.
            dimension: Dimension of vectors in the collection.
            metric: Similarity metric (cosine, euclidean, dot, l2).
            index_type: Index type (hnsw, ivf, brute_force, scalar).

        Returns:
            Server response.
        """
        return self._request(
            "POST",
            "/api/collections",
            {
                "name": collection,
                "dimension": dimension,
                "metric": metric,
                "index_type": index_type,
            },
        )

    def delete_collection(self, collection: str) -> Dict[str, Any]:
        """
        Delete a collection.

        Args:
            collection: Name of the collection to delete.

        Returns:
            Server response.
        """
        return self._request("DELETE", f"/api/collections/{collection}")

    def list_collections(self) -> List[str]:
        """
        List all collections.

        Returns:
            List of collection names.
        """
        response = self._request("GET", "/api/collections")
        return response.get("collections", [])

    def get_collection_info(self, collection: str) -> Dict[str, Any]:
        """
        Get information about a collection.

        Args:
            collection: Name of the collection.

        Returns:
            Dict with 'name', 'dimension', 'metric', 'vector_count' keys.
        """
        return self._request("GET", f"/api/collections/{collection}/stats")
