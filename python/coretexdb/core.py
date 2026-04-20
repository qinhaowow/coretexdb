# Core Python bindings for CortexDB
# This module provides the main Python API for interacting with CortexDB

import numpy as np
from typing import List, Dict, Any, Optional

class CortexDB:
    """
    Main CortexDB class for Python

    Example:
    --------
    import cortexdb
    import numpy as np

    # Initialize database
    db = cortexdb.CortexDB("data")

    # Insert vectors
    vectors = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    db.insert("collection1", vectors)

    # Search for similar vectors
    query = np.array([1.1, 2.1, 3.1])
    results = db.search("collection1", query, k=10)
    print(results)
    """

    def __init__(self, data_dir: str = "data"):
        """
        Initialize a new CortexDB instance

        Args:
            data_dir: Path to the data directory for storage
        """
        self.data_dir = data_dir
        # TODO: Initialize Rust-based backend
        # self.backend = cortexdb_rs::CortexDB::new(data_dir)
    
    def insert(self, collection: str, vectors: np.ndarray) -> None:
        """
        Insert vectors into a collection

        Args:
            collection: Name of the collection
            vectors: 2D numpy array of vectors (shape: [n_vectors, dimension])
        
        Raises:
            ValueError: If vectors is not a 2D numpy array
        """
        if vectors.ndim != 2:
            raise ValueError("Vectors must be a 2D numpy array")
        
        # TODO: Convert numpy array to Rust format and insert
        # self.backend.insert(collection, vectors)
    
    def search(self, collection: str, query: np.ndarray, k: int = 10) -> List[Dict[str, Any]]:
        """
        Search for similar vectors

        Args:
            collection: Name of the collection
            query: 1D numpy array of query vector
            k: Number of results to return
        
        Returns:
            List of search results, each containing 'id', 'score', and 'metadata'
        
        Raises:
            ValueError: If query is not a 1D numpy array
        """
        if query.ndim != 1:
            raise ValueError("Query must be a 1D numpy array")
        
        # TODO: Convert numpy array to Rust format and search
        # results = self.backend.search(collection, query, k)
        # return [
        #     {"id": r.id, "score": r.score, "metadata": r.metadata}
        #     for r in results
        # ]
        
        # Placeholder implementation
        return [
            {"id": f"doc_{i}", "score": 1.0 / (i + 1), "metadata": None}
            for i in range(k)
        ]
    
    def create_collection(self, collection: str, dimension: int) -> None:
        """
        Create a new collection with specified dimension

        Args:
            collection: Name of the collection
            dimension: Dimension of vectors in the collection
        """
        # TODO: Implement collection creation
        pass
    
    def delete_collection(self, collection: str) -> None:
        """
        Delete a collection

        Args:
            collection: Name of the collection to delete
        """
        # TODO: Implement collection deletion
        pass
    
    def list_collections(self) -> List[str]:
        """
        List all collections

        Returns:
            List of collection names
        """
        # TODO: Implement collection listing
        return []
    
    def get_collection_info(self, collection: str) -> Dict[str, Any]:
        """
        Get information about a collection

        Args:
            collection: Name of the collection
        
        Returns:
            Dictionary with collection information
        """
        # TODO: Implement collection info retrieval
        return {
            "name": collection,
            "dimension": 0,
            "count": 0
        }