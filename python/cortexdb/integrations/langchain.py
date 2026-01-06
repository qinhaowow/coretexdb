"""
LangChain integration for CortexDB
"""

from typing import List, Dict, Any, Optional, Iterable, Tuple
import numpy as np
from cortexdb import CortexDBClient

# Lazy import for optional langchain dependency
try:
    from langchain.vectorstores.base import VectorStore
    from langchain.embeddings.base import Embeddings
    from langchain.schema import Document
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


class CortexDBVectorStore:
    """LangChain vector store implementation for CortexDB"""

    def __init__(
        self,
        client: CortexDBClient,
        collection_name: str,
        embedding_function: Any,  # Using Any since Embeddings might not be available
        dimension: Optional[int] = None,
    ):
        """
        Initialize CortexDB vector store for LangChain

        Args:
            client: CortexDB client instance
            collection_name: Name of the collection to use
            embedding_function: Embedding function to use
            dimension: Vector dimension (optional, will be inferred if not provided)

        Raises:
            ImportError: If langchain is not installed
        """
        if not LANGCHAIN_AVAILABLE:
            raise ImportError(
                "LangChain integration requires langchain to be installed. "
                "Please install it with: pip install 'cortexdb[langchain]'"
            )
        
        self.client = client
        self.collection_name = collection_name
        self.embedding_function = embedding_function
        self.dimension = dimension

        # Create collection if it doesn't exist
        try:
            collections = self.client.list_collections()
            if collection_name not in collections:
                if dimension is None:
                    # Infer dimension from a sample embedding
                    sample_embedding = self.embedding_function.embed_query("sample")
                    dimension = len(sample_embedding)
                self.client.create_collection(collection_name, dimension)
        except Exception as e:
            raise RuntimeError(f"Failed to initialize CortexDB vector store: {e}")

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[str]:
        """
        Add texts to the vector store

        Args:
            texts: Iterable of strings to add
            metadatas: Optional list of metadata dicts
            ids: Optional list of ids
            **kwargs: Additional keyword arguments

        Returns:
            List of ids for the added texts
        """
        texts = list(texts)
        if metadatas is None:
            metadatas = [{} for _ in texts]

        # Generate embeddings
        embeddings = self.embedding_function.embed_documents(texts)

        # Add metadata to each embedding
        for i, (text, metadata) in enumerate(zip(texts, metadatas)):
            metadata["text"] = text

        # Insert into CortexDB
        self.client.insert(self.collection_name, embeddings, metadatas)

        # Return ids (currently not supported, will be implemented later)
        return [f"doc_{i}" for i in range(len(texts))]

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        filter: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[Any]:  # Using Any since Document might not be available
        """
        Search for similar documents

        Args:
            query: Query string
            k: Number of results to return
            filter: Optional filter
            **kwargs: Additional keyword arguments

        Returns:
            List of Document objects
        """
        # Generate embedding for query
        query_embedding = self.embedding_function.embed_query(query)

        # Search in CortexDB
        results = self.client.search(self.collection_name, query_embedding, k=k, filter=filter)

        # Convert to LangChain Document objects
        documents = []
        for result in results:
            metadata = result.metadata.copy() if result.metadata else {}
            text = metadata.pop("text", "")
            document = Document(page_content=text, metadata=metadata)
            documents.append(document)

        return documents

    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        filter: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[Tuple[Any, float]]:  # Using Any since Document might not be available
        """
        Search for similar documents with scores

        Args:
            query: Query string
            k: Number of results to return
            filter: Optional filter
            **kwargs: Additional keyword arguments

        Returns:
            List of (Document, score) tuples
        """
        # Generate embedding for query
        query_embedding = self.embedding_function.embed_query(query)

        # Search in CortexDB
        results = self.client.search(self.collection_name, query_embedding, k=k, filter=filter)

        # Convert to (Document, score) tuples
        document_score_pairs = []
        for result in results:
            metadata = result.metadata.copy() if result.metadata else {}
            text = metadata.pop("text", "")
            document = Document(page_content=text, metadata=metadata)
            document_score_pairs.append((document, result.score))

        return document_score_pairs

    @classmethod
    def from_texts(
        cls,
        texts: List[str],
        embedding: Any,  # Using Any since Embeddings might not be available
        metadatas: Optional[List[Dict[str, Any]]] = None,
        collection_name: str = "langchain",
        client_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> "CortexDBVectorStore":
        """
        Create a CortexDBVectorStore from texts

        Args:
            texts: List of texts to add
            embedding: Embedding function to use
            metadatas: Optional list of metadata dicts
            collection_name: Name of the collection to use
            client_kwargs: Optional keyword arguments for the client
            **kwargs: Additional keyword arguments

        Returns:
            CortexDBVectorStore instance

        Raises:
            ImportError: If langchain is not installed
        """
        if not LANGCHAIN_AVAILABLE:
            raise ImportError(
                "LangChain integration requires langchain to be installed. "
                "Please install it with: pip install 'cortexdb[langchain]'"
            )
            
        if client_kwargs is None:
            client_kwargs = {}

        # Initialize client
        client = CortexDBClient(**client_kwargs)

        # Create and return vector store
        return cls(client, collection_name, embedding, **kwargs)