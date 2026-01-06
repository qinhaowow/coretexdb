"""
OpenAI integration for CortexDB
"""

from typing import List, Optional
import openai


class OpenAIEmbeddingAdapter:
    """
    Adapter for OpenAI API to generate embeddings compatible with CortexDB
    """

    def __init__(
        self,
        model_name: str = "text-embedding-ada-002",
        api_key: Optional[str] = None,
        organization: Optional[str] = None,
    ):
        """
        Initialize OpenAI embedding adapter

        Args:
            model_name: Name of the OpenAI embedding model to use
            api_key: OpenAI API key (if not set in environment)
            organization: OpenAI organization ID (if not set in environment)
        """
        self.model_name = model_name
        
        # Set API key if provided
        if api_key:
            openai.api_key = api_key
        
        # Set organization if provided
        if organization:
            openai.organization = organization

    def embed_query(self, text: str) -> List[float]:
        """
        Generate embedding for a single query

        Args:
            text: Query text

        Returns:
            Embedding vector as a list of floats
        """
        return self.embed_documents([text])[0]

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple documents

        Args:
            texts: List of document texts

        Returns:
            List of embedding vectors
        """
        # Call OpenAI API
        response = openai.embeddings.create(
            input=texts,
            model=self.model_name
        )

        # Extract embeddings
        embeddings = [item.embedding for item in response.data]

        return embeddings

    def get_dimension(self) -> int:
        """
        Get the dimension of the embeddings

        Returns:
            Embedding dimension
        """
        # Different models have different dimensions
        # This is a simple mapping for common models
        dimension_map = {
            "text-embedding-ada-002": 1536,
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
        }

        return dimension_map.get(self.model_name, 1536)