"""
HuggingFace integration for CortexDB
"""

from typing import List, Optional, Union
import numpy as np
from transformers import AutoTokenizer, AutoModel
import torch


class HuggingFaceEmbeddingAdapter:
    """
    Adapter for HuggingFace models to generate embeddings compatible with CortexDB
    """

    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        device: Optional[str] = None,
        normalize_embeddings: bool = True,
    ):
        """
        Initialize HuggingFace embedding adapter

        Args:
            model_name: Name of the HuggingFace model to use
            device: Device to run the model on (e.g., "cpu", "cuda")
            normalize_embeddings: Whether to normalize embeddings to unit length
        """
        self.model_name = model_name
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.normalize_embeddings = normalize_embeddings

        # Load tokenizer and model
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name).to(self.device)
        self.model.eval()

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
        # Tokenize texts
        inputs = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt"
        ).to(self.device)

        # Generate embeddings
        with torch.no_grad():
            outputs = self.model(**inputs)
            # Use [CLS] token embedding
            embeddings = outputs.last_hidden_state[:, 0, :].cpu().numpy()

        # Normalize embeddings if requested
        if self.normalize_embeddings:
            embeddings = self._normalize(embeddings)

        # Convert to list of lists
        return embeddings.tolist()

    def _normalize(self, embeddings: np.ndarray) -> np.ndarray:
        """
        Normalize embeddings to unit length

        Args:
            embeddings: Embeddings to normalize

        Returns:
            Normalized embeddings
        """
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        return embeddings / norms

    def get_dimension(self) -> int:
        """
        Get the dimension of the embeddings

        Returns:
            Embedding dimension
        """
        # Get hidden size from model configuration
        return self.model.config.hidden_size