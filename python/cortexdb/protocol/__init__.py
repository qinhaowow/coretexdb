"""
Protocol definitions for CortexDB Python SDK
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from enum import Enum


class MetricType(str, Enum):
    """Vector similarity metric types"""
    COSINE = "cosine"
    EUCLIDEAN = "euclidean"
    DOT_PRODUCT = "dot_product"


class CollectionConfig(BaseModel):
    """Collection configuration"""
    name: str = Field(..., description="Collection name")
    dimension: int = Field(..., description="Vector dimension")
    metric: MetricType = Field(default=MetricType.COSINE, description="Similarity metric")
    index_type: str = Field(default="hnsw", description="Index type")
    index_params: Optional[Dict[str, Any]] = Field(default=None, description="Index parameters")


class VectorInsert(BaseModel):
    """Vector insertion request"""
    vectors: List[List[float]] = Field(..., description="List of vectors")
    ids: Optional[List[str]] = Field(default=None, description="Optional list of vector IDs")
    metadata: Optional[List[Dict[str, Any]]] = Field(default=None, description="Optional list of metadata")


class SearchQuery(BaseModel):
    """Search query request"""
    query: List[float] = Field(..., description="Query vector")
    k: int = Field(default=10, description="Number of results to return")
    filter: Optional[Dict[str, Any]] = Field(default=None, description="Optional filter")
    include_metadata: bool = Field(default=True, description="Include metadata in results")


class SearchResult(BaseModel):
    """Search result item"""
    id: str = Field(..., description="Vector ID")
    score: float = Field(..., description="Similarity score")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Vector metadata")


class BatchSearchQuery(BaseModel):
    """Batch search request"""
    queries: List[List[float]] = Field(..., description="List of query vectors")
    k: int = Field(default=10, description="Number of results per query")
    filter: Optional[Dict[str, Any]] = Field(default=None, description="Optional filter")
    include_metadata: bool = Field(default=True, description="Include metadata in results")


class BatchSearchResult(BaseModel):
    """Batch search result"""
    results: List[List[SearchResult]] = Field(..., description="List of search results per query")


class DeleteRequest(BaseModel):
    """Delete request"""
    ids: List[str] = Field(..., description="List of vector IDs to delete")


class UpdateRequest(BaseModel):
    """Update request"""
    ids: List[str] = Field(..., description="List of vector IDs to update")
    vectors: Optional[List[List[float]]] = Field(default=None, description="Optional list of new vectors")
    metadata: Optional[List[Dict[str, Any]]] = Field(default=None, description="Optional list of new metadata")


class CollectionStats(BaseModel):
    """Collection statistics"""
    name: str = Field(..., description="Collection name")
    vector_count: int = Field(..., description="Number of vectors")
    dimension: int = Field(..., description="Vector dimension")
    metric: MetricType = Field(..., description="Similarity metric")
    index_type: str = Field(..., description="Index type")
    index_size: Optional[int] = Field(default=None, description="Index size in bytes")


class ErrorResponse(BaseModel):
    """Error response"""
    error: str = Field(..., description="Error message")
    code: int = Field(..., description="Error code")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Error details")


class HealthCheckResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Server status")
    version: str = Field(..., description="Server version")
    uptime: float = Field(..., description="Server uptime in seconds")