# CortexDB Python Package
# A multimodal vector database for AI applications

"""
CortexDB Python package
======================

A multimodal vector database for AI applications, providing:
- Vector storage and indexing
- Similarity search
- Query processing
- Python-native API

Example usage:
--------------
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

from .core import CortexDB
from .client import CortexDBClient, AsyncCortexDBClient
from .version import __version__
from . import integrations
from . import protocol

__all__ = [
    "CortexDB", 
    "CortexDBClient", 
    "AsyncCortexDBClient",
    "integrations",
    "protocol",
    "__version__"
]