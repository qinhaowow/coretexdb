# CoreTexDB Python Package
# A multimodal vector database for AI applications

"""
CoreTexDB Python package
======================

A multimodal vector database for AI applications, providing:
- Vector storage and indexing
- Similarity search
- Query processing
- Python-native API

Example usage:
--------------
import coretexdb
import numpy as np

# Initialize database (talks to a running server over REST)
db = coretexdb.CoreTexDB("localhost", port=5000)

# Create a collection, then insert vectors
db.create_collection("collection1", dimension=3)
vectors = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
db.insert("collection1", vectors)

# Search for similar vectors
query = np.array([1.1, 2.1, 3.1])
results = db.search("collection1", query, k=2)
print(results)

Naming: the canonical classes are ``CoreTexDB*`` (brand-consistent with the
``coretexdb`` package). The pre-1.0 ``CortexDB*`` names remain importable as
aliases of the same objects until 1.0.
"""

from .core import CoreTexDB, CortexDB
from .client import (
    AsyncCoreTexDBClient,
    AsyncCortexDBClient,
    CoreTexDBClient,
    CortexDBClient,
)
from .grpc_client import (
    AsyncCoreTexDBGrpcClient,
    AsyncCortexDBGrpcClient,
    CoreTexDBGrpcClient,
    CortexDBGrpcClient,
)
from .version import __version__
from . import integrations
from . import protocol

__all__ = [
    # Canonical names (brand-consistent).
    "CoreTexDB",
    "CoreTexDBClient",
    "AsyncCoreTexDBClient",
    "CoreTexDBGrpcClient",
    "AsyncCoreTexDBGrpcClient",
    # Pre-1.0 compatibility aliases (same objects; removed at 1.0).
    "CortexDB",
    "CortexDBClient",
    "AsyncCortexDBClient",
    "CortexDBGrpcClient",
    "AsyncCortexDBGrpcClient",
    "integrations",
    "protocol",
    "__version__",
]
