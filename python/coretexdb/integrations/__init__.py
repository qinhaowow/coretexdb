"""
Integrations for CortexDB Python SDK
"""

# Handle optional dependencies
try:
    from .langchain import CortexDBVectorStore
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

try:
    from .huggingface import HuggingFaceEmbeddingAdapter
    HUGGINGFACE_AVAILABLE = True
except ImportError:
    HUGGINGFACE_AVAILABLE = False

try:
    from .openai import OpenAIEmbeddingAdapter
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Define __all__ based on available dependencies
__all__ = []

if LANGCHAIN_AVAILABLE:
    __all__.append("CortexDBVectorStore")

if HUGGINGFACE_AVAILABLE:
    __all__.append("HuggingFaceEmbeddingAdapter")

if OPENAI_AVAILABLE:
    __all__.append("OpenAIEmbeddingAdapter")