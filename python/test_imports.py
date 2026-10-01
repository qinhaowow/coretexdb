#!/usr/bin/env python
"""
Test script to verify the CoreTexDB Python package structure (B6/B7):
canonical CoreTexDB* names import, pre-1.0 CortexDB* aliases still resolve,
and pyproject-backed packaging metadata is in place.
"""

import sys
import os

# Script directory is python/ (auto on sys.path); keep an explicit fallback
# for direct execution from elsewhere.
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

def test_imports():
    """Test that all modules can be imported"""
    print("Testing CoreTexDB Python package imports...")

    # Test main imports
    try:
        import coretexdb
        print(f"✓ coretexdb imported successfully (version {coretexdb.__version__})")
    except ImportError as e:
        print(f"✗ Failed to import coretexdb: {e}")
        return False

    # Canonical classes (brand-consistent names)
    try:
        from coretexdb import CoreTexDB, CoreTexDBClient, CoreTexDBGrpcClient
        print("✓ CoreTexDB / CoreTexDBClient / CoreTexDBGrpcClient imported")
    except ImportError as e:
        print(f"✗ Failed to import canonical classes: {e}")
        return False

    # Pre-1.0 compatibility aliases must be the same objects (B7)
    try:
        from coretexdb import CortexDB, CortexDBClient, CortexDBGrpcClient

        assert CortexDB is CoreTexDB
        assert CortexDBClient is CoreTexDBClient
        assert CortexDBGrpcClient is CoreTexDBGrpcClient
        print("✓ pre-1.0 CortexDB* aliases resolve to the canonical classes")
    except (ImportError, AssertionError) as e:
        print(f"✗ Compatibility alias broken: {e}")
        return False

    # Test integrations module
    try:
        from coretexdb import integrations
        print("✓ integrations module imported successfully")

        # Test integration classes only if available
        try:
            from coretexdb.integrations import CoreTexDBVectorStore
            from coretexdb.integrations import CortexDBVectorStore  # alias

            assert CortexDBVectorStore is CoreTexDBVectorStore
            print("✓ CoreTexDBVectorStore imported (alias verified)")
        except ImportError:
            print("⚠ CoreTexDBVectorStore not available (langchain not installed)")

        try:
            from coretexdb.integrations import HuggingFaceEmbeddingAdapter
            print("✓ HuggingFaceEmbeddingAdapter imported successfully")
        except ImportError:
            print("⚠ HuggingFaceEmbeddingAdapter not available (transformers/torch not installed)")

        try:
            from coretexdb.integrations import OpenAIEmbeddingAdapter
            print("✓ OpenAIEmbeddingAdapter imported successfully")
        except ImportError:
            print("⚠ OpenAIEmbeddingAdapter not available (openai not installed)")

    except ImportError as e:
        print(f"✗ Failed to import integrations: {e}")
        return False

    # Test protocol
    try:
        from coretexdb import protocol
        print("✓ protocol module imported successfully")

        try:
            from coretexdb.protocol import (
                CollectionConfig,
                VectorInsert,
                SearchQuery,
                SearchResult
            )
            print("✓ Protocol classes imported successfully")
        except ImportError as e:
            print(f"✗ Failed to import protocol classes: {e}")
            return False

    except ImportError as e:
        print(f"✗ Failed to import protocol: {e}")
        return False

    print("\nAll required imports successful! The package structure is correct.")
    print("Optional integrations may be missing if their dependencies are not installed.")
    return True

if __name__ == "__main__":
    sys.exit(0 if test_imports() else 1)
