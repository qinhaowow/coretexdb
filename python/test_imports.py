#!/usr/bin/env python
"""
Test script to verify the CortexDB Python package structure
"""

import sys
import os

# Add the parent directory to the path so we can import cortexdb
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def test_imports():
    """Test that all modules can be imported"""
    print("Testing CortexDB Python package imports...")
    
    # Test main imports
    try:
        import cortexdb
        print("✓ cortexdb imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import cortexdb: {e}")
        return False
    
    # Test version
    try:
        version = cortexdb.__version__
        print(f"✓ cortexdb version: {version}")
    except AttributeError as e:
        print(f"✗ Failed to get version: {e}")
        return False
    
    # Test core classes
    try:
        from cortexdb import CortexDB
        print("✓ CortexDB imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import CortexDB: {e}")
        return False
    
    # Test client classes
    try:
        from cortexdb import CortexDBClient, AsyncCortexDBClient
        print("✓ CortexDBClient and AsyncCortexDBClient imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import clients: {e}")
        return False
    
    # Test integrations module
    try:
        from cortexdb import integrations
        print("✓ integrations module imported successfully")
        
        # Test integration classes only if available
        try:
            from cortexdb.integrations import CortexDBVectorStore
            print("✓ CortexDBVectorStore imported successfully")
        except ImportError:
            print("⚠ CortexDBVectorStore not available (langchain not installed)")
        
        try:
            from cortexdb.integrations import HuggingFaceEmbeddingAdapter
            print("✓ HuggingFaceEmbeddingAdapter imported successfully")
        except ImportError:
            print("⚠ HuggingFaceEmbeddingAdapter not available (transformers/torch not installed)")
        
        try:
            from cortexdb.integrations import OpenAIEmbeddingAdapter
            print("✓ OpenAIEmbeddingAdapter imported successfully")
        except ImportError:
            print("⚠ OpenAIEmbeddingAdapter not available (openai not installed)")
            
    except ImportError as e:
        print(f"✗ Failed to import integrations: {e}")
        return False
    
    # Test protocol
    try:
        from cortexdb import protocol
        print("✓ protocol imported successfully")
        
        # Test protocol classes
        try:
            from cortexdb.protocol import (
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
    test_imports()