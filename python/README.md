# CortexDB Python SDK

Python SDK for CortexDB vector database, providing a high-level interface for working with vector embeddings and similarity search.

## Features

- **Simple Python API** for vector database operations
- **Integration with popular AI frameworks** (LangChain, HuggingFace, OpenAI)
- **Async support** for high-performance operations
- **DataFrame integration** with pandas
- **Type hints** for better IDE support
- **Pydantic models** for data validation

## Installation

### Basic Installation

```bash
pip install cortexdb
```

### With Optional Dependencies

```bash
# With LangChain integration
pip install "cortexdb[langchain]"

# With HuggingFace integration
pip install "cortexdb[huggingface]"

# With OpenAI integration
pip install "cortexdb[openai]"

# With all integrations
pip install "cortexdb[langchain,huggingface,openai]"

# For development
pip install "cortexdb[dev]"
```

## Quick Start

### Basic Usage

```python
import cortexdb
import numpy as np

# Initialize client
client = cortexdb.CortexDBClient(host="localhost", port=8080)

# Create a collection
client.create_collection(
    name="documents",
    dimension=768,  # BERT embedding dimension
    metric="cosine"
)

# Insert vectors
vectors = np.random.rand(10, 768).astype(np.float32)
metadata = [{"id": i, "text": f"Document {i}"} for i in range(10)]
client.insert("documents", vectors, metadata)

# Search for similar vectors
query_vector = np.random.rand(768).astype(np.float32)
results = client.search("documents", query_vector, k=5)

print("Search results:")
for result in results:
    print(f"ID: {result.id}, Score: {result.score}, Metadata: {result.metadata}")
```

### Using with Pandas

```python
import pandas as pd
import cortexdb

# Initialize client
client = cortexdb.CortexDBClient(host="localhost", port=8080)

# Load data
df = pd.read_csv("data.csv")

# Generate embeddings (using your preferred model)
# df["embedding"] = generate_embeddings(df["text"])

# Insert into collection
# client.insert("documents", df["embedding"].tolist(), df.to_dict("records"))

# Search
# results = client.search("documents", query_embedding, k=5)
```

### Async Usage

```python
import asyncio
import cortexdb

async def main():
    # Initialize async client
    client = cortexdb.AsyncCortexDBClient(host="localhost", port=8080)
    
    # Create collection
    await client.create_collection("documents", dimension=768)
    
    # Insert vectors
    # await client.insert("documents", vectors, metadata)
    
    # Search
    # results = await client.search("documents", query_vector, k=5)
    
    print("Operation completed successfully")

if __name__ == "__main__":
    asyncio.run(main())
```

## API Reference

### Client Initialization

```python
# Synchronous client
client = cortexdb.CortexDBClient(
    host="localhost",
    port=8080,
    api_key=None,
    timeout=30.0
)

# Asynchronous client
async_client = cortexdb.AsyncCortexDBClient(
    host="localhost",
    port=8080,
    api_key=None,
    timeout=30.0
)
```

### Collection Operations

- `create_collection(name, dimension, metric="cosine")`: Create a new collection
- `list_collections()`: List all collections
- `delete_collection(name)`: Delete a collection
- `get_collection_stats(name)`: Get collection statistics

### Vector Operations

- `insert(collection, vectors, metadata=None)`: Insert vectors with optional metadata
- `search(collection, query, k=10, filter=None)`: Search for similar vectors
- `delete(collection, ids)`: Delete vectors by ID
- `update(collection, ids, vectors=None, metadata=None)`: Update vectors and/or metadata

### Batch Operations

- `batch_insert(collection, vectors_batch, metadata_batch=None)`: Insert multiple batches
- `batch_search(collection, queries, k=10)`: Search with multiple queries

## Integrations

### LangChain

```python
from langchain.vectorstores import CortexDB
from langchain.embeddings import OpenAIEmbeddings

# Initialize vector store
embeddings = OpenAIEmbeddings()
vectorstore = CortexDB(
    embedding_function=embeddings,
    collection_name="documents",
    host="localhost",
    port=8080
)

# Add documents
# vectorstore.add_documents(documents)

# Similarity search
# results = vectorstore.similarity_search("query text", k=5)
```

### HuggingFace

```python
from transformers import AutoTokenizer, AutoModel
import torch
import cortexdb

# Load model
tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

# Initialize client
client = cortexdb.CortexDBClient(host="localhost", port=8080)

# Generate embeddings
def get_embedding(text):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
    with torch.no_grad():
        embeddings = model(**inputs).last_hidden_state.mean(dim=1)
    return embeddings.squeeze().numpy()

# Use embeddings
# query_embedding = get_embedding("query text")
# results = client.search("documents", query_embedding, k=5)
```

### OpenAI

```python
import openai
import cortexdb

# Set up OpenAI
openai.api_key = "your-api-key"

# Initialize client
client = cortexdb.CortexDBClient(host="localhost", port=8080)

# Generate embeddings
def get_embedding(text):
    response = openai.embeddings.create(
        input=text,
        model="text-embedding-ada-002"
    )
    return response.data[0].embedding

# Use embeddings
# query_embedding = get_embedding("query text")
# results = client.search("documents", query_embedding, k=5)
```

## Configuration

### Environment Variables

You can configure the client using environment variables:

- `CORTEXDB_HOST`: Hostname of the CortexDB server (default: localhost)
- `CORTEXDB_PORT`: Port of the CortexDB server (default: 8080)
- `CORTEXDB_API_KEY`: API key for authentication (if required)
- `CORTEXDB_TIMEOUT`: Request timeout in seconds (default: 30.0)

### Example: Using Environment Variables

```bash
export CORTEXDB_HOST=localhost
export CORTEXDB_PORT=8080
python my_script.py
```

```python
# my_script.py
import cortexdb

# Client will use environment variables
client = cortexdb.CortexDBClient()
```

## Troubleshooting

### Common Issues

1. **Connection Error**: Ensure the CortexDB server is running and accessible
2. **Authentication Error**: Check your API key if authentication is enabled
3. **Embedding Dimension Mismatch**: Ensure your vectors match the collection's dimension
4. **Timeout Error**: Increase the timeout value for large operations

### Logging

Enable logging for debugging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

Contributions are welcome! Please see the [GitHub repository](https://github.com/yourusername/cortexdb) for more information.

## License

Apache License 2.0