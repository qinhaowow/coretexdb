# CoreTexDB Python SDK

A multimodal vector database for AI applications — vector storage, similarity
search, hybrid retrieval, and a server-backed client.

The SDK talks to a running CoreTexDB server over REST (or gRPC via the
`grpc_client` module). It does **not** embed the engine.

---

## Install

```bash
pip install coretexdb
```

From a checkout of this repository:

```bash
# The gRPC stubs are generated, not committed. They go *into* the package,
# because a wheel carries only `coretexdb*` — stubs left beside it would be
# dropped, and the installed SDK would import fine while reporting protobuf as
# unavailable and raising on the first gRPC call.
pip install grpcio-tools
python -m grpc_tools.protoc -I./src/coretex_grpc \
  --python_out=./python/coretexdb --grpc_python_out=./python/coretexdb \
  ./src/coretex_grpc/coretex.proto

# protoc emits an absolute import; inside the package it must be relative.
sed -i 's/^import coretex_pb2 as /from . import coretex_pb2 as /' \
  python/coretexdb/coretex_pb2_grpc.py

cd python && pip install -e .
```

CI does the same two steps before packaging, in both `build.yml` and
`release.yml`.

Extras:

| Extra | Adds | Needed for |
|---|---|---|
| `coretexdb[openai]` | `openai` | OpenAI embedding helpers |
| `coretexdb[langchain]` | `langchain` | `CoreTexDBVectorStore` |
| `coretexdb[huggingface]` | `transformers`, `torch` | `HuggingFaceEmbeddingAdapter` |

The gRPC clients ship in the base package; they need `grpcio` present at
runtime, which the server's install root provides.

Requires Python 3.9+ (per `requires-python`) and `numpy`.

---

## Naming

The package is `coretexdb`; the classes are `CoreTexDB*`. Before 1.0 the older
`CortexDB*` spellings remain importable **as aliases of the same objects**, so
existing code keeps working:

```python
import coretexdb

coretexdb.CoreTexDB is coretexdb.CortexDB          # True — same object
```

| Canonical | Alias (until 1.0) |
|---|---|
| `CoreTexDB` | `CortexDB` |
| `CoreTexDBClient` | `CortexDBClient` |
| `AsyncCoreTexDBClient` | `AsyncCortexDBClient` |
| `CoreTexDBGrpcClient` | `CortexDBGrpcClient` |
| `AsyncCoreTexDBGrpcClient` | `AsyncCortexDBGrpcClient` |
| `CoreTexDBVectorStore` | `CortexDBVectorStore` |

New code should use the canonical names.

---

## Quick start

```python
import numpy as np
import coretexdb

db = coretexdb.CoreTexDB("localhost", port=5000)

db.create_collection("docs", dimension=128)

vectors = np.random.randn(1000, 128).astype(np.float32)
db.insert("docs", vectors)

query = np.random.randn(128).astype(np.float32)
for hit in db.search("docs", query, k=5):
    print(hit.id, hit.score)
```

Start the server first:

```bash
./target/release/coretex server --data-dir /opt/CoreTexDB
```

---

## Configuration

Connection settings come from arguments or the environment:

| Argument | Environment variable | Default |
|---|---|---|
| `host` | `CORTEXDB_HOST` | `localhost` |
| `port` | `CORTEXDB_PORT` | `5000` |
| `api_key` | `CORTEXDB_API_KEY` | none |
| `timeout` | `CORTEXDB_TIMEOUT` | `30.0` |

`api_key` is sent as `Authorization: Bearer <key>`, which matters only when the
server runs with `enable_auth`.

---

## `CoreTexDB` — the main interface

```python
CoreTexDB(host="localhost", port=5000, api_key=None, timeout=30.0)
```

| Method | Description |
|---|---|
| `create_collection(name, dimension, ...)` | Create a collection; see below for the metric/index arguments |
| `insert(collection, vectors, ids=None, metadata=None)` | Insert or overwrite vectors |
| `search(collection, query, k=10, filter=None)` | Nearest neighbours |
| `delete_collection(name)` | Drop a collection and its data |
| `list_collections()` | Names of every collection |
| `get_collection_info(name)` | Dimension, metric, index type, row count |

### Insert

```python
db.insert(
    "docs",
    vectors,                       # (n, dim) float array
    ids=[f"doc-{i}" for i in range(len(vectors))],
    metadata=[{"lang": "en"}, {"lang": "fr"}],
)
```

### Search

```python
results = db.search("docs", query, k=10, filter={"lang": "en"})
```

`filter` is a MongoDB-style document, evaluated on `metadata`. Supported
operators: `$eq` (implicit), `$gt` / `$gte` / `$lt` / `$lte`, `$in`, `$ne`,
`$exists`, `$regex`, `$and`, `$or`, `$not`. A broad filter does not shorten the
result — the engine oversamples the index and re-checks, falling back to an
exact scan when the proposals cannot fill `k`.

The same engine exposes a metadata inverted index, so selective filters are
sub-linear rather than a full scan.

---

## Hybrid retrieval

The engine fuses vector similarity with BM25 text matching over `metadata`
(reciprocal rank fusion), and can rescore the fused candidates. Both are
reachable over the REST API:

```bash
curl -X POST localhost:5000/api/collections/docs/hybrid-search \
  -H 'Content-Type: application/json' \
  -d '{"vector":[0.1,0.2],"text":"error handling","k":10,"text_field":"body"}'
```

The Python wrapper (`CoreTexDB`) does not expose hybrid search yet — use
`CoreTexDBClient` against the endpoint directly, or the `coretex search`
CLI.

## Clients

### `CoreTexDBClient` — the lower-level HTTP client

Use it when you want a specific REST endpoint rather than the convenience
wrapper:

```python
from coretexdb import CoreTexDBClient

client = CoreTexDBClient("localhost", port=5000)
client.create_collection("docs", dimension=8)
client.insert("docs", [{"id": "a", "vector": [1.0] * 8}])
client.batch_search("docs", queries=[[0.1] * 8], k=3)
client.health_check()
```

| Method | REST |
|---|---|
| `create_collection` | `POST /api/collections` |
| `list_collections` | `GET /api/collections` |
| `delete_collection` | `DELETE /api/collections/:name` |
| `get_collection_stats` | `GET /api/collections/:name/stats` |
| `insert` | `POST /api/collections/:name/vectors` |
| `update` | `PUT /api/collections/:name/vectors` |
| `delete` | `DELETE /api/collections/:name/vectors` |
| `search` | `POST /api/collections/:name/search` |
| `batch_search` | `POST /api/collections/:name/batch-search` |
| `health_check` | `GET /health` |

### `AsyncCoreTexDBClient`

Same surface, `async`/`await`:

```python
from coretexdb import AsyncCoreTexDBClient

client = AsyncCoreTexDBClient("localhost", port=5000)
await client.create_collection("docs", dimension=8)
hits = await client.search("docs", [0.1] * 8, k=5)
```

### gRPC

```python
from coretexdb import CoreTexDBGrpcClient

client = CoreTexDBGrpcClient("localhost", port=50051)
```

Requires the server started with gRPC enabled and the `grpc` extra installed.

---

## LangChain integration

```python
from coretexdb import CoreTexDBVectorStore

store = CoreTexDBVectorStore(
    collection="docs",
    embedding=my_embeddings,        # any LangChain Embeddings
    host="localhost",
    port=5000,
)

store.add_texts(["error handling guide", "install steps"])
docs = store.similarity_search("how do I install?", k=3)
docs_with_scores = store.similarity_search_with_score("install", k=3)
```

`CoreTexDBVectorStore.from_texts(...)` creates the collection and populates it in
one step.

---

## HuggingFace embeddings

```python
from coretexdb.integrations.huggingface import HuggingFaceEmbeddingAdapter

embeddings = HuggingFaceEmbeddingAdapter(model_name="BAAI/bge-small-en-v1.5")
vec = embeddings.embed_query("error handling")
dim = embeddings.get_dimension()
```

---

## Errors

The SDK raises `requests.exceptions.HTTPError` subclasses for transport
problems, and `ValueError` for malformed arguments (a filter that is not a JSON
document, a dimension mismatch). Server-side error bodies come back as the
exception's message.

```python
from coretexdb import CoreTexDB

db = CoreTexDB("localhost", port=5000)
try:
    db.search("does-not-exist", [0.0] * 8, k=5)
except Exception as exc:
    print("search failed:", exc)
```

---

## Tests

```bash
cd python && python -m pytest tests/ -v
```

Tests live in `tests/test_core.py` and cover the naming aliases, the REST
surface, and the integrations' importability.

---

## Version

`coretexdb.__version__` is `1.0.12` — an independent line from the Rust crate's
`0.2.5`. `python/coretexdb/version.py` is the single source of truth; the
`pyproject.toml` reads it rather than repeating the number.