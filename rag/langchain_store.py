from typing import Any, Dict, List, Optional, Callable
import numpy as np


class LangChainVectorStore:
    def __init__(
        self,
        cortex_client,
        embedding_service,
        collection_name: str = "langchain_store",
        text_field: str = "text",
        metadata_field: str = "metadata"
    ):
        self.cortex_client = cortex_client
        self.embedding_service = embedding_service
        self.collection_name = collection_name
        self.text_field = text_field
        self.metadata_field = metadata_field
        self._initialized = False

    def initialize(self):
        if not self.cortex_client.collection_exists(self.collection_name):
            self.cortex_client.create_collection(
                name=self.collection_name,
                vector_size=self.embedding_service.get_dimension()
            )
        self._initialized = True

    def add_texts(
        self,
        texts: List[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
        ids: Optional[List[str]] = None,
        **kwargs
    ) -> List[str]:
        if not self._initialized:
            self.initialize()

        if metadatas is None:
            metadatas = [{}] * len(texts)

        if ids is None:
            ids = [f"doc_{i}" for i in range(len(texts))]

        embeddings = self.embedding_service.embed_text(texts)

        payloads = []
        for text, metadata in zip(texts, metadatas):
            payload = {
                self.text_field: text,
                self.metadata_field: metadata
            }
            payloads.append(payload)

        self.cortex_client.insert(
            collection=self.collection_name,
            vectors=embeddings,
            payloads=payloads,
            ids=ids
        )

        return ids

    def add_documents(self, documents: List[Any], **kwargs) -> List[str]:
        texts = []
        metadatas = []

        for doc in documents:
            if hasattr(doc, 'page_content'):
                texts.append(doc.page_content)
            elif hasattr(doc, 'text'):
                texts.append(doc.text)
            else:
                texts.append(str(doc))

            if hasattr(doc, 'metadata'):
                metadatas.append(doc.metadata)
            else:
                metadatas.append({})

        return self.add_texts(texts, metadatas, **kwargs)

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        filter: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        if not self._initialized:
            self.initialize()

        results = self.cortex_client.search(
            collection=self.collection_name,
            query_vector=self.embedding_service.embed_text(query)[0],
            limit=k,
            filter_dict=filter
        )

        return [r.get("payload", {}) for r in results]

    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        filter: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> List[tuple]:
        if not self._initialized:
            self.initialize()

        results = self.cortex_client.search(
            collection=self.collection_name,
            query_vector=self.embedding_service.embed_text(query)[0],
            limit=k,
            filter_dict=filter
        )

        return [
            (r.get("payload", {}), r.get("score", 0.0))
            for r in results
        ]

    def similarity_search_by_vector(
        self,
        embedding: List[float],
        k: int = 4,
        filter: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        if not self._initialized:
            self.initialize()

        results = self.cortex_client.search(
            collection=self.collection_name,
            query_vector=embedding,
            limit=k,
            filter_dict=filter
        )

        return [r.get("payload", {}) for r in results]

    def max_marginal_relevance_search(
        self,
        query: str,
        k: int = 4,
        fetch_k: int = 20,
        lambda_mult: float = 0.5,
        filter: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        if not self._initialized:
            self.initialize()

        results = self.cortex_client.search(
            collection=self.collection_name,
            query_vector=self.embedding_service.embed_text(query)[0],
            limit=fetch_k,
            filter_dict=filter
        )

        if len(results) <= k:
            return [r.get("payload", {}) for r in results]

        selected = []
        query_embedding = self.embedding_service.embed_text(query)[0]

        for _ in range(k):
            best_idx = None
            best_score = -1

            for idx, r in enumerate(results):
                if r in selected:
                    continue

                doc_embedding = r.get("embedding", [0.0] * 384)
                relevance = self._cosine_similarity(query_embedding, doc_embedding)

                diversity = 0.0
                for sel in selected:
                    sel_embedding = sel.get("embedding", [0.0] * 384)
                    diversity += self._cosine_similarity(doc_embedding, sel_embedding)

                if len(selected) > 0:
                    diversity /= len(selected)

                mmr_score = lambda_mult * relevance + (1 - lambda_mult) * (1 - diversity)

                if mmr_score > best_score:
                    best_score = mmr_score
                    best_idx = idx

            if best_idx is not None:
                selected.append(results[best_idx])

        return [r.get("payload", {}) for r in selected]

    def delete(self, ids: Optional[List[str]] = None, **kwargs):
        if ids:
            self.cortex_client.delete(
                collection=self.collection_name,
                ids=ids
            )

    def get_by_ids(self, ids: List[str]) -> List[Dict[str, Any]]:
        results = self.cortex_client.get(
            collection=self.collection_name,
            ids=ids
        )
        return [r.get("payload", {}) for r in results]

    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = sum(a * a for a in vec1) ** 0.5
        norm2 = sum(b * b for b in vec2) ** 0.5
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return dot_product / (norm1 * norm2)

    @classmethod
    def from_texts(
        cls,
        texts: List[str],
        embedding_service,
        cortex_client,
        metadatas: Optional[List[Dict[str, Any]]] = None,
        collection_name: str = "langchain_store",
        **kwargs
    ) -> 'LangChainVectorStore':
        store = cls(
            cortex_client=cortex_client,
            embedding_service=embedding_service,
            collection_name=collection_name
        )
        store.initialize()
        store.add_texts(texts, metadatas)
        return store
