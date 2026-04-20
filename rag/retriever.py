from typing import Any, Dict, List, Optional
import numpy as np


class HybridRetriever:
    def __init__(
        self,
        cortex_client,
        embedding_service,
        vector_weight: float = 0.7,
        scalar_weight: float = 0.3,
        min_score: float = 0.0
    ):
        self.cortex_client = cortex_client
        self.embedding_service = embedding_service
        self.vector_weight = vector_weight
        self.scalar_weight = scalar_weight
        self.min_score = min_score

    def retrieve(
        self,
        query: str,
        collection: str = "default",
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        score_threshold: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        query_embedding = self.embedding_service.embed_text(query)[0]

        vector_results = self.cortex_client.search(
            collection=collection,
            query_vector=query_embedding,
            limit=limit * 2,
            filter_dict=filters,
            score_threshold=score_threshold
        )

        if filters:
            scalar_results = self._scalar_search(collection, filters, limit * 2)
            combined_results = self._combine_results(
                vector_results,
                scalar_results,
                limit
            )
            return combined_results

        return vector_results[:limit]

    def retrieve_by_vector(
        self,
        query_vector: List[float],
        collection: str = "default",
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        results = self.cortex_client.search(
            collection=collection,
            query_vector=query_vector,
            limit=limit,
            filter_dict=filters
        )
        return results

    def retrieve_by_keyword(
        self,
        keywords: List[str],
        collection: str = "default",
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        results = self.cortex_client.search(
            collection=collection,
            query_vector=self.embedding_service.embed_text(" ".join(keywords))[0],
            limit=limit
        )
        return results

    def _scalar_search(
        self,
        collection: str,
        filters: Dict[str, Any],
        limit: int
    ) -> List[Dict[str, Any]]:
        results = self.cortex_client.search(
            collection=collection,
            query_vector=[0.0] * 384,
            limit=limit,
            filter_dict=filters
        )
        return results

    def _combine_results(
        self,
        vector_results: List[Dict[str, Any]],
        scalar_results: List[Dict[str, Any]],
        limit: int
    ) -> List[Dict[str, Any]]:
        score_map = {}

        for result in vector_results:
            doc_id = result.get("id")
            if doc_id:
                vector_score = result.get("score", 0.0)
                scalar_score = 0.0
                for sr in scalar_results:
                    if sr.get("id") == doc_id:
                        scalar_score = sr.get("score", 0.0)
                        break

                combined_score = (
                    vector_score * self.vector_weight +
                    scalar_score * self.scalar_weight
                )
                score_map[doc_id] = {
                    "id": doc_id,
                    "score": combined_score,
                    "payload": result.get("payload", {})
                }

        for result in scalar_results:
            doc_id = result.get("id")
            if doc_id and doc_id not in score_map:
                scalar_score = result.get("score", 0.0)
                score_map[doc_id] = {
                    "id": doc_id,
                    "score": scalar_score * self.scalar_weight,
                    "payload": result.get("payload", {})
                }

        combined = list(score_map.values())
        combined.sort(key=lambda x: x.get("score", 0.0), reverse=True)

        if self.min_score > 0:
            combined = [r for r in combined if r.get("score", 0.0) >= self.min_score]

        return combined[:limit]

    def set_weights(self, vector_weight: float, scalar_weight: float):
        total = vector_weight + scalar_weight
        if total == 0:
            return
        self.vector_weight = vector_weight / total
        self.scalar_weight = scalar_weight / total
