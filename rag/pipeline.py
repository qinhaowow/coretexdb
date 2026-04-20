from typing import Any, Dict, List, Optional, Callable


class RAGPipeline:
    def __init__(
        self,
        retriever,
        llm_client=None,
        reranker: Optional[Any] = None,
        max_context_length: int = 4096,
        temperature: float = 0.7
    ):
        self.retriever = retriever
        self.llm_client = llm_client
        self.reranker = reranker
        self.max_context_length = max_context_length
        self.temperature = temperature

    def query(
        self,
        query_text: str,
        collection: str = "default",
        top_k: int = 5,
        return_sources: bool = True,
        use_reranker: bool = False
    ) -> Dict[str, Any]:
        retrieved_docs = self.retriever.retrieve(
            query=query_text,
            collection=collection,
            limit=top_k
        )

        if use_reranker and self.reranker:
            retrieved_docs = self._rerank(query_text, retrieved_docs, top_k)

        context = self._build_context(retrieved_docs)

        if self.llm_client:
            prompt = self._build_prompt(query_text, context)
            response = self.llm_client.generate(
                prompt=prompt,
                temperature=self.temperature
            )
            result = {
                "answer": response,
                "query": query_text
            }
        else:
            result = {
                "answer": context[:500],
                "query": query_text
            }

        if return_sources:
            sources = [
                {
                    "content": doc.get("payload", {}).get("text", ""),
                    "score": doc.get("score", 0.0)
                }
                for doc in retrieved_docs
            ]
            result["sources"] = sources

        return result

    def query_stream(
        self,
        query_text: str,
        collection: str = "default",
        top_k: int = 5,
        callback: Optional[Callable[[str], None]] = None
    ) -> Dict[str, Any]:
        retrieved_docs = self.retriever.retrieve(
            query=query_text,
            collection=collection,
            limit=top_k
        )

        context = self._build_context(retrieved_docs)

        if self.llm_client:
            prompt = self._build_prompt(query_text, context)
            response = self.llm_client.generate_stream(
                prompt=prompt,
                temperature=self.temperature,
                callback=callback
            )
            return {
                "answer": response,
                "query": query_text
            }

        return {
            "answer": context[:500],
            "query": query_text
        }

    def batch_query(
        self,
        queries: List[str],
        collection: str = "default",
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        results = []
        for query in queries:
            result = self.query(query, collection, top_k)
            results.append(result)
        return results

    def add_documents(
        self,
        texts: List[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
        collection: str = "default"
    ) -> Dict[str, Any]:
        if metadatas is None:
            metadatas = [{}] * len(texts)

        vectors = []
        payloads = []

        for i, text in enumerate(texts):
            if hasattr(self.retriever.embedding_service, 'embed_text'):
                embedding = self.retriever.embedding_service.embed_text(text)[0]
            else:
                embedding = self.retriever.embedding_service(text)

            vectors.append(embedding)
            payloads.append({
                "text": text,
                "metadata": metadatas[i]
            })

        self.retriever.cortex_client.insert(
            collection=collection,
            vectors=vectors,
            payloads=payloads
        )

        return {
            "status": "success",
            "count": len(texts)
        }

    def _build_context(self, documents: List[Dict[str, Any]]) -> str:
        context_parts = []
        total_length = 0

        for doc in documents:
            content = doc.get("payload", {}).get("text", "")
            if content:
                if total_length + len(content) > self.max_context_length:
                    remaining = self.max_context_length - total_length
                    if remaining > 0:
                        context_parts.append(content[:remaining])
                    break
                context_parts.append(content)
                total_length += len(content)

        return "\n\n".join(context_parts)

    def _build_prompt(self, query: str, context: str) -> str:
        prompt = f"""Based on the following context, please answer the question.

Context:
{context}

Question: {query}

Answer:"""
        return prompt

    def _rerank(
        self,
        query: str,
        documents: List[Dict[str, Any]],
        top_k: int
    ) -> List[Dict[str, Any]]:
        if self.reranker:
            return self.reranker.rerank(query, documents, top_k)
        return documents[:top_k]

    def get_collections(self) -> List[str]:
        return self.retriever.cortex_client.get_collections()

    def create_collection(
        self,
        name: str,
        vector_size: int = 384,
        distance_metric: str = "cosine"
    ) -> Dict[str, Any]:
        return self.retriever.cortex_client.create_collection(
            name=name,
            vector_size=vector_size,
            distance_metric=distance_metric
        )
