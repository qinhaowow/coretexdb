import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.cortex_client import CortexClient
from core.embedding_service import EmbeddingService
from rag.retriever import HybridRetriever
from rag.langchain_store import LangChainVectorStore
from rag.pipeline import RAGPipeline


class TestHybridRetriever(unittest.TestCase):
    def setUp(self):
        self.client = CortexClient()
        self.client.connect()
        self.embedding_service = EmbeddingService()
        self.embedding_service.load_model()
        self.retriever = HybridRetriever(
            cortex_client=self.client,
            embedding_service=self.embedding_service
        )

    def test_retrieve(self):
        results = self.retriever.retrieve(
            query="test query",
            collection="test",
            limit=5
        )
        self.assertIsInstance(results, list)

    def test_retrieve_by_vector(self):
        query_vector = [0.1] * 384
        results = self.retriever.retrieve_by_vector(
            query_vector=query_vector,
            collection="test",
            limit=5
        )
        self.assertIsInstance(results, list)

    def test_retrieve_by_keyword(self):
        keywords = ["test", "robot", "memory"]
        results = self.retriever.retrieve_by_keyword(
            keywords=keywords,
            collection="test",
            limit=5
        )
        self.assertIsInstance(results, list)

    def test_set_weights(self):
        self.retriever.set_weights(0.6, 0.4)
        self.assertAlmostEqual(self.retriever.vector_weight, 0.6)
        self.assertAlmostEqual(self.retriever.scalar_weight, 0.4)


class TestLangChainVectorStore(unittest.TestCase):
    def setUp(self):
        self.client = CortexClient()
        self.client.connect()
        self.embedding_service = EmbeddingService()
        self.embedding_service.load_model()
        self.store = LangChainVectorStore(
            cortex_client=self.client,
            embedding_service=self.embedding_service,
            collection_name="test_langchain"
        )

    def test_initialize(self):
        self.store.initialize()
        self.assertTrue(self.store._initialized)

    def test_add_texts(self):
        self.store.initialize()
        texts = ["test document 1", "test document 2", "test document 3"]
        ids = self.store.add_texts(texts)
        self.assertEqual(len(ids), 3)

    def test_similarity_search(self):
        self.store.initialize()
        results = self.store.similarity_search("test query", k=5)
        self.assertIsInstance(results, list)

    def test_similarity_search_with_score(self):
        self.store.initialize()
        results = self.store.similarity_search_with_score("test query", k=5)
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 0)

    def test_get_by_ids(self):
        self.store.initialize()
        ids = ["test_1", "test_2"]
        results = self.store.get_by_ids(ids)
        self.assertIsInstance(results, list)


class TestRAGPipeline(unittest.TestCase):
    def setUp(self):
        self.client = CortexClient()
        self.client.connect()
        self.embedding_service = EmbeddingService()
        self.embedding_service.load_model()
        self.retriever = HybridRetriever(
            cortex_client=self.client,
            embedding_service=self.embedding_service
        )
        self.pipeline = RAGPipeline(retriever=self.retriever)

    def test_query(self):
        result = self.pipeline.query(
            query_text="test query",
            collection="test",
            top_k=5,
            return_sources=True
        )
        self.assertIsInstance(result, dict)
        self.assertIn("answer", result)
        self.assertIn("query", result)

    def test_query_without_llm(self):
        result = self.pipeline.query(
            query_text="test query",
            collection="test"
        )
        self.assertIn("answer", result)

    def test_batch_query(self):
        queries = ["query 1", "query 2", "query 3"]
        results = self.pipeline.batch_query(
            queries=queries,
            collection="test",
            top_k=5
        )
        self.assertEqual(len(results), 3)

    def test_add_documents(self):
        result = self.pipeline.add_documents(
            texts=["doc 1", "doc 2"],
            collection="test"
        )
        self.assertEqual(result["status"], "success")

    def test_get_collections(self):
        collections = self.pipeline.get_collections()
        self.assertIsInstance(collections, list)

    def test_create_collection(self):
        result = self.pipeline.create_collection(
            name="new_test_collection",
            vector_size=384
        )
        self.assertEqual(result["status"], "success")


if __name__ == "__main__":
    unittest.main()
