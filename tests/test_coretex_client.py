import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.cortex_client import CortexClient
from core.embedding_service import EmbeddingService


class TestCortexClient(unittest.TestCase):
    def setUp(self):
        self.client = CortexClient(host="localhost", port=5000)

    def test_connect(self):
        result = self.client.connect()
        self.assertTrue(result)
        self.assertTrue(self.client.is_connected())

    def test_disconnect(self):
        self.client.connect()
        self.client.disconnect()
        self.assertFalse(self.client.is_connected())

    def test_create_collection(self):
        self.client.connect()
        result = self.client.create_collection(
            name="test_collection",
            vector_size=128,
            distance_metric="cosine"
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["name"], "test_collection")

    def test_insert(self):
        self.client.connect()
        vectors = [[0.1] * 128 for _ in range(10)]
        payloads = [{"text": f"doc_{i}"} for i in range(10)]
        ids = [f"doc_{i}" for i in range(10)]

        result = self.client.insert(
            collection="test_collection",
            vectors=vectors,
            payloads=payloads,
            ids=ids
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["count"], 10)

    def test_search(self):
        self.client.connect()
        query_vector = [0.1] * 128
        results = self.client.search(
            collection="test_collection",
            query_vector=query_vector,
            limit=5
        )
        self.assertIsInstance(results, list)

    def test_get(self):
        self.client.connect()
        results = self.client.get(
            collection="test_collection",
            limit=10
        )
        self.assertIsInstance(results, list)

    def test_delete(self):
        self.client.connect()
        result = self.client.delete(
            collection="test_collection",
            ids=["doc_0"]
        )
        self.assertEqual(result["status"], "success")

    def test_delete_collection(self):
        self.client.connect()
        result = self.client.delete_collection("test_collection")
        self.assertEqual(result["status"], "success")

    def test_get_collections(self):
        self.client.connect()
        collections = self.client.get_collections()
        self.assertIsInstance(collections, list)


class TestEmbeddingService(unittest.TestCase):
    def setUp(self):
        self.service = EmbeddingService()

    def test_load_model(self):
        result = self.service.load_model()
        self.assertIsNotNone(result)

    def test_embed_text_single(self):
        self.service.load_model()
        embedding = self.service.embed_text("Hello world")
        self.assertIsInstance(embedding, list)
        self.assertEqual(len(embedding), 1)
        self.assertEqual(len(embedding[0]), self.service.dimension)

    def test_embed_text_multiple(self):
        self.service.load_model()
        texts = ["Hello world", "Test document", "Sample text"]
        embeddings = self.service.embed_text(texts)
        self.assertEqual(len(embeddings), 3)
        for emb in embeddings:
            self.assertEqual(len(emb), self.service.dimension)

    def test_embed_image(self):
        self.service.load_model()
        embedding = self.service.embed_image("test_image.jpg")
        self.assertIsInstance(embedding, list)
        self.assertEqual(len(embedding), self.service.dimension)

    def test_embed_audio(self):
        self.service.load_model()
        embedding = self.service.embed_audio("test_audio.wav")
        self.assertIsInstance(embedding, list)
        self.assertEqual(len(embedding), self.service.dimension)

    def test_get_dimension(self):
        dimension = self.service.get_dimension()
        self.assertEqual(dimension, self.service.dimension)

    def test_batch_embed(self):
        self.service.load_model()
        texts = [f"text_{i}" for i in range(50)]
        embeddings = self.service.batch_embed(texts)
        self.assertEqual(len(embeddings), 50)


if __name__ == "__main__":
    unittest.main()
