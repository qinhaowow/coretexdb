"""Tests for coretexdb.CortexDB (standalone Python client)."""

import json
import unittest
from unittest.mock import MagicMock, patch

import numpy as np


class TestCortexDBInit(unittest.TestCase):
    """Tests for CortexDB initialization."""

    @patch("coretexdb.core.requests", MagicMock())
    def test_default_init(self):
        from coretexdb.core import CortexDB

        db = CortexDB()
        assert db.host == "localhost"
        assert db.port == 5000
        assert db.base_url == "http://localhost:5000"

    @patch("coretexdb.core.requests", MagicMock())
    def test_custom_init(self):
        from coretexdb.core import CortexDB

        db = CortexDB(host="10.0.0.1", port=8080, timeout=5.0)
        assert db.host == "10.0.0.1"
        assert db.port == 8080
        assert db.base_url == "http://10.0.0.1:8080"
        assert db.timeout == 5.0

    @patch("coretexdb.core.requests", MagicMock())
    def test_api_key_in_header(self):
        from coretexdb.core import CortexDB

        db = CortexDB(api_key="test-secret")
        assert db.headers["Authorization"] == "Bearer test-secret"


class TestCortexDBInsert(unittest.TestCase):
    """Tests for CortexDB.insert."""

    @patch("coretexdb.core.requests")
    def test_insert_2d_array(self, mock_requests):
        from coretexdb.core import CortexDB

        mock_response = MagicMock()
        mock_response.json.return_value = {"ids": ["0", "1"], "count": 2}
        mock_response.raise_for_status = MagicMock()
        mock_requests.request.return_value = mock_response

        db = CortexDB()
        vectors = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
        result = db.insert("my_collection", vectors)

        assert result["count"] == 2
        mock_requests.request.assert_called_once()

    def test_insert_rejects_1d_array(self):
        from coretexdb.core import CortexDB

        with patch("coretexdb.core.requests", MagicMock()):
            db = CortexDB()
        with self.assertRaises(ValueError):
            db.insert("col", np.array([1.0, 2.0, 3.0]))

    def test_insert_rejects_3d_array(self):
        from coretexdb.core import CortexDB

        with patch("coretexdb.core.requests", MagicMock()):
            db = CortexDB()
        with self.assertRaises(ValueError):
            db.insert("col", np.ones((2, 3, 4)))

    @patch("coretexdb.core.requests")
    def test_insert_with_metadata(self, mock_requests):
        from coretexdb.core import CortexDB

        mock_response = MagicMock()
        mock_response.json.return_value = {"ids": ["0"], "count": 1}
        mock_response.raise_for_status = MagicMock()
        mock_requests.request.return_value = mock_response

        db = CortexDB()
        vectors = np.array([[1.0, 2.0]])
        meta = [{"label": "test"}]
        db.insert("col", vectors, metadata=meta)

        call_args = mock_requests.request.call_args
        payload = call_args[1]["json"]
        assert payload["metadata"] == [{"label": "test"}]


class TestCortexDBSearch(unittest.TestCase):
    """Tests for CortexDB.search."""

    @patch("coretexdb.core.requests")
    def test_search_returns_results(self, mock_requests):
        from coretexdb.core import CortexDB

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {"id": "0", "score": 0.95, "metadata": {}},
                {"id": "1", "score": 0.87, "metadata": {}},
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_requests.request.return_value = mock_response

        db = CortexDB()
        query = np.array([0.1, 0.2, 0.3])
        results = db.search("col", query, k=2)

        assert len(results) == 2
        assert results[0]["id"] == "0"
        assert results[0]["score"] == 0.95

    def test_search_rejects_2d_query(self):
        from coretexdb.core import CortexDB

        with patch("coretexdb.core.requests", MagicMock()):
            db = CortexDB()
        with self.assertRaises(ValueError):
            db.search("col", np.ones((1, 3)))


class TestCortexDBCollectionOps(unittest.TestCase):
    """Tests for collection CRUD operations."""

    @patch("coretexdb.core.requests")
    def test_create_collection(self, mock_requests):
        from coretexdb.core import CortexDB

        mock_response = MagicMock()
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status = MagicMock()
        mock_requests.request.return_value = mock_response

        db = CortexDB()
        result = db.create_collection("new_col", dimension=128, metric="cosine")

        assert result["success"] is True
        call_args = mock_requests.request.call_args
        payload = call_args[1]["json"]
        assert payload["name"] == "new_col"
        assert payload["dimension"] == 128

    @patch("coretexdb.core.requests")
    def test_delete_collection(self, mock_requests):
        from coretexdb.core import CortexDB

        mock_response = MagicMock()
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status = MagicMock()
        mock_requests.request.return_value = mock_response

        db = CortexDB()
        db.delete_collection("old_col")

        call_args = mock_requests.request.call_args
        assert "old_col" in call_args[0][1]

    @patch("coretexdb.core.requests")
    def test_list_collections(self, mock_requests):
        from coretexdb.core import CortexDB

        mock_response = MagicMock()
        mock_response.json.return_value = {"collections": ["a", "b", "c"]}
        mock_response.raise_for_status = MagicMock()
        mock_requests.request.return_value = mock_response

        db = CortexDB()
        cols = db.list_collections()

        assert cols == ["a", "b", "c"]

    @patch("coretexdb.core.requests")
    def test_get_collection_info(self, mock_requests):
        from coretexdb.core import CortexDB

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "name": "col",
            "dimension": 256,
            "vector_count": 1000,
        }
        mock_response.raise_for_status = MagicMock()
        mock_requests.request.return_value = mock_response

        db = CortexDB()
        info = db.get_collection_info("col")

        assert info["dimension"] == 256
        assert info["vector_count"] == 1000


class TestCortexDBConnectionError(unittest.TestCase):
    """Tests for connection error handling."""

    @patch("coretexdb.core.requests")
    def test_connection_error(self, mock_requests):
        import requests as req_lib
        from coretexdb.core import CortexDB

        mock_requests.ConnectionError = req_lib.ConnectionError
        mock_requests.request.side_effect = req_lib.ConnectionError("refused")

        db = CortexDB()
        with self.assertRaises(ConnectionError):
            db.list_collections()


if __name__ == "__main__":
    unittest.main()
