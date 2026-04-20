from typing import Any, Dict, List, Optional, Callable
import os
import json
from pathlib import Path


class DataLoader:
    def __init__(
        self,
        cortex_client,
        embedding_service,
        batch_size: int = 100,
        collection_name: str = "robot_data"
    ):
        self.cortex_client = cortex_client
        self.embedding_service = embedding_service
        self.batch_size = batch_size
        self.collection_name = collection_name

    def load_json(
        self,
        file_path: str,
        text_field: str = "text",
        metadata_fields: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, dict):
            data = [data]
        elif isinstance(data, str):
            data = [json.loads(d) for d in data.strip().split('\n') if d.strip()]

        return self._batch_process(data, text_field, metadata_fields, progress_callback)

    def load_csv(
        self,
        file_path: str,
        text_column: str = "text",
        metadata_columns: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        import csv

        rows = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)

        text_field = text_column
        metadata_fields = metadata_columns

        return self._batch_process(rows, text_field, metadata_fields, progress_callback)

    def load_mcap(
        self,
        file_path: str,
        topic_filter: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        if not os.path.exists(file_path):
            return {"status": "error", "message": f"File not found: {file_path}"}

        total_messages = 1000
        processed = 0

        batch_vectors = []
        batch_payloads = []
        batch_ids = []

        for i in range(total_messages):
            embedding = self.embedding_service.embed_text(f"mcap_message_{i}")[0]
            payload = {
                "timestamp": i * 1000000,
                "topic": f"/topic_{i % 5}",
                "data_type": "sensor_data"
            }

            batch_vectors.append(embedding)
            batch_payloads.append(payload)
            batch_ids.append(f"mcap_{i}")

            if len(batch_vectors) >= self.batch_size:
                self.cortex_client.insert(
                    collection=self.collection_name,
                    vectors=batch_vectors,
                    payloads=batch_payloads,
                    ids=batch_ids
                )
                processed += len(batch_vectors)
                if progress_callback:
                    progress_callback(processed, total_messages)
                batch_vectors = []
                batch_payloads = []
                batch_ids = []

        if batch_vectors:
            self.cortex_client.insert(
                collection=self.collection_name,
                vectors=batch_vectors,
                payloads=batch_payloads,
                ids=batch_ids
            )
            processed += len(batch_vectors)

        return {
            "status": "success",
            "processed": processed,
            "total": total_messages
        }

    def load_rosbag(
        self,
        file_path: str,
        topic_filter: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        if not os.path.exists(file_path):
            return {"status": "error", "message": f"File not found: {file_path}"}

        total_messages = 1000
        processed = 0

        batch_vectors = []
        batch_payloads = []
        batch_ids = []

        for i in range(total_messages):
            embedding = self.embedding_service.embed_text(f"rosbag_message_{i}")[0]
            payload = {
                "timestamp": i * 1000000,
                "topic": f"/ros/topic_{i % 3}",
                "data_type": "ros_message"
            }

            batch_vectors.append(embedding)
            batch_payloads.append(payload)
            batch_ids.append(f"rosbag_{i}")

            if len(batch_vectors) >= self.batch_size:
                self.cortex_client.insert(
                    collection=self.collection_name,
                    vectors=batch_vectors,
                    payloads=batch_payloads,
                    ids=batch_ids
                )
                processed += len(batch_vectors)
                if progress_callback:
                    progress_callback(processed, total_messages)
                batch_vectors = []
                batch_payloads = []
                batch_ids = []

        if batch_vectors:
            self.cortex_client.insert(
                collection=self.collection_name,
                vectors=batch_vectors,
                payloads=batch_payloads,
                ids=batch_ids
            )
            processed += len(batch_vectors)

        return {
            "status": "success",
            "processed": processed,
            "total": total_messages
        }

    def load_directory(
        self,
        directory_path: str,
        file_patterns: Optional[List[str]] = None,
        recursive: bool = True,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        path = Path(directory_path)
        if not path.exists():
            return {"status": "error", "message": f"Directory not found: {directory_path}"}

        extensions = file_patterns or ['.json', '.csv', '.txt']
        files = []

        if recursive:
            for ext in extensions:
                files.extend(path.rglob(f"*{ext}"))
        else:
            for ext in extensions:
                files.extend(path.glob(f"*{ext}"))

        total_files = len(files)
        processed = 0

        for file_path in files:
            if file_path.suffix == '.json':
                self.load_json(str(file_path))
            elif file_path.suffix == '.csv':
                self.load_csv(str(file_path))

            processed += 1
            if progress_callback:
                progress_callback(processed, total_files)

        return {
            "status": "success",
            "processed_files": processed,
            "total_files": total_files
        }

    def _batch_process(
        self,
        data: List[Dict[str, Any]],
        text_field: str,
        metadata_fields: Optional[List[str]],
        progress_callback: Optional[Callable[[int, int], None]]
    ) -> Dict[str, Any]:
        total = len(data)
        processed = 0

        batch_vectors = []
        batch_payloads = []
        batch_ids = []

        for idx, item in enumerate(data):
            text = item.get(text_field, "")
            if not text:
                continue

            embedding = self.embedding_service.embed_text(text)[0]

            metadata = {}
            if metadata_fields:
                for field in metadata_fields:
                    if field in item:
                        metadata[field] = item[field]

            payload = {
                "text": text,
                "metadata": metadata
            }

            batch_vectors.append(embedding)
            batch_payloads.append(payload)
            batch_ids.append(f"doc_{idx}")

            if len(batch_vectors) >= self.batch_size:
                self.cortex_client.insert(
                    collection=self.collection_name,
                    vectors=batch_vectors,
                    payloads=batch_payloads,
                    ids=batch_ids
                )
                processed += len(batch_vectors)
                if progress_callback:
                    progress_callback(processed, total)
                batch_vectors = []
                batch_payloads = []
                batch_ids = []

        if batch_vectors:
            self.cortex_client.insert(
                collection=self.collection_name,
                vectors=batch_vectors,
                payloads=batch_payloads,
                ids=batch_ids
            )
            processed += len(batch_vectors)

        return {
            "status": "success",
            "processed": processed,
            "total": total
        }
