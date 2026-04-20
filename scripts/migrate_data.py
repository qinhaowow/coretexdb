import json
import argparse
import sys
import os
from typing import Any, Dict, List, Optional
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class DataMigrationTool:
    def __init__(
        self,
        source_client,
        target_client,
        embedding_service=None
    ):
        self.source_client = source_client
        self.target_client = target_client
        self.embedding_service = embedding_service

    def migrate_collection(
        self,
        collection_name: str,
        target_collection: Optional[str] = None,
        batch_size: int = 100,
        progress_callback=None
    ) -> Dict[str, Any]:
        if target_collection is None:
            target_collection = collection_name

        print(f"Migrating collection: {collection_name} -> {target_collection}")

        self.target_client.create_collection(
            name=target_collection,
            vector_size=self.embedding_service.get_dimension() if self.embedding_service else 384
        )

        total_migrated = 0
        offset = 0
        batch_num = 0

        while True:
            results = self.source_client.get(
                collection=collection_name,
                limit=batch_size
            )

            if not results:
                break

            vectors = []
            payloads = []
            ids = []

            for result in results:
                vector = result.get("vector", [0.0] * 384)
                payload = result.get("payload", {})
                doc_id = result.get("id", f"migrated_{offset}_{total_migrated}")

                vectors.append(vector)
                payloads.append(payload)
                ids.append(doc_id)

            if vectors and self.embedding_service:
                text_contents = [p.get("text", "") for p in payloads]
                new_vectors = self.embedding_service.embed_text(text_contents)
                vectors = new_vectors

            self.target_client.insert(
                collection=target_collection,
                vectors=vectors,
                payloads=payloads,
                ids=ids
            )

            total_migrated += len(results)
            offset += len(results)
            batch_num += 1

            if progress_callback:
                progress_callback(batch_num, total_migrated, len(results))

            print(f"Batch {batch_num}: Migrated {len(results)} documents (total: {total_migrated})")

            if len(results) < batch_size:
                break

        return {
            "status": "success",
            "source_collection": collection_name,
            "target_collection": target_collection,
            "total_migrated": total_migrated,
            "batches": batch_num
        }

    def migrate_all_collections(
        self,
        prefix: str = "migrated_",
        batch_size: int = 100
    ) -> Dict[str, Any]:
        collections = self.source_client.get_collections()

        results = {
            "timestamp": datetime.now().isoformat(),
            "total_collections": len(collections),
            "collections": {}
        }

        for collection in collections:
            target = f"{prefix}{collection}"
            result = self.migrate_collection(
                collection_name=collection,
                target_collection=target,
                batch_size=batch_size
            )
            results["collections"][collection] = result

        return results

    def export_collection(
        self,
        collection_name: str,
        output_file: str,
        batch_size: int = 100
    ) -> Dict[str, Any]:
        print(f"Exporting collection: {collection_name} -> {output_file}")

        all_documents = []
        offset = 0

        while True:
            results = self.source_client.get(
                collection=collection_name,
                limit=batch_size
            )

            if not results:
                break

            all_documents.extend(results)
            offset += len(results)

            if len(results) < batch_size:
                break

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_documents, f, indent=2, ensure_ascii=False)

        return {
            "status": "success",
            "collection": collection_name,
            "output_file": output_file,
            "total_documents": len(all_documents)
        }

    def import_collection(
        self,
        input_file: str,
        collection_name: str,
        batch_size: int = 100,
        progress_callback=None
    ) -> Dict[str, Any]:
        print(f"Importing collection: {input_file} -> {collection_name}")

        with open(input_file, 'r', encoding='utf-8') as f:
            documents = json.load(f)

        if not isinstance(documents, list):
            documents = [documents]

        self.target_client.create_collection(
            name=collection_name,
            vector_size=self.embedding_service.get_dimension() if self.embedding_service else 384
        )

        total_imported = 0
        batch_num = 0

        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]

            vectors = []
            payloads = []
            ids = []

            for doc in batch:
                vector = doc.get("vector", [0.0] * 384)
                payload = doc.get("payload", {})
                doc_id = doc.get("id", f"imported_{i}_{total_imported}")

                vectors.append(vector)
                payloads.append(payload)
                ids.append(doc_id)

            if vectors and self.embedding_service:
                text_contents = [p.get("text", "") for p in payloads]
                new_vectors = self.embedding_service.embed_text(text_contents)
                vectors = new_vectors

            self.target_client.insert(
                collection=collection_name,
                vectors=vectors,
                payloads=payloads,
                ids=ids
            )

            total_imported += len(batch)
            batch_num += 1

            if progress_callback:
                progress_callback(batch_num, total_imported, len(batch))

            print(f"Batch {batch_num}: Imported {len(batch)} documents (total: {total_imported})")

        return {
            "status": "success",
            "input_file": input_file,
            "collection": collection_name,
            "total_imported": total_imported,
            "batches": batch_num
        }

    def verify_migration(
        self,
        source_collection: str,
        target_collection: str
    ) -> Dict[str, Any]:
        source_results = self.source_client.get(
            collection=source_collection,
            limit=10000
        )
        target_results = self.target_client.get(
            collection=target_collection,
            limit=10000
        )

        source_count = len(source_results)
        target_count = len(target_results)

        return {
            "source_collection": source_collection,
            "target_collection": target_collection,
            "source_count": source_count,
            "target_count": target_count,
            "match": source_count == target_count,
            "difference": abs(source_count - target_count)
        }


def main():
    from core.cortex_client import CortexClient
    from core.embedding_service import EmbeddingService

    parser = argparse.ArgumentParser(description="Data Migration Tool")
    parser.add_argument("command", choices=["migrate", "export", "import", "verify"])
    parser.add_argument("--source-host", default="localhost", help="Source CortexDB host")
    parser.add_argument("--source-port", type=int, default=5000, help="Source CortexDB port")
    parser.add_argument("--target-host", default="localhost", help="Target CortexDB host")
    parser.add_argument("--target-port", type=int, default=5001, help="Target CortexDB port")
    parser.add_argument("--collection", help="Collection name")
    parser.add_argument("--target-collection", help="Target collection name")
    parser.add_argument("--input", help="Input file (for import)")
    parser.add_argument("--output", help="Output file (for export)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size")
    parser.add_argument("--prefix", default="migrated_", help="Prefix for migrated collections")

    args = parser.parse_args()

    source_client = CortexClient(host=args.source_host, port=args.source_port)
    source_client.connect()

    target_client = CortexClient(host=args.target_host, port=args.target_port)
    target_client.connect()

    embedding_service = EmbeddingService()
    embedding_service.load_model()

    migrator = DataMigrationTool(
        source_client=source_client,
        target_client=target_client,
        embedding_service=embedding_service
    )

    if args.command == "migrate":
        if not args.collection:
            print("Error: --collection is required for migrate command")
            sys.exit(1)

        result = migrator.migrate_collection(
            collection_name=args.collection,
            target_collection=args.target_collection,
            batch_size=args.batch_size
        )
        print(json.dumps(result, indent=2))

    elif args.command == "export":
        if not args.collection or not args.output:
            print("Error: --collection and --output are required for export command")
            sys.exit(1)

        result = migrator.export_collection(
            collection_name=args.collection,
            output_file=args.output,
            batch_size=args.batch_size
        )
        print(json.dumps(result, indent=2))

    elif args.command == "import":
        if not args.input or not args.collection:
            print("Error: --input and --collection are required for import command")
            sys.exit(1)

        result = migrator.import_collection(
            input_file=args.input,
            collection_name=args.collection,
            batch_size=args.batch_size
        )
        print(json.dumps(result, indent=2))

    elif args.command == "verify":
        if not args.collection or not args.target_collection:
            print("Error: --collection and --target-collection are required for verify command")
            sys.exit(1)

        result = migrator.verify_migration(
            source_collection=args.collection,
            target_collection=args.target_collection
        )
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
