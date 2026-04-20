from typing import Any, Dict, List, Optional
import rclpy
from rclpy.node import Node


class QueryService(Node):
    def __init__(
        self,
        node_name: str,
        cortex_client,
        embedding_service,
        service_name: str = "memory_query",
        collection: str = "robot_memory"
    ):
        super().__init__(node_name)
        self.cortex_client = cortex_client
        self.embedding_service = embedding_service
        self.service_name = service_name
        self.collection = collection

        self._create_services()
        self.get_logger().info(f"QueryService initialized with service: {service_name}")

    def _create_services(self):
        try:
            from cortexdb_srv.srv import QueryMemory, QueryByTime, QueryByType
            self.query_memory_srv = self.create_service(
                QueryMemory,
                f"{self.service_name}/query",
                self._handle_query
            )
            self.query_by_time_srv = self.create_service(
                QueryByTime,
                f"{self.service_name}/by_time",
                self._handle_query_by_time
            )
            self.query_by_type_srv = self.create_service(
                QueryByType,
                f"{self.service_name}/by_type",
                self._handle_query_by_type
            )
        except ImportError:
            self.get_logger().warn("CortexDB service types not found, using generic services")

    def _handle_query(self, request, response):
        query_text = request.query
        limit = request.limit if hasattr(request, 'limit') else 10

        query_embedding = self.embedding_service.embed_text(query_text)[0]

        results = self.cortex_client.search(
            collection=self.collection,
            query_vector=query_embedding,
            limit=limit
        )

        response.results = self._convert_to_service_response(results)
        response.success = True
        response.message = f"Found {len(results)} results"

        return response

    def _handle_query_by_time(self, request, response):
        start_time = request.start_time if hasattr(request, 'start_time') else 0
        end_time = request.end_time if hasattr(request, 'end_time') else 0

        all_results = self.cortex_client.get(
            collection=self.collection,
            limit=1000
        )

        filtered_results = []
        for result in all_results:
            payload = result.get("payload", {})
            timestamp = payload.get("timestamp", 0)
            if start_time <= timestamp <= end_time:
                filtered_results.append(result)

        response.results = self._convert_to_service_response(filtered_results)
        response.success = True
        response.message = f"Found {len(filtered_results)} results in time range"

        return response

    def _handle_query_by_type(self, request, response):
        memory_type = request.memory_type if hasattr(request, 'memory_type') else "episodic"

        filter_dict = {"memory_type": memory_type}

        results = self.cortex_client.search(
            collection=self.collection,
            query_vector=[0.0] * 384,
            limit=100,
            filter_dict=filter_dict
        )

        response.results = self._convert_to_service_response(results)
        response.success = True
        response.message = f"Found {len(results)} results of type {memory_type}"

        return response

    def _convert_to_service_response(self, results: List[Dict[str, Any]]) -> List:
        from std_msgs.msg import String
        service_results = []
        for result in results:
            msg = String()
            msg.data = str(result)
            service_results.append(msg)
        return service_results

    def query(
        self,
        query_text: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        query_embedding = self.embedding_service.embed_text(query_text)[0]

        results = self.cortex_client.search(
            collection=self.collection,
            query_vector=query_embedding,
            limit=limit,
            filter_dict=filters
        )

        return results

    def query_by_time(
        self,
        start_time: float,
        end_time: float,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        all_results = self.cortex_client.get(
            collection=self.collection,
            limit=1000
        )

        filtered_results = []
        for result in all_results:
            payload = result.get("payload", {})
            timestamp = payload.get("timestamp", 0)
            if start_time <= timestamp <= end_time:
                filtered_results.append(result)

        return filtered_results[:limit]

    def query_by_type(
        self,
        memory_type: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        filter_dict = {"memory_type": memory_type}

        results = self.cortex_client.search(
            collection=self.collection,
            query_vector=[0.0] * 384,
            limit=limit,
            filter_dict=filter_dict
        )

        return results

    def query_by_metadata(
        self,
        metadata_filters: Dict[str, Any],
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        results = self.cortex_client.search(
            collection=self.collection,
            query_vector=[0.0] * 384,
            limit=limit,
            filter_dict=metadata_filters
        )

        return results

    def get_collection_stats(self) -> Dict[str, Any]:
        all_results = self.cortex_client.get(
            collection=self.collection,
            limit=1000
        )

        memory_types = {}
        for result in all_results:
            payload = result.get("payload", {})
            mem_type = payload.get("memory_type", "unknown")
            memory_types[mem_type] = memory_types.get(mem_type, 0) + 1

        return {
            "collection": self.collection,
            "total_count": len(all_results),
            "memory_types": memory_types
        }

    def shutdown(self):
        self.get_logger().info("Shutting down QueryService")
