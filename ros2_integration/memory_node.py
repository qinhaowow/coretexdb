from typing import Any, Dict, List, Optional
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy


class MemoryNode(Node):
    def __init__(
        self,
        node_name: str = "memory_node",
        cortex_client=None,
        memory_manager=None,
        qos_depth: int = 10
    ):
        super().__init__(node_name)
        self.cortex_client = cortex_client
        self.memory_manager = memory_manager

        qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            history=HistoryPolicy.KEEP_LAST,
            depth=qos_depth
        )

        self.query_service = None
        self.sensor_subscribers = {}

        self.get_logger().info(f"MemoryNode '{node_name}' initialized")

    def set_query_service(self, query_service):
        self.query_service = query_service

    def add_sensor_subscriber(self, topic_name: str, subscriber):
        self.sensor_subscribers[topic_name] = subscriber
        self.get_logger().info(f"Added sensor subscriber for topic: {topic_name}")

    def store_memory(
        self,
        content: Any,
        memory_type: str = "episodic",
        importance: float = 0.5,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        if self.memory_manager:
            memory_id = self.memory_manager.add_memory(
                content=content,
                memory_type=memory_type,
                importance=importance,
                metadata=metadata
            )
            self.get_logger().debug(f"Stored memory: {memory_id}")
            return memory_id

        if self.cortex_client:
            embedding = None
            if isinstance(content, str):
                from .embedding_service import EmbeddingService
                embedding_service = EmbeddingService()
                embedding = embedding_service.embed_text(content)[0]

            result = self.cortex_client.insert(
                collection="robot_memory",
                vectors=[embedding] if embedding else [[0.0] * 384],
                payloads=[{"content": content, "metadata": metadata or {}}]
            )
            return result.get("ids", ["unknown"])[0] if result.get("ids") else "unknown"

        return ""

    def retrieve_memories(
        self,
        query: str,
        limit: int = 10,
        memory_types: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        if self.memory_manager:
            return self.memory_manager.retrieve(
                query=query,
                limit=limit,
                memory_types=memory_types
            )

        if self.cortex_client:
            from .embedding_service import EmbeddingService
            embedding_service = EmbeddingService()
            query_embedding = embedding_service.embed_text(query)[0]

            results = self.cortex_client.search(
                collection="robot_memory",
                query_vector=query_embedding,
                limit=limit
            )
            return results

        return []

    def query_by_time_range(
        self,
        start_time,
        end_time,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        if self.memory_manager:
            return self.memory_manager.retrieve(
                query="",
                limit=limit,
                time_range=(start_time, end_time)
            )

        return []

    def get_node_status(self) -> Dict[str, Any]:
        status = {
            "node_name": self.get_name(),
            "topics_subscribed": list(self.sensor_subscribers.keys()),
            "query_service_active": self.query_service is not None
        }

        if self.memory_manager:
            stats = self.memory_manager.get_stats()
            status.update(stats)

        return status

    def shutdown(self):
        self.get_logger().info("Shutting down MemoryNode")
        for topic, subscriber in self.sensor_subscribers.items():
            try:
                self.destroy_subscription(subscriber)
            except:
                pass
        self.sensor_subscribers.clear()
