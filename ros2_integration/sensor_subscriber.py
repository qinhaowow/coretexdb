from typing import Any, Callable, Dict, List, Optional
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy


class SensorSubscriber(Node):
    def __init__(
        self,
        node_name: str,
        cortex_client,
        embedding_service,
        topic_name: str,
        message_type: str,
        qos_depth: int = 10,
        store_data: bool = True,
        preprocess_callback: Optional[Callable] = None
    ):
        super().__init__(node_name)
        self.cortex_client = cortex_client
        self.embedding_service = embedding_service
        self.topic_name = topic_name
        self.message_type = message_type
        self.store_data = store_data
        self.preprocess_callback = preprocess_callback

        self.message_count = 0
        self.subscription = None

        qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            history=HistoryPolicy.KEEP_LAST,
            depth=qos_depth
        )

        self._create_subscription(topic_name, message_type, qos)
        self.get_logger().info(f"SensorSubscriber initialized for topic: {topic_name}")

    def _create_subscription(self, topic_name: str, message_type: str, qos):
        try:
            if message_type == "sensor_msgs/PointCloud2":
                from sensor_msgs.msg import PointCloud2
                self.subscription = self.create_subscription(
                    PointCloud2,
                    topic_name,
                    self._pointcloud_callback,
                    qos
                )
            elif message_type == "sensor_msgs/Image":
                from sensor_msgs.msg import Image
                self.subscription = self.create_subscription(
                    Image,
                    topic_name,
                    self._image_callback,
                    qos
                )
            elif message_type == "sensor_msgs/LaserScan":
                from sensor_msgs.msg import LaserScan
                self.subscription = self.create_subscription(
                    LaserScan,
                    topic_name,
                    self._laserscan_callback,
                    qos
                )
            elif message_type == "sensor_msgs/Imu":
                from sensor_msgs.msg import Imu
                self.subscription = self.create_subscription(
                    Imu,
                    topic_name,
                    self._imu_callback,
                    qos
                )
            elif message_type == "nav_msgs/Odometry":
                from nav_msgs.msg import Odometry
                self.subscription = self.create_subscription(
                    Odometry,
                    topic_name,
                    self._odometry_callback,
                    qos
                )
            elif message_type == "geometry_msgs/Pose":
                from geometry_msgs.msg import Pose
                self.subscription = self.create_subscription(
                    Pose,
                    topic_name,
                    self._pose_callback,
                    qos
                )
            elif message_type == "std_msgs/String":
                from std_msgs.msg import String
                self.subscription = self.create_subscription(
                    String,
                    topic_name,
                    self._string_callback,
                    qos
                )
            else:
                self.get_logger().warn(f"Unsupported message type: {message_type}")
        except Exception as e:
            self.get_logger().error(f"Failed to create subscription: {e}")

    def _pointcloud_callback(self, msg):
        self.message_count += 1

        processed_data = {
            "type": "pointcloud",
            "timestamp": msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9,
            "frame_id": msg.header.frame_id,
            "width": msg.width,
            "height": msg.height,
            "point_step": msg.point_step,
            "row_step": msg.row_step
        }

        if self.preprocess_callback:
            processed_data = self.preprocess_callback(msg)

        if self.store_data:
            self._store_sensor_data(processed_data)

    def _image_callback(self, msg):
        self.message_count += 1

        processed_data = {
            "type": "image",
            "timestamp": msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9,
            "frame_id": msg.header.frame_id,
            "height": msg.height,
            "width": msg.width,
            "encoding": msg.encoding,
            "is_bigendian": msg.is_bigendian
        }

        if self.preprocess_callback:
            processed_data = self.preprocess_callback(msg)

        if self.store_data:
            self._store_sensor_data(processed_data)

    def _laserscan_callback(self, msg):
        self.message_count += 1

        processed_data = {
            "type": "laserscan",
            "timestamp": msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9,
            "frame_id": msg.header.frame_id,
            "angle_min": msg.angle_min,
            "angle_max": msg.angle_max,
            "range_min": msg.range_min,
            "range_max": msg.range_max,
            "ranges_count": len(msg.ranges)
        }

        if self.preprocess_callback:
            processed_data = self.preprocess_callback(msg)

        if self.store_data:
            self._store_sensor_data(processed_data)

    def _imu_callback(self, msg):
        self.message_count += 1

        processed_data = {
            "type": "imu",
            "timestamp": msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9,
            "frame_id": msg.header.frame_id,
            "has_orientation": hasattr(msg, 'orientation') and not all(v == 0 for v in [msg.orientation.x, msg.orientation.y, msg.orientation.z, msg.orientation.w])
        }

        if self.preprocess_callback:
            processed_data = self.preprocess_callback(msg)

        if self.store_data:
            self._store_sensor_data(processed_data)

    def _odometry_callback(self, msg):
        self.message_count += 1

        processed_data = {
            "type": "odometry",
            "timestamp": msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9,
            "frame_id": msg.header.frame_id,
            "child_frame_id": msg.child_frame_id
        }

        if self.preprocess_callback:
            processed_data = self.preprocess_callback(msg)

        if self.store_data:
            self._store_sensor_data(processed_data)

    def _pose_callback(self, msg):
        self.message_count += 1

        processed_data = {
            "type": "pose",
            "timestamp": 0,
            "frame_id": "unknown"
        }

        if hasattr(msg, 'header'):
            processed_data["timestamp"] = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9
            processed_data["frame_id"] = msg.header.frame_id

        if self.preprocess_callback:
            processed_data = self.preprocess_callback(msg)

        if self.store_data:
            self._store_sensor_data(processed_data)

    def _string_callback(self, msg):
        self.message_count += 1

        processed_data = {
            "type": "string",
            "data": msg.data,
            "timestamp": 0
        }

        if hasattr(msg, 'header'):
            processed_data["timestamp"] = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9

        if self.preprocess_callback:
            processed_data = self.preprocess_callback(msg)

        if self.store_data:
            self._store_sensor_data(processed_data)

    def _store_sensor_data(self, data: Dict[str, Any]):
        text_representation = str(data)
        embedding = self.embedding_service.embed_text(text_representation)[0]

        payload = {
            "topic": self.topic_name,
            "message_type": self.message_type,
            "data": data
        }

        self.cortex_client.insert(
            collection="sensor_data",
            vectors=[embedding],
            payloads=[payload],
            ids=[f"sensor_{self.topic_name}_{self.message_count}"]
        )

    def get_stats(self) -> Dict[str, Any]:
        return {
            "topic": self.topic_name,
            "message_type": self.message_type,
            "message_count": self.message_count,
            "store_data": self.store_data
        }

    def stop(self):
        if self.subscription:
            self.destroy_subscription(self.subscription)
            self.subscription = None
        self.get_logger().info(f"Stopped subscription for topic: {self.topic_name}")
