import asyncio
from typing import List, Dict, Any, Optional
from cortexdb import CortexClient, FieldDefinition, FieldType, StoreLocation

class RobotMemoryClient:
    """CoretexDB 客户端封装，专为机器人记忆场景定制"""
    
    def __init__(self, base_url: str = "http://localhost:8000", 
                 api_key: Optional[str] = None,
                 timeout: float = 60.0):
        self.base_url = base_url
        self.client = CortexClient(base_url, api_key=api_key, timeout=timeout)
        self._collections_initialized = False
    
    async def initialize_collections(self):
        """初始化机器人所需的数据库集合"""
        if self._collections_initialized:
            return
        
        # 创建传感器记忆集合
        try:
            await self.client.collections.create(
                name="robot_memories",
                fields=[
                    FieldDefinition(name="timestamp", type=FieldType.FLOAT),
                    FieldDefinition(name="sensor_type", type=FieldType.STRING),
                    FieldDefinition(name="raw_data_path", type=FieldType.STRING),
                    FieldDefinition(name="text_description", type=FieldType.TEXT, vectorize=True),
                    FieldDefinition(name="scene_vector", type=FieldType.VECTOR, dimension=768),
                    FieldDefinition(name="metadata", type=FieldType.JSON)
                ]
            )
        except Exception:
            # 集合已存在时忽略
            pass
        
        # 创建任务记忆集合（用于长期经验存储）
        try:
            await self.client.collections.create(
                name="task_experiences",
                fields=[
                    FieldDefinition(name="task_id", type=FieldType.STRING),
                    FieldDefinition(name="timestamp", type=FieldType.FLOAT),
                    FieldDefinition(name="success", type=FieldType.BOOLEAN),
                    FieldDefinition(name="context_vector", type=FieldType.VECTOR, dimension=768),
                    FieldDefinition(name="llm_response", type=FieldType.TEXT),
                    FieldDefinition(name="metadata", type=FieldType.JSON)
                ]
            )
        except Exception:
            pass
        
        self._collections_initialized = True
    
    async def store_sensor_memory(self, 
                                   timestamp: float,
                                   sensor_type: str,
                                   raw_data_path: str,
                                   text_description: str,
                                   scene_vector: List[float],
                                   metadata: Dict[str, Any]) -> str:
        """存储单条传感器记忆"""
        await self.initialize_collections()
        
        record = await self.client.records.create(
            collection="robot_memories",
            data={
                "timestamp": timestamp,
                "sensor_type": sensor_type,
                "raw_data_path": raw_data_path,
                "text_description": text_description,
                "scene_vector": scene_vector,
                "metadata": metadata
            }
        )
        return record.id
    
    async def semantic_search(self,
                               query: str,
                               limit: int = 10,
                               filters: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """语义搜索 - 根据自然语言描述检索相关记忆"""
        await self.initialize_collections()
        
        results = await self.client.records.query(
            collection="robot_memories",
            query=query,
            limit=limit,
            filters=filters
        )
        
        return [
            {
                "id": r.id,
                "score": r.score,
                "data": r.data
            }
            for r in results
        ]
    
    async def vector_similarity_search(self,
                                        query_vector: List[float],
                                        limit: int = 10,
                                        filters: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """向量相似度检索 - 直接使用向量进行搜索"""
        await self.initialize_collections()
        
        results = await self.client.records.vector_query(
            collection="robot_memories",
            vector=query_vector,
            limit=limit,
            filters=filters
        )
        
        return [{"id": r.id, "score": r.score, "data": r.data} for r in results]
    
    async def close(self):
        """关闭客户端连接"""
        await self.client.aclose()