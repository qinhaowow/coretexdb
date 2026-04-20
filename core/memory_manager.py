from collections import deque
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import time
import asyncio

@dataclass
class MemoryEntry:
    """记忆条目"""
    id: str
    timestamp: float
    content: str
    vector: List[float]
    importance: float = 1.0  # 重要性权重
    access_count: int = 0    # 访问次数
    
class RobotMemoryManager:
    """机器人记忆管理器 - 实现长短时记忆系统"""
    
    def __init__(self, 
                 cortex_client: 'RobotMemoryClient',
                 embedding_service: 'MultimodalEmbeddingService',
                 stm_capacity: int = 100,
                 stm_ttl_seconds: float = 300.0):
        self.cortex = cortex_client
        self.embedding = embedding_service
        
        # 短期记忆（内存队列）
        self.stm_queue = deque(maxlen=stm_capacity)
        self.stm_ttl = stm_ttl_seconds
        self.stm_index = {}  # ID -> MemoryEntry 快速查找
        
        # 长期记忆（通过CoretexDB持久化）
        self.ltm_consolidation_threshold = 0.7  # 重要性阈值
        
        # 后台任务
        self._consolidation_task: Optional[asyncio.Task] = None
    
    async def add_memory(self,
                          content: str,
                          sensor_type: str = "text",
                          raw_data_path: Optional[str] = None,
                          importance: float = 0.5,
                          metadata: Optional[Dict] = None) -> str:
        """添加新记忆"""
        timestamp = time.time()
        
        # 生成向量
        vector = await self.embedding.encode_sensor_data(sensor_type, content)
        
        # 存入长期记忆（CoretexDB）
        record_id = await self.cortex.store_sensor_memory(
            timestamp=timestamp,
            sensor_type=sensor_type,
            raw_data_path=raw_data_path or "",
            text_description=content,
            scene_vector=vector,
            metadata=metadata or {}
        )
        
        # 同时存入短期记忆（高频访问缓存）
        entry = MemoryEntry(
            id=record_id,
            timestamp=timestamp,
            content=content,
            vector=vector,
            importance=importance
        )
        self.stm_queue.append(entry)
        self.stm_index[record_id] = entry
        
        return record_id
    
    async def recall(self,
                      query: str,
                      use_stm: bool = True,
                      use_ltm: bool = True,
                      limit: int = 10) -> List[MemoryEntry]:
        """回忆相关记忆"""
        results = []
        
        # 1. 短期记忆检索（语义相似度）
        if use_stm and self.stm_queue:
            query_vector = self.embedding.encode_text(query)
            stm_results = self._search_stm(query_vector, limit=limit)
            results.extend(stm_results)
        
        # 2. 长期记忆检索（通过CoretexDB语义搜索）
        if use_ltm:
            ltm_results = await self.cortex.semantic_search(
                query=query,
                limit=limit
            )
            for r in ltm_results:
                entry = MemoryEntry(
                    id=r["id"],
                    timestamp=r["data"].get("timestamp", 0),
                    content=r["data"].get("text_description", ""),
                    vector=r["data"].get("scene_vector", []),
                    importance=r["score"]
                )
                results.append(entry)
        
        # 去重并按重要性排序
        seen_ids = set()
        unique_results = []
        for r in sorted(results, key=lambda x: x.importance, reverse=True):
            if r.id not in seen_ids:
                seen_ids.add(r.id)
                unique_results.append(r)
        
        return unique_results[:limit]
    
    def _search_stm(self, query_vector: List[float], limit: int) -> List[MemoryEntry]:
        """在短期记忆中搜索（余弦相似度）"""
        import numpy as np
        
        query_vec = np.array(query_vector)
        results = []
        
        for entry in self.stm_queue:
            if time.time() - entry.timestamp > self.stm_ttl:
                continue  # 过期记忆跳过
            
            entry_vec = np.array(entry.vector)
            similarity = np.dot(query_vec, entry_vec) / (
                np.linalg.norm(query_vec) * np.linalg.norm(entry_vec) + 1e-8
            )
            entry.importance = similarity * entry.importance
            results.append(entry)
        
        return sorted(results, key=lambda x: x.importance, reverse=True)[:limit]
    
    async def start_consolidation(self, interval_seconds: float = 60.0):
        """启动记忆巩固后台任务"""
        async def consolidate_loop():
            while True:
                await asyncio.sleep(interval_seconds)
                await self._consolidate_stm_to_ltm()
        
        self._consolidation_task = asyncio.create_task(consolidate_loop())
    
    async def _consolidate_stm_to_ltm(self):
        """将重要的短期记忆巩固到长期记忆"""
        for entry in list(self.stm_queue):
            if entry.importance >= self.ltm_consolidation_threshold:
                # 更新重要性分数到数据库
                await self.cortex.client.records.update(
                    collection="robot_memories",
                    record_id=entry.id,
                    data={"importance": entry.importance}
                )
    
    async def stop(self):
        """停止后台任务"""
        if self._consolidation_task:
            self._consolidation_task.cancel()
            try:
                await self._consolidation_task
            except asyncio.CancelledError:
                pass