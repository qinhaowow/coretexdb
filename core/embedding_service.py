import numpy as np
from typing import List, Union, Optional
from pathlib import Path
from sentence_transformers import SentenceTransformer
import asyncio

class MultimodalEmbeddingService:
    """多模态向量化服务 - 支持文本、图像、点云等"""
    
    def __init__(self,
                 text_model_name: str = "BAAI/bge-small-en-v1.5",
                 image_model_name: str = "openai/clip-vit-base-patch32",
                 use_gpu: bool = True):
        # 文本嵌入模型（BGE系列在边缘端性能优秀）
        self.text_model = SentenceTransformer(text_model_name)
        if use_gpu:
            self.text_model = self.text_model.to("cuda")
        
        # 图像嵌入模型（CLIP 支持图文跨模态检索）
        from transformers import CLIPProcessor, CLIPModel
        self.image_model = CLIPModel.from_pretrained(image_model_name)
        self.image_processor = CLIPProcessor.from_pretrained(image_model_name)
        if use_gpu:
            self.image_model = self.image_model.to("cuda")
        
        self._cache = {}  # 简单内存缓存，避免重复编码
    
    def encode_text(self, text: str) -> List[float]:
        """将文本编码为向量"""
        if text in self._cache:
            return self._cache[text]
        
        embedding = self.text_model.encode(text, normalize_embeddings=True)
        result = embedding.tolist()
        self._cache[text] = result
        return result
    
    def encode_image(self, image_path: Union[str, Path]) -> List[float]:
        """将图像编码为向量"""
        from PIL import Image
        import torch
        
        cache_key = f"img_{image_path}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        image = Image.open(image_path)
        inputs = self.image_processor(images=image, return_tensors="pt")
        
        with torch.no_grad():
            image_features = self.image_model.get_image_features(**inputs)
            # 归一化
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        
        result = image_features.squeeze().cpu().numpy().tolist()
        self._cache[cache_key] = result
        return result
    
    def encode_batch(self, texts: List[str]) -> List[List[float]]:
        """批量编码文本"""
        embeddings = self.text_model.encode(
            texts,
            normalize_embeddings=True,
            show_progress_bar=False,
            batch_size=32
        )
        return embeddings.tolist()
    
    async def encode_sensor_data(self,
                                  sensor_type: str,
                                  data: Union[str, Path, np.ndarray]) -> List[float]:
        """统一的多模态数据向量化入口"""
        if sensor_type == "camera":
            return self.encode_image(data)
        elif sensor_type == "text":
            return self.encode_text(data)
        elif sensor_type == "lidar":
            # LiDAR点云需要专门的编码模型（如PointNet++），这里简化为投影到图像
            # 实际应用中应使用专门的点云编码器
            raise NotImplementedError("LiDAR encoding requires specialized model")
        else:
            raise ValueError(f"Unknown sensor type: {sensor_type}")