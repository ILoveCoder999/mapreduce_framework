import pickle
import json
from typing import Any
from utils.logger import get_logger


class DataSerializer:
    """数据序列化器"""

    def __init__(self):
        self.logger = get_logger("DataSerializer")

    def serialize_pickle(self, data: Any) -> bytes:
        """使用pickle序列化数据"""
        try:
            return pickle.dumps(data)
        except Exception as e:
            self.logger.error(f"Pickle序列化失败: {e}")
            raise

    def deserialize_pickle(self, data: bytes) -> Any:
        """使用pickle反序列化数据"""
        try:
            return pickle.loads(data)
        except Exception as e:
            self.logger.error(f"Pickle反序列化失败: {e}")
            raise

    def serialize_json(self, data: Any) -> str:
        """使用JSON序列化数据"""
        try:
            return json.dumps(data)
        except Exception as e:
            self.logger.error(f"JSON序列化失败: {e}")
            raise

    def deserialize_json(self, data: str) -> Any:
        """使用JSON反序列化数据"""
        try:
            return json.loads(data)
        except Exception as e:
            self.logger.error(f"JSON反序列化失败: {e}")
            raise