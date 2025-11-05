import os
import pickle
from typing import Any
from utils.logger import get_logger


class FileManager:
    """文件管理器，用于处理中间结果的磁盘存储"""

    def __init__(self, base_dir: str = "./temp_mapreduce"):
        self.base_dir = base_dir
        self.logger = get_logger("FileManager")
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        """确保目录存在"""
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
            self.logger.info(f"创建目录: {self.base_dir}")

    def save_data(self, data: Any, filename: str) -> str:
        """
        保存数据到文件

        Args:
            data: 要保存的数据
            filename: 文件名

        Returns:
            文件路径
        """
        filepath = os.path.join(self.base_dir, filename)
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)
            self.logger.debug(f"数据已保存到: {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
            raise

    def load_data(self, filename: str) -> Any:
        """
        从文件加载数据

        Args:
            filename: 文件名

        Returns:
            加载的数据
        """
        filepath = os.path.join(self.base_dir, filename)
        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
            self.logger.debug(f"从文件加载数据: {filepath}")
            return data
        except Exception as e:
            self.logger.error(f"加载数据失败: {e}")
            raise

    def cleanup(self):
        """清理临时文件"""
        try:
            for filename in os.listdir(self.base_dir):
                filepath = os.path.join(self.base_dir, filename)
                os.remove(filepath)
            self.logger.info(f"已清理临时目录: {self.base_dir}")
        except Exception as e:
            self.logger.error(f"清理临时文件失败: {e}")