import hashlib
from typing import Any


class Partitioner:
    """数据分区器"""

    def __init__(self, num_partitions: int):
        self.num_partitions = num_partitions

    def get_partition(self, key: Any) -> str:
        """
        根据key获取分区

        Args:
            key: 键值

        Returns:
            分区标识符
        """
        key_str = str(key)
        partition_id = int(hashlib.md5(key_str.encode()).hexdigest()[:8], 16) % self.num_partitions
        return f"part_{partition_id}"

    def get_reducer_for_key(self, key: Any) -> int:
        """获取处理指定key的reducer ID"""
        key_str = str(key)
        return int(hashlib.md5(key_str.encode()).hexdigest()[:8], 16) % self.num_partitions