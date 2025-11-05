import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Any, Dict
import hashlib

from storage.file_manager import FileManager
from storage.data_serializer import DataSerializer
from utils.logger import get_logger
from core.partitioner import Partitioner


class MapReduce:
    """MapReduce框架核心类"""

    def __init__(self, num_workers: int = 4, temp_dir: str = "./temp_mapreduce",
                 use_disk_storage: bool = False):
        """
        初始化MapReduce框架

        Args:
            num_workers: worker线程数量
            temp_dir: 临时文件目录
            use_disk_storage: 是否使用磁盘存储中间结果
        """
        self.num_workers = num_workers
        self.temp_dir = temp_dir
        self.use_disk_storage = use_disk_storage
        self.partitioner = Partitioner(num_workers)
        self.logger = get_logger("mapreduce_framework")

        if use_disk_storage:
            self.file_manager = FileManager(temp_dir)
            self.serializer = DataSerializer()

        self._create_temp_dir()

    def _create_temp_dir(self):
        """创建临时目录"""
        import os
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

    def map_phase(self, mapper: Callable, data: List[Any]) -> Dict[str, List]:
        """
        Map阶段：将输入数据转换为键值对
        """
        self.logger.info("开始Map阶段...")
        intermediate = defaultdict(list)

        def process_chunk(chunk_id, chunk):
            """处理数据块"""
            local_intermediate = defaultdict(list)
            for item in chunk:
                try:
                    for key, value in mapper(item):
                        partition_key = self.partitioner.get_partition(key)
                        local_intermediate[partition_key].append((key, value))
                except Exception as e:
                    self.logger.error(f"Map处理错误: {e}")

            # 根据配置选择存储方式
            if self.use_disk_storage:
                filename = f"map_output_{chunk_id}.pkl"
                self.file_manager.save_data(local_intermediate, filename)
                return filename
            else:
                return local_intermediate

        # 将数据分块
        chunk_size = max(1, len(data) // self.num_workers)
        chunks = [(i, data[i:i + chunk_size]) for i in range(0, len(data), chunk_size)]

        # 并行执行map任务
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            results = list(executor.map(lambda x: process_chunk(x[0], x[1]), chunks))

        # 合并结果
        if self.use_disk_storage:
            # 从磁盘加载并合并
            for filename in results:
                chunk_data = self.file_manager.load_data(filename)
                for partition, kvs in chunk_data.items():
                    intermediate[partition].extend(kvs)
        else:
            # 内存中合并
            for result in results:
                for partition, kvs in result.items():
                    intermediate[partition].extend(kvs)

        map_count = sum(len(v) for v in intermediate.values())
        self.logger.info(f"Map阶段完成，生成 {map_count} 个中间键值对")
        return intermediate

    def shuffle_phase(self, intermediate: Dict[str, List]) -> Dict[Any, List]:
        """
        Shuffle阶段：按键分组，为Reduce阶段准备数据
        """
        self.logger.info("开始Shuffle阶段...")
        grouped_data = defaultdict(list)

        # 收集所有键值对并按key分组
        for partition_data in intermediate.values():
            for key, value in partition_data:
                grouped_data[key].append(value)

        self.logger.info(f"Shuffle阶段完成，生成 {len(grouped_data)} 个不同的key")
        return grouped_data

    def reduce_phase(self, reducer: Callable, grouped_data: Dict[Any, List]) -> Dict[Any, Any]:
        """
        Reduce阶段：对每个key的所有value进行归约
        """
        self.logger.info("开始Reduce阶段...")
        results = {}

        def process_group(key_values):
            """处理一个key的所有values"""
            key, values = key_values
            try:
                result = reducer(key, values)
                return key, result
            except Exception as e:
                self.logger.error(f"Reduce处理错误 key={key}: {e}")
                return key, None

        # 并行执行reduce任务
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            reduce_results = list(executor.map(process_group, grouped_data.items()))

        # 收集结果
        for key, result in reduce_results:
            if result is not None:
                results[key] = result

        self.logger.info(f"Reduce阶段完成，生成 {len(results)} 个最终结果")
        return results

    def run(self, data: List[Any], mapper: Callable, reducer: Callable) -> Dict[Any, Any]:
        """
        执行完整的MapReduce作业
        """
        self.logger.info(f"开始MapReduce作业，数据量: {len(data)}, Workers: {self.num_workers}")
        start_time = time.time()

        # 1. Map阶段
        intermediate = self.map_phase(mapper, data)

        # 2. Shuffle阶段
        grouped_data = self.shuffle_phase(intermediate)

        # 3. Reduce阶段
        results = self.reduce_phase(reducer, grouped_data)

        end_time = time.time()
        self.logger.info(f"MapReduce作业完成，耗时: {end_time - start_time:.2f}秒")

        return results