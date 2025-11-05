from collections import defaultdict
from typing import Callable, List, Any, Dict
import time

from utils.logger import get_logger
from core.partitioner import Partitioner


class DistributedMapReduce:
    """分布式版本的MapReduce（模拟）"""

    def __init__(self, num_mappers: int = 3, num_reducers: int = 2):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.partitioner = Partitioner(num_reducers)
        self.logger = get_logger("DistributedMapReduce")
        self.mapper_results = []
        self.reducer_results = {}

    def simulate_distributed_execution(self, data: List[Any], mapper: Callable, reducer: Callable) -> Dict[Any, Any]:
        """模拟分布式执行"""
        self.logger.info(f"开始分布式MapReduce模拟: Mappers={self.num_mappers}, Reducers={self.num_reducers}")
        start_time = time.time()

        # 模拟数据分片
        data_shards = self._split_data(data, self.num_mappers)
        self.logger.info(f"数据分片完成: {len(data_shards)} 个分片")

        # Map阶段（在不同节点上并行执行）
        self.logger.info("开始Map阶段...")
        for i, shard in enumerate(data_shards):
            self.logger.info(f"Mapper {i + 1} 处理 {len(shard)} 条数据")
            intermediate = []
            for item in shard:
                for key, value in mapper(item):
                    # 根据key选择reducer
                    reducer_id = self.partitioner.get_reducer_for_key(key)
                    intermediate.append((reducer_id, key, value))
            self.mapper_results.append(intermediate)
            self.logger.info(f"Mapper {i + 1} 生成 {len(intermediate)} 个中间结果")

        # Shuffle阶段（网络传输）
        self.logger.info("开始Shuffle阶段...")
        shuffled_data = self._shuffle_data()

        # Reduce阶段（在不同节点上并行执行）
        self.logger.info("开始Reduce阶段...")
        final_results = {}
        for reducer_id, group_data in shuffled_data.items():
            self.logger.info(f"Reducer {reducer_id + 1} 处理 {len(group_data)} 个key")
            results = {}
            for key, values in group_data.items():
                results[key] = reducer(key, values)
            final_results.update(results)
            self.logger.info(f"Reducer {reducer_id + 1} 生成 {len(results)} 个最终结果")

        end_time = time.time()
        self.logger.info(f"分布式MapReduce完成，耗时: {end_time - start_time:.2f}秒")
        return final_results

    def _split_data(self, data: List[Any], num_shards: int) -> List[List[Any]]:
        """数据分片"""
        shard_size = max(1, len(data) // num_shards)
        return [data[i:i + shard_size] for i in range(0, len(data), shard_size)]

    def _shuffle_data(self) -> Dict[int, Dict[Any, List]]:
        """Shuffle数据到对应的Reducer"""
        shuffled = defaultdict(lambda: defaultdict(list))

        for mapper_result in self.mapper_results:
            for reducer_id, key, value in mapper_result:
                shuffled[reducer_id][key].append(value)

        return dict(shuffled)