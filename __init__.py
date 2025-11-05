"""
mapreduce_framework Framework

一个完整的MapReduce框架实现，支持并行处理和分布式计算模拟。
"""

__version__ = "1.0.0"
__author__ = "mapreduce_framework Framework Team"

from .core.mapreduce import MapReduce
from .core.distributed import DistributedMapReduce
from .examples.word_count import word_count_mapper, word_count_reducer
from .examples.inverted_index import inverted_index_mapper, inverted_index_reducer

__all__ = [
    'MapReduce',
    'DistributedMapReduce',
    'word_count_mapper',
    'word_count_reducer',
    'inverted_index_mapper',
    'inverted_index_reducer',
]