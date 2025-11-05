#!/usr/bin/env python3
"""
MapReduce框架使用演示
放在项目根目录下运行
"""

import sys
import os
import time

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.mapreduce import MapReduce
from core.distributed import DistributedMapReduce
from examples.word_count import word_count_mapper, word_count_reducer
from examples.inverted_index import inverted_index_mapper, inverted_index_reducer


def word_count_demo():
    """词频统计演示"""
    print("=" * 60)
    print("词频统计示例")
    print("=" * 60)

    # 测试数据
    documents = [
        "hello world",
        "hello mapreduce",
        "mapreduce is powerful",
        "hello from the other side",
        "big data processing with mapreduce",
        "world of data",
        "hello big data world"
    ]

    # 使用基本MapReduce
    print("\n1. 基本MapReduce版本:")
    mr = MapReduce(num_workers=2)
    results = mr.run(documents, word_count_mapper, word_count_reducer)

    print("词频统计结果:")
    for word, count in sorted(results.items(), key=lambda x: x[1], reverse=True):
        print(f"  {word}: {count}")

    # 使用分布式版本
    print("\n2. 分布式MapReduce版本:")
    dmr = DistributedMapReduce(num_mappers=2, num_reducers=2)
    distributed_results = dmr.simulate_distributed_execution(
        documents, word_count_mapper, word_count_reducer
    )

    print("分布式词频统计结果:")
    for word, count in sorted(distributed_results.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {word}: {count}")


def inverted_index_demo():
    """倒排索引演示"""
    print("\n" + "=" * 60)
    print("倒排索引示例")
    print("=" * 60)

    # 测试数据
    documents_with_id = [
        (1, "apple banana orange"),
        (2, "banana cherry apple"),
        (3, "orange peach apple"),
        (4, "banana apple grape")
    ]

    mr = MapReduce(num_workers=2)
    results = mr.run(documents_with_id, inverted_index_mapper, inverted_index_reducer)

    print("倒排索引结果:")
    for word, doc_ids in sorted(results.items()):
        print(f"  {word}: {sorted(doc_ids)}")


def performance_demo():
    """性能测试演示"""
    print("\n" + "=" * 60)
    print("性能测试")
    print("=" * 60)

    # 创建大数据集
    large_data = ["hello world mapreduce big data " * 10] * 500
    print(f"测试数据量: {len(large_data)} 条记录")

    print("\n不同worker数量的性能对比:")
    for workers in [1, 2, 4, 8]:
        mr = MapReduce(num_workers=workers)
        start = time.time()
        results = mr.run(large_data, word_count_mapper, word_count_reducer)
        end = time.time()
        print(f"  Workers: {workers}, 耗时: {end - start:.3f}秒, 唯一单词数: {len(results)}")


def disk_storage_demo():
    """磁盘存储演示"""
    print("\n" + "=" * 60)
    print("磁盘存储模式演示")
    print("=" * 60)

    documents = [
        "hello world",
        "hello mapreduce",
        "mapreduce is powerful"
    ]

    # 使用磁盘存储模式
    mr_disk = MapReduce(num_workers=2, use_disk_storage=True, temp_dir="./temp_demo")
    results = mr_disk.run(documents, word_count_mapper, word_count_reducer)

    print("磁盘存储模式结果:")
    for word, count in sorted(results.items()):
        print(f"  {word}: {count}")

    # 清理临时文件
    if hasattr(mr_disk, 'file_manager'):
        mr_disk.file_manager.cleanup()


def custom_example_demo():
    """自定义MapReduce示例"""
    print("\n" + "=" * 60)
    print("自定义示例：计算平均值")
    print("=" * 60)

    # 销售数据： (类别, 销售额)
    sales_data = [
        ("electronics", 1000),
        ("clothing", 500),
        ("electronics", 1500),
        ("clothing", 300),
        ("books", 200),
        ("electronics", 800),
        ("clothing", 600)
    ]

    def average_mapper(record):
        """平均值计算的Map函数"""
        category, value = record
        yield (category, (value, 1))  # (值, 计数)

    def average_reducer(key, values):
        """平均值计算的Reduce函数"""
        total = 0
        count = 0
        for value, cnt in values:
            total += value
            count += cnt
        return total / count if count > 0 else 0

    mr = MapReduce(num_workers=2)
    results = mr.run(sales_data, average_mapper, average_reducer)

    print("各类别平均销售额:")
    for category, avg in sorted(results.items()):
        print(f"  {category}: ${avg:.2f}")


def main():
    """主演示函数"""
    print("MapReduce框架完整演示")
    print("本项目演示了完整的MapReduce计算原理和过程")
    print()

    # 运行各个演示
    word_count_demo()
    inverted_index_demo()
    performance_demo()
    disk_storage_demo()
    custom_example_demo()

    print("\n" + "=" * 60)
    print("演示完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()