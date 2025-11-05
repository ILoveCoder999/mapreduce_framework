from typing import Iterator, Tuple, Any, List
from core.mapreduce import MapReduce
from core.distributed import DistributedMapReduce


def word_count_mapper(document: str) -> Iterator[Tuple[str, int]]:
    """
    词频统计的Map函数

    Args:
        document: 文档字符串

    Yields:
        (word, 1) 键值对
    """
    words = document.split()
    for word in words:
        # 简单清理单词
        clean_word = word.strip('.,!?;:"()[]').lower()
        if clean_word:
            yield (clean_word, 1)


def word_count_reducer(key: str, values: List[int]) -> int:
    """
    词频统计的Reduce函数

    Args:
        key: 单词
        values: 计数值列表

    Returns:
        总计数
    """
    return sum(values)


def run_word_count_example():
    """运行词频统计示例"""
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

    print("=" * 50)
    print("词频统计示例")
    print("=" * 50)

    # 使用基本MapReduce
    mr = MapReduce(num_workers=2)
    results = mr.run(documents, word_count_mapper, word_count_reducer)

    print("\n词频统计结果:")
    for word, count in sorted(results.items(), key=lambda x: x[1], reverse=True):
        print(f"  {word}: {count}")

    # 使用分布式版本
    print("\n分布式版本:")
    dmr = DistributedMapReduce(num_mappers=2, num_reducers=2)
    distributed_results = dmr.simulate_distributed_execution(
        documents, word_count_mapper, word_count_reducer
    )

    print("\n分布式词频统计结果:")
    for word, count in sorted(distributed_results.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {word}: {count}")


if __name__ == "__main__":
    run_word_count_example()