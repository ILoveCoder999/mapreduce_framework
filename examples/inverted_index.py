from typing import Iterator, Tuple, Any, List


def inverted_index_mapper(document: Tuple[int, str]) -> Iterator[Tuple[str, int]]:
    """
    倒排索引的Map函数

    Args:
        document: (文档ID, 文档内容) 元组

    Yields:
        (word, doc_id) 键值对
    """
    doc_id, content = document
    words = content.split()
    for word in words:
        clean_word = word.strip('.,!?;:"()[]').lower()
        if clean_word:
            yield (clean_word, doc_id)


def inverted_index_reducer(key: str, values: List[int]) -> List[int]:
    """
    倒排索引的Reduce函数

    Args:
        key: 单词
        values: 文档ID列表

    Returns:
        去重后的文档ID列表
    """
    return list(set(values))  # 去重


def run_inverted_index_example():
    """运行倒排索引示例"""
    # 测试数据
    documents_with_id = [
        (1, "apple banana orange"),
        (2, "banana cherry apple"),
        (3, "orange peach apple"),
        (4, "banana apple grape")
    ]

    from ..core.mapreduce import MapReduce

    print("=" * 50)
    print("倒排索引示例")
    print("=" * 50)

    mr = MapReduce(num_workers=2)
    results = mr.run(documents_with_id, inverted_index_mapper, inverted_index_reducer)

    print("\n倒排索引结果:")
    for word, doc_ids in sorted(results.items()):
        print(f"  {word}: {sorted(doc_ids)}")


if __name__ == "__main__":
    run_inverted_index_example()