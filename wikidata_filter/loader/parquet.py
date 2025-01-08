"""
读取parquet格式的文件 按照行进行输出

@dependencies pyarrow
"""
from typing import Iterable, Any

from wikidata_filter.loader.file import BinaryFile

try:
    import pyarrow.parquet as pq
except:
    print("Error! you need to install pyarrow first: pip install pyarrow")
    raise ImportError("pyarrow not installed")


class Parquet(BinaryFile):
    """基于pyarrow读取parquet文件"""
    def __init__(self, input_file: str, **kwargs):
        super().__init__(input_file, auto_open=False)

    def iter(self) -> Iterable[Any]:
        table = pq.read_table(self.input_file)
        for batch in table.to_batches():
            for row in batch.to_pylist():
                yield row
