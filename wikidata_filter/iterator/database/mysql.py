from wikidata_filter.iterator.buffer import BufferedWriter
from wikidata_filter.util.database.mysql_ops import MySQLOps


class MySQL(BufferedWriter):
    def __init__(self, host: str = 'localhost',
                 port: int = 3306,
                 user="root",
                 password="",
                 database=None,
                 table=None,
                 buffer_size= 1000,
                 mode: str = "insert", **kwargs):
        super().__init__(buffer_size=buffer_size)
        self.table = table
        self.mysql_ops = MySQLOps(host=host, port=port, username=user, password=password, database=database)
        self.mode = mode or 'insert'

    def write_batch(self, rows: list):
        n, _ = self.mysql_ops.insert_many(self.table, rows, mode=self.mode)
        print(f"Write {n} rows to table[{self.table}]")
