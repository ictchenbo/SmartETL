import sqlite3
from .rdb_base import RDBBase


class SQLite(RDBBase):
    """SQLite数据库操作  注意：SQLite不支持管理上下文"""
    def __init__(self, dbpath: str, **kwargs):
        super().__init__(**kwargs)

        self.conn = sqlite3.connect(dbpath)

    def list_tables(self, **kwargs):
        sql = "SELECT name FROM sqlite_master WHERE type='table';"
        cursor = self.conn.execute(sql)
        tables = [row[0] for row in cursor]
        cursor.close()
        return tables

    def desc_table(self, table: str, **kwargs):
        sql = f"PRAGMA table_info({table})"
        cursor = self.conn.execute(sql)
        return list(cursor.fetchall())

    def fetch_cursor(self, query: str):
        cursor = self.conn.execute(query)
        cursor.execute(query)
        col_name_list = [tup[0] for tup in cursor.description]
        for row in cursor:
            yield row if isinstance(row, dict) else dict(zip(col_name_list, row))
        cursor.close()
