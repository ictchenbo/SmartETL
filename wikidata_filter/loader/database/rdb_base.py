from wikidata_filter.loader.base import DataProvider


class RDBBase(DataProvider):
    conn = None

    def __init__(self, database: str = 'default',
                 table: str = None,
                 select: str = "*",
                 where: str = None,
                 limit: str = None,
                 batch_size: int = 1000,
                 paging: bool = True, **kwargs):
        self.database = database
        self.table = table
        self.select = select
        self.where = where
        self.limit = limit
        self.paging = paging
        self.batch_size = min(10000, max(batch_size, 100))

    def iter(self):
        base_query = f'select {self.select} from {self.table}'
        if self.where:
            base_query = base_query + " where " + self.where
        if self.limit:
            # 指定了limit
            query = base_query + f" limit {self.limit}"
            print("Query:", query)
            for row in self.fetch_all(query):
                yield row
        elif self.paging:
            # 用limit分页的方式读取数据
            skip = 0
            while True:
                query = base_query + f" limit {skip}, {self.batch_size}"
                print("Query:", query)
                num = 0
                for row in self.fetch_all(query):
                    num += 1
                    yield row
                if num == 0:
                    break
                skip += self.batch_size
        else:
            # 直接基于游标读取全部数据
            print("Query:", base_query)
            for row in self.fetch_cursor(base_query):
                yield row

    def fetch_cursor(self, query: str):
        """基于游标读取全部数据 需要连接支持"""
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            col_name_list = [tup[0] for tup in cursor.description]
            for row in cursor:
                yield row if isinstance(row, dict) else dict(zip(col_name_list, row))

    def fetch_all(self, query: str, fmt="json"):
        """基于缓存的读取全部数据 仅适合小规模数据"""
        cursor = self.conn.cursor()
        cursor.execute(query)
        if fmt == "tuple":
            for row in cursor:
                if isinstance(row, tuple):
                    yield row
                else:
                    yield tuple(row.values())
        else:
            col_name_list = [tup[0] for tup in cursor.description]
            for item in cursor.fetchall():
                if isinstance(item, tuple):
                    yield dict(zip(col_name_list, list(item)))
                else:
                    yield item

    def show_create(self, table: str, database: str = None):
        table = f'`{database}`.`{table}`' if database else f'`{table}`'
        sql = f"show create table {table}"
        return list(self.fetch_all(sql, fmt="tuple"))

    def list_tables(self, db: str = None):
        sql = f"show tables"
        if db:
            sql = f"show tables in `{db}`"
        return [row[0] for row in self.fetch_all(sql, fmt="tuple")]

    def desc_table(self, table: str, database: str = None):
        table = f'`{database}`.`{table}`' if database else f'`{table}`'
        sql = f"describe {table}"
        return list(self.fetch_all(sql))

    def close(self):
        if self.conn:
            self.conn.close()
