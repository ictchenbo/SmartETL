import json
from .base import Database


class RDBBase(Database):
    conn = None

    def __init__(self, database: str = 'default',
                 table: str = None,
                 paging: bool = True,
                 key_col: str = 'id', **kwargs):
        self.database = database
        self.table = table
        self.paging = paging
        self.key_col = key_col

    def scroll(self, select: str = "*",
               where: str = None,
               fetch_size: str = None,
               batch_size: int = 1000,
               table: str = None, **kwargs):
        table = table or self.table
        base_query = f'select {select} from `{table}`'
        if where:
            base_query = base_query + " where " + where
        if fetch_size:
            # 指定了limit
            query = base_query + f" limit {fetch_size}"
            print("Query:", query)
            for row in self.fetchall(query):
                yield row
        elif self.paging:
            # 用limit分页的方式读取数据
            skip = 0
            while True:
                query = base_query + f" limit {skip}, {batch_size}"
                print("Query:", query)
                num = 0
                for row in self.fetchall(query):
                    num += 1
                    yield row
                if num == 0:
                    break
                skip += batch_size
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

    def fetchall(self, query: str, fmt="json"):
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

    def execute(self, sql, params=None):
        with self.conn.cursor() as cursor:
            print("Executing SQL:", cursor.mogrify(sql, params))
            count = cursor.execute(sql, params)
            insert_id = cursor.lastrowid
            cursor.close()
            self.conn.commit()
            return count, insert_id

    def fetchone(self, sql, params=None):
        with self.conn.cursor() as cursor:
            print("Executing SQL:", cursor.mogrify(sql, params))
            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            if row:
                row = {columns[i]: row[i] for i in range(len(row))}
            return row

    def show_create(self, table: str, database: str = None):
        table = f'`{database}`.`{table}`' if database else f'`{table}`'
        sql = f"show create table {table}"
        return list(self.fetchall(sql, fmt="tuple"))

    def list_tables(self, database: str = None):
        sql = f"show tables"
        if database:
            sql = f"show tables in `{database}`"
        return [row[0] for row in self.fetchall(sql, fmt="tuple")]

    def desc_table(self, table: str, database: str = None):
        table = f'`{database}`.`{table}`' if database else f'`{table}`'
        sql = f"describe {table}"
        return list(self.fetchall(sql))

    def exists(self, _id, table: str = None, **kwargs):
        table = table or self.table
        sql = f'select * from {table} where {self.key_col} = %s'
        one = self.fetchone(sql, (_id, ))
        return one is not None

    def delete(self, ids, table: str = None, **kwargs):
        table = table or self.table
        sql = f'delete from {table} where {self.key_col} = %s'
        return self.execute(sql, (ids, ))

    @staticmethod
    def gen_sql(row: dict):
        fields = []
        placeholders = []
        values = []
        for key, value in row.items():
            fields.append(f'`{key}`')
            placeholders.append('%s')
            # 字典或数组则自动转为json字符串格式
            if isinstance(value, dict) or isinstance(value, list):
                value = json.dumps(value, ensure_ascii=False)
            values.append(value)
        return fields, placeholders, values

    def upsert(self, items: dict or list,
               write_mode: str = "upsert",
               table: str = None,
               **kwargs):
        if isinstance(items, list):
            self.insert_many(items, mode=write_mode, table=table)
        else:
            self.insert(items, mode=write_mode, table=table)

    def insert(self, row: dict, mode='insert', table: str = None):
        table = table or self.table
        fields, placeholders, values = self.gen_sql(row)
        cmd = 'replace' if mode != 'insert' else 'insert'
        sql = f"{cmd} into {table}({','.join(fields)}) values ({','.join(placeholders)})"
        return self.execute(sql, values)

    def insert_many(self, rows: list, mode='insert', table: str = None):
        table = table or self.table
        if not rows:
            return 0
        list_values = []
        for row in rows:
            fields, placeholders, values = self.gen_sql(row)
            list_values.append(values)
        cmd = 'replace' if mode != 'insert' else 'insert'
        sql = f"{cmd} into {table}({','.join(fields)}) values ({','.join(placeholders)})"
        with self.conn.cursor() as cursor:
            # print("Executing SQL:", cursor.mogrify(sql, params))
            count = cursor.executemany(sql, list_values)
            insert_id = cursor.lastrowid
            cursor.close()
            self.conn.commit()
            return count, insert_id

    def update(self, table, _id, row: dict):
        fields, placeholders, values = self.gen_sql(row)
        sets = [f'{field} = %s' for field in fields]
        sql = f"update {table} set {','.join(sets)} where id=%s"
        values.append(_id)
        return self.execute(sql, values)

    def dump(self, data: dict, table: str = None, database: str = None, **kwargs):
        table = data.get("table") or table or self.table
        database = data.get("database") or database or self.database
        create_sql = self.show_create(table, database)
        table1 = f'`{database}`.`{table}`' if database else f'`{table}`'
        columns = self.desc_table(table, database)
        print("Query", table)

        yield {"action": "query_start", "meta": {"database": database, "table": table, "columns": columns, "create_sql": create_sql}}

        sql = f"select * from {table1}"
        for row in self.fetchall(sql):
            yield row

        yield {"action": "query_end", "meta": {"database": database, "table": table}}

    def close(self):
        if self.conn:
            self.conn.close()
