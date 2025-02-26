import json
from .base import Database

try:
    import pymysql
except ImportError:
    print('install pymysql first!')
    raise "pymysql not installed"


class MySQLOps(Database):
    def __init__(self, host='localhost',
                 port=3306,
                 username='root',
                 password='123456',
                 database='goin',
                 table: str = None,
                 key_col: str = 'id',
                 charset='utf8'):
        self.params = dict(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            charset=charset
        )
        self.table = table
        self.key_col = key_col

    def get_conn(self):
        return pymysql.connect(**self.params)

    def fetchall(self, sql, params=None):
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                print("Executing SQL:", cursor.mogrify(sql, params))
                cursor.execute(sql, params)
                columns = [desc[0] for desc in cursor.description]
                res = cursor.fetchall()
                for row in res:
                    yield {columns[i]: row[i] for i in range(len(row))}

    def execute(self, sql, params=None):
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                print("Executing SQL:", cursor.mogrify(sql, params))
                count = cursor.execute(sql, params)
                insert_id = cursor.lastrowid
                cursor.close()
                conn.commit()
                return count, insert_id

    def fetchone(self, sql, params=None):
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                print("Executing SQL:", cursor.mogrify(sql, params))
                cursor.execute(sql, params)
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
                if row:
                    row = {columns[i]: row[i] for i in range(len(row))}
                return row

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

    def insert(self, table, row: dict, mode='insert'):
        fields, placeholders, values = self.gen_sql(row)
        cmd = 'replace' if mode != 'insert' else 'insert'
        sql = f"{cmd} into {table}({','.join(fields)}) values ({','.join(placeholders)})"
        return self.execute(sql, values)

    def insert_many(self, table, rows: list, mode='insert'):
        if not rows:
            return 0
        list_values = []
        for row in rows:
            fields, placeholders, values = self.gen_sql(row)
            list_values.append(values)
        cmd = 'replace' if mode != 'insert' else 'insert'
        sql = f"{cmd} into {table}({','.join(fields)}) values ({','.join(placeholders)})"
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                # print("Executing SQL:", cursor.mogrify(sql, params))
                count = cursor.executemany(sql, list_values)
                insert_id = cursor.lastrowid
                cursor.close()
                conn.commit()
                return count, insert_id

    def update(self, table, _id, row: dict):
        fields, placeholders, values = self.gen_sql(row)
        sets = [f'{field} = %s' for field in fields]
        sql = f"update {table} set {','.join(sets)} where id=%s"
        values.append(_id)
        return self.execute(sql, values)

    def exists(self, _id, **kwargs):
        table = kwargs.get("table") or self.table
        sql = f'select * from {table} where {self.key_col} = %s'
        one = self.fetchone(sql, (_id, ))
        return one is not None
