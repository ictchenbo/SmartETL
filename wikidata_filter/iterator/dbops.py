from wikidata_filter.iterator.base import DictProcessorBase


class DBOps(DictProcessorBase):
    def __init__(self, db):
        self.db = db


class SQLDump(DBOps):
    def __init__(self, db, query: str = None, table: str = None, database: str = None, **kwargs):
        super().__init__(db)
        self.query = query
        self.database = database
        self.table = table

    def on_data(self, data: dict, *args):
        table = self.table or data.get("table")
        database = self.database or data.get("database")
        create_sql = self.db.show_create(table, database)
        table1 = f'`{database}`.`{table}`' if database else f'`{table}`'
        columns = self.db.desc_table(table, database)
        print("Query", table)

        yield {"action": "query_start", "meta": {"database": database, "table": table, "columns": columns, "create_sql": create_sql}}

        sql = self.query or f"select * from {table1}"
        for row in self.db.fetch_all(sql):
            yield row

        yield {"action": "query_end", "meta": {"database": database, "table": table}}


class Delete(DictProcessorBase):
    """
    删除ES数据
    """
    def __init__(self, host="localhost",
                 port=9200,
                 username=None,
                 password=None,
                 index=None,
                 id_key: str = "_id", **kwargs):
        self.url = f"http://{host}:{port}"
        if password:
            self.auth = (username, password)
        else:
            self.auth = None
        self.index = index
        self.id_key = id_key

    def on_data(self, data: dict, *args):
        row_id = data.get(self.id_key)
        if row_id:
            res = requests.delete(f'{self.url}/{self.index}/_doc/{row_id}', auth=self.auth)
            if res.status_code == 200:
                print("Deleted:", row_id)
            else:
                print("Error:", res.json())
        return data

