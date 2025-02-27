import json
from wikidata_filter.iterator.buffer import BufferedWriter


class CKWriter(BufferedWriter):
    """
    数据写入CK中
    """
    def __init__(self, host='localhost',
                 tcp_port=9000,
                 username="default",
                 password="",
                 database='default',
                 table=None,
                 cluster=None,
                 buffer_size=1000, **kwargs):
        super().__init__(buffer_size=buffer_size)
        try:
            from clickhouse_driver import Client
        except:
            print('install clickhouse_driver first!')
            raise "clickhouse_driver not installed"

        self.table = table
        self.client = Client(host=host,
                             port=tcp_port,
                             database=database,
                             user=username,
                             password=password,
                             send_receive_timeout=20)
        # 如果cluster不为空 表示分布式表插入
        self.cluster = f'on cluster {cluster}' if cluster else ''

    def write_batch(self, rows: list):
        json_data = [json.dumps(row, ensure_ascii=False) for row in rows]
        sql = f"insert into {self.table} {self.cluster} format JSONEachRow {','.join(json_data)}"
        # print(sql)
        try:
            self.client.execute(sql)
            return True
        except Exception as e:
            print(e)
            return False
