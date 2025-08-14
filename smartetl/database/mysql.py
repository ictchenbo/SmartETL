from .rdb_base import RDBBase


class MySQL(RDBBase):
    def __init__(self, host='localhost',
                 port=3306,
                 username='root',
                 password='123456',
                 charset='utf8mb4', **kwargs):
        super().__init__(**kwargs)
        try:
            import pymysql
        except ImportError:
            print('install pymysql first!')
            raise "pymysql not installed"

        self.conn = pymysql.connect(host=host,
            port=port,
            user=username,
            password=password,
            charset=charset,
            database=self.database
            # cursorclass = pymysql.cursors.SSCursor
        )
