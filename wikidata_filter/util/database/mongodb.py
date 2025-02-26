from .base import Database

try:
    import pymongo
except:
    print('install pymongo first!')
    raise "pymongo not installed"


class MongoDB(Database):
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 27017,
                 username: str = None,
                 password: str = None,
                 auth_db: str = 'admin',
                 database: str = 'default',
                 collection: str = None,
                 **kwargs):
        self.host = host
        self.port = port
        self.url = f'{host}:{port}/{database}/{collection}'
        self.client = pymongo.MongoClient(host=host, port=port)
        if username:
            self.client[auth_db].authenticate(username, password)
        self.db = self.client[database]
        self.coll = self.db[collection]

    def table(self, table=None, database=None):
        if database:
            db = self.client[database]
            return db[table]
        if table:
            return self.db[table]
        else:
            return self.coll

    def exists(self, _id, **kwargs):
        query = {"_id": _id}
        res = self.table(**kwargs).find_one(query)
        return res is not None

    def close(self):
        self.client.close()
