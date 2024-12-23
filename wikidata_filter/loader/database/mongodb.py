from wikidata_filter.loader.base import DataProvider


class Mongo(DataProvider):
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 27017,
                 username: str = None,
                 password: str = None,
                 auth_db: str = 'admin',
                 database: str = 'default',
                 collection: str = None,
                 sortby: str = None,
                 query: dict = None,
                 skip: int = 0,
                 limit=None, **kwargs):
        self.host = host
        self.port = port
        self.url = f'{host}:{port}/{database}/{collection}'
        try:
            import pymongo
        except:
            print('install pymongo first!')
            raise "pymongo not installed"

        self.client = pymongo.MongoClient(host=host, port=port)
        if username:
            self.client[auth_db].authenticate(username, password)
        self.db = self.client[database]
        self.coll = self.db[collection]
        self.sortby = sortby
        self.query = query
        self.skip = skip
        self.limit = limit

    def iter(self):
        cursor = self.coll.find(self.query)
        if self.skip > 0:
            cursor.skip(self.skip)
        if self.limit is not None and self.limit > 0:
            cursor.limit(self.limit)
        if self.sortby:
            cursor.sort(self.sortby, -1)
        for doc in cursor:
            yield doc

    def close(self):
        self.client.close()

    def __str__(self):
        return f'MongoLoader[{self.url}]'
