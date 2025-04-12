from .base import Database


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
        self.collection = collection

    def scroll(self, query: dict = None,
               skip: int = 0,
               limit: int = None,
               sortby: str = None,
               collection: str = None, **kwargs):
        collection = collection or self.collection
        coll = self.db[collection]
        cursor = coll.find(query or {})
        if skip > 0:
            cursor.skip(skip)
        if limit is not None and limit > 0:
            cursor.limit(limit)
        if sortby:
            cursor.sort(sortby, -1)
        for doc in cursor:
            yield doc

    def exists(self, _id, **kwargs):
        query = {"_id": _id}
        res = self.table(**kwargs).find_one(query)
        return res is not None

    def close(self):
        self.client.close()

    def insert_batch(self, coll, rows: list):
        try:
            coll.insert_many(rows)
            print(f'{len(rows)} rows inserted')
            return True
        except Exception as e:
            print(e)
            return False

    def upsert(self, rows: list, collection: str = None, write_mode: str = "upsert", **kwargs):
        collection = collection or self.collection
        coll = self.db[collection]
        if "insert" == write_mode:
            self.insert_batch(coll, rows)
        elif "upsert" == write_mode:
            to_insert = []
            for row in rows:
                if "_id" in row:
                    query = {"_id": row["_id"]}
                    coll.find_one_and_update(query, {'$set': row}, upsert=True)
                else:
                    to_insert.append(row)
            if to_insert:
                self.insert_batch(coll, to_insert)
