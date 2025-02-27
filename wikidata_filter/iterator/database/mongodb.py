
from wikidata_filter.iterator.buffer import BufferedWriter


class MongoWriter(BufferedWriter):
    def __init__(self, host='localhost',
                 port=27017,
                 username=None,
                 password=None,
                 auth_db='admin',
                 database='default',
                 collection=None,
                 buffer_size: int = 1000,
                 mode: str = "upsert", **kwargs):
        super().__init__(buffer_size)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.auth_db = auth_db
        self.database = database
        self.collection = collection
        self.mode = mode

    def get_coll(self):
        try:
            import pymongo
        except:
            print('install pymongo first!')
            raise "pymongo not installed"
        client = pymongo.MongoClient(host=self.host, port=self.port)
        if self.username:
            client[self.auth_db].authenticate(self.username, self.password)
        db = client[self.database]
        return db[self.collection]

    def insert_batch(self, rows: list):
        coll = self.get_coll()
        try:
            coll.insert_many(rows)
            print(f'{len(rows)} rows inserted')
            return True
        except Exception as e:
            print(e)
            return False

    def write_batch(self, rows: list):
        # print(f'mongo commit batch in {self.mode} mode:', len(rows))
        if "insert" == self.mode:
            self.insert_batch(rows)
        elif "upsert" == self.mode:
            to_insert = []
            for row in rows:
                # print(row)
                if "_id" in row:
                    query = {"_id": row["_id"]}
                    self.get_coll().find_one_and_update(query, {'$set': row}, upsert=True)
                    # print(f'one row upserted')
                else:
                    to_insert.append(row)
            if to_insert:
                self.insert_batch(to_insert)
