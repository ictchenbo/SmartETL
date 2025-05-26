import uuid
import requests
from wikidata_filter.iterator.buffer import BufferedWriter


class Qdrant(BufferedWriter):
    def __init__(self, host: str = 'localhost',
                 port: int = 6333,
                 api_key=None,
                 collection: str = "chunks",
                 buffer_size: int = 100,
                 vector_field='vector'):
        super().__init__(buffer_size)
        self.api_base = f'http://{host}:{port}'
        self.api_key = api_key
        self.collection = collection
        self.vector_field = vector_field
        self.headers = {}
        if api_key:
            self.headers['api-key'] = api_key

    def write_batch(self, rows: list):
        items = []
        for item in rows:
            if self.vector_field not in item:
                print('Warning, no vector field, ignore')
                continue

            if '_id' in item:
                _id = item.pop('_id')
            elif 'id' in item:
                _id = item.pop('id')
            else:
                _id = str(uuid.uuid4())

            row = {
                'id': _id,
                'vector': item.pop(self.vector_field),
                'payload': item
            }
            items.append(row)

        if not items:
            print('Warning, empty list')
            return False

        data = {
            "points": items
        }
        res = requests.put(f'{self.api_base}/collections/{self.collection}/points', json=data, headers=self.headers)
        if res.status_code == 200:
            data = res.json()
            if data['status'] == 'ok':
                return True
            print("qdrant upsert error:", data)
            return False
        print("ERROR: ", res.text)
        return False
