import json

import requests


class Qdrant:
    def __init__(self, host: str = 'localhost', port: int = 6333, api_key=None):
        self.api_base = f'http://{host}:{port}'
        self.api_key = api_key
        self.headers = {}
        if api_key:
            self.headers['api-key'] = api_key

    def index_exists(self, index: str):
        res = requests.get(f'{self.api_base}/collections/{index}/exists', headers=self.headers)
        if res.status_code == 200 and res.json()['result']['exists']:
            return True
        print("ERROR: ", res.text)
        return False

    def index_create(self, index: str, size: int, distance: str = 'Cosine'):
        if not self.index_exists(index):
            data = {
                "vectors": {
                    "size": size,
                    "distance": distance
                }
            }
            res = requests.put(f'{self.api_base}/collections/{index}', json=data, headers=self.headers)
            if res.status_code == 200 and res.json()['status'] == 'ok':
                return True
            print("ERROR: ", res.text)
            return False
        return True

    def index_drop(self, index: str):
        res = requests.delete(f'{self.api_base}/collections/{index}', headers=self.headers)
        if res.status_code == 200 and res.json()['status'] == 'ok':
            return True
        print("ERROR: ", res.text)
        return False

    def upsert(self, index: str, items: list):
        rows = []
        for item in items:
            row = {
                'id': item.pop('_id'),
                'vector': item.pop('vector'),
                'payload': item
            }
            rows.append(row)
        data = {
            "points": rows
        }
        print(json.dumps(data, ensure_ascii=False))
        res = requests.put(f'{self.api_base}/collections/{index}/points', json=data, headers=self.headers)
        if res.status_code == 200:
            data = res.json()
            if data['status'] == 'ok':
                return True
            print("qdrant upsert error:", data)
            return False
        print("ERROR: ", res.text)
        return False

    def search(self, index: str, query_vector: list, filter_=None, offset: int = 0, limit: int = 5):
        data = {
            'vector': query_vector,
            'offset': offset,
            'limit': limit,
            'with_payload': True
        }
        if filter_:
            data['filter'] = filter_
        res = requests.post(f'{self.api_base}/collections/{index}/points/search', json=data, headers=self.headers)
        if res.status_code == 200:
            result = res.json()['result']
            rows = []
            for item in result:
                payload = item['payload']
                payload['_id'] = item['id']
                payload['_score'] = item['score']
                rows.append(payload)
            return rows
        return None
