import uuid
import requests

from .base import Database


class Qdrant(Database):
    def __init__(self, host: str = 'localhost',
                 port: int = 6333,
                 api_key=None,
                 auto_create: bool = False,
                 collection: str = None,
                 **kwargs):
        self.api_base = f'http://{host}:{port}'
        self.api_key = api_key
        self.headers = {}
        if api_key:
            self.headers['api-key'] = api_key
        self.collection = collection
        if collection and auto_create and not self.index_exists(collection):
            self.index_create(collection, **kwargs)

    def index_exists(self, index: str):
        """判断集合是否存在"""
        res = requests.get(f'{self.api_base}/collections/{index}/exists', headers=self.headers)
        if res.status_code == 200 and res.json()['result']['exists']:
            return True
        print("ERROR: ", res.text)
        return False

    def index_create(self, index: str, size: int = 1024, distance: str = 'Cosine'):
        """创建集合"""
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

    def index_drop(self, index: str):
        """删除集合"""
        res = requests.delete(f'{self.api_base}/collections/{index}', headers=self.headers)
        if res.status_code == 200 and res.json()['status'] == 'ok':
            return True
        print("ERROR: ", res.text)
        return False

    def scroll(self, batch_size: int = 1000,
               with_payload: bool = True,
               with_vector: bool = False,
               flat: bool = True,
               offset: int or str = None,
               collection: str = None,
               **kwargs):
        """滚动遍历数据"""
        collection = collection or self.collection
        data = {
            "limit": batch_size,
            "with_payload": with_payload,
            "with_vector": with_vector
        }
        if offset is not None:
            data["offset"] = offset
        while True:
            res = requests.post(f'{self.api_base}/collections/{collection}/points/scroll', json=data,
                                headers=self.headers)
            if res.status_code == 200:
                result = res.json()["result"]
                for point in result["points"]:
                    ret = {'id': point['id']}
                    if flat:
                        if with_payload:
                            ret.update(**point.get('payload', {}))
                        if with_vector:
                            ret['vector'] = point.get('vector')
                    yield ret
                if "next_page_offset" in result:
                    data["offset"] = result["next_page_offset"]
                else:
                    break
            else:
                print(res.text)
                break

    def upsert(self, items: dict or list,
               vector_field: str = 'vector',
               collection: str = None, **kwargs):
        rows = []
        if not isinstance(items, list):
            items = [items]
        collection = collection or self.collection
        for item in items:
            _id = None
            if '_id' in item:
                _id = item.pop('_id')
            elif 'id' in item:
                _id = item.pop('id')
            else:
                _id = str(uuid.uuid4())
            row = {
                'id': _id,
                'vector': item.pop('vector'),
                'payload': item
            }
            rows.append(row)
        data = {
            "points": rows
        }
        res = requests.put(f'{self.api_base}/collections/{collection}/points?wait=true', json=data, headers=self.headers)
        if res.status_code == 200:
            data = res.json()
            if data['status'] == 'ok':
                return True
            print("qdrant upsert error:", data)
            return False
        print("ERROR: ", res.text)
        return False

    def search(self, query_vector: list,
               filter_: dict = None,
               offset: int = 0,
               limit: int = 5,
               collection: str = None,
               **kwargs):
        data = {
            'vector': query_vector,
            'offset': offset,
            'limit': limit,
            'with_payload': True
        }
        collection = collection or self.collection
        if filter_:
            data['filter'] = filter_
        res = requests.post(f'{self.api_base}/collections/{collection}/points/search', json=data, headers=self.headers)
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

    def delete(self, ids, collection: str = None, **kwargs):
        rows = ids
        if not isinstance(rows, list):
            rows = [ids]
        ids = []
        for row in rows:
            if isinstance(row, dict):
                ids.append(row.get("_id") or row.get("id"))
            else:
                ids.append(row)
        collection = collection or self.collection
        data = {
            "points": ids
        }
        res = requests.post(f'{self.api_base}/collections/{collection}/points/delete', json=data, headers=self.headers)
        if res.status_code == 200:
            return True
        else:
            print(res.text)
            return False


if __name__ == '__main__':
    client = Qdrant("10.60.1.145", collection="chunk_news_v2")
    client.delete(1235312132)
