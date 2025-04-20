import json
import requests
from .base import Database

id_keys = ["_id", "id", "mongo_id"]


class ES(Database):
    """
    读取ES指定索引全部数据，支持提供查询条件
    """
    def __init__(self, host: str = "localhost",
                 port: int = 9200,
                 user: str = None,
                 password: str = None,
                 index: str = None,
                 **kwargs):
        self.url = f"http://{host}:{port}"
        self.cache = []
        if password:
            self.auth = (user, password)
        else:
            self.auth = None
        self.index = index

    def scroll(self, query: dict = None,
               batch_size: int = 100,
               fetch_size: int = 0,
               index: str = None,
               **kwargs):
        index = index or self.index
        query = query or {"match_all": {}}
        scroll_id = None
        total = 0
        while True:
            if scroll_id:
                # 后续请求
                url = f'{self.url}/_search/scroll'
                res = requests.post(url, auth=self.auth, json={'scroll': self.scroll, 'scroll_id': scroll_id})
            else:
                # 第一次请求 scroll
                query_body = {
                    "query": query
                }
                url = f'{self.url}/{index}/_search?scroll={self.scroll}'
                res = requests.post(url, auth=self.auth, json=query_body)

            if res.status_code != 200:
                break

            res = res.json()
            if '_scroll_id' in res:
                scroll_id = res['_scroll_id']

            if 'hits' not in res or 'hits' not in res['hits']:
                print('ERROR', res)
                continue

            hits = res['hits']['hits']
            for hit in hits:
                doc = hit['_source']
                doc['_id'] = hit['_id']
                yield doc

            total += len(hits)

            if len(hits) < batch_size or 0 < fetch_size <= total:
                # clear scroll
                url = f'{self.url}/_search/scroll'
                requests.delete(url, auth=self.auth, json={'scroll_id': scroll_id})
                break

    def exists(self, _id, index: str = None, **kwargs):
        index = index or self.index
        if isinstance(_id, dict):
            _id = _id.get("_id") or _id.get("id")
        url = f'{self.url}/{index}/_doc/{_id}?_source=_id'
        res = requests.get(url, auth=self.auth)
        if res.status_code == 200:
            return res.json().get("found") is True
        return False

    def delete(self, _id, index: str = None, **kwargs):
        index = index or self.index
        if isinstance(_id, dict):
            _id = _id.get("_id") or _id.get("id")
        url = f'{self.url}/{index}/_doc/{_id}'
        res = requests.delete(url, auth=self.auth)
        return res.status_code == 200

    def upsert(self, items: dict or list, index: str = None, **kwargs):
        index = index or self.index
        header = {
            "Content-Type": "application/json"
        }
        if not isinstance(items, list):
            items = [items]
        lines = []
        for row in items:
            action_row = {}
            for key in id_keys:
                if key in row:
                    action_row["_id"] = row.pop(key)
                    break
            # row_meta = json.dumps({"index": action_row})
            row_meta = json.dumps({"index": action_row})
            row_data = json.dumps(row)
            lines.append(row_meta)
            lines.append(row_data)
        body = '\n'.join(lines)
        body += '\n'
        print(f"{self.url}/{index} bulk")
        res = requests.post(f'{self.url}/{index}/_bulk', data=body, headers=header, auth=self.auth)
        if res.status_code != 200:
            print("Warning, ES bulk load failed:", res.text)
            return False
        return True
