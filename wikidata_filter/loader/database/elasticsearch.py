from wikidata_filter.loader.base import DataProvider
import requests


class ES(DataProvider):
    """
    读取ES指定索引全部数据，支持提供查询条件
    """
    def __init__(self, host: str = "localhost",
                 port: int = 9200,
                 user: str = None,
                 password: str = None,
                 index: str = None,
                 query: dict = None,
                 batch_size: int = 1000,
                 fetch_size: int = 0,
                 **kwargs):
        self.url = f"http://{host}:{port}"
        self.cache = []
        if password:
            self.auth = (user, password)
        else:
            self.auth = None
        self.index_name = index
        self.query = query
        self.scroll = "1m"
        self.batch_size = batch_size
        self.fetch_size = fetch_size
        self.query_body = {
            'size': batch_size,
            'query': query if query else {'match_all': {}}
        }

    def iter(self):
        scroll_id = None
        total = 0
        while True:
            if scroll_id:
                # 后续请求
                url = f'{self.url}/_search/scroll'
                res = requests.post(url, auth=self.auth, json={'scroll': self.scroll, 'scroll_id': scroll_id})
            else:
                # 第一次请求 scroll
                url = f'{self.url}/{self.index_name}/_search?scroll={self.scroll}'
                res = requests.post(url, auth=self.auth, json=self.query_body)

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

            if len(hits) < self.batch_size or 0 < self.fetch_size <= total:
                # clear scroll
                url = f'{self.url}/_search/scroll'
                requests.delete(url, auth=self.auth, json={'scroll_id': scroll_id})
                break
