import json
import requests
from requests.auth import HTTPBasicAuth
from .base import Database

id_keys = ["_id", "id", "mongo_id"]

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}


class ES(Database):
    """
    基于RESTFul API的ES数据库组件，提供ES检索、扫描、获取指定记录、获取索引列表、写入数据、删除数据等操作。
    根据参数自动创建索引
    """
    def __init__(self, host: str = "localhost",
                 port: int = 9200,
                 username: str = None,
                 password: str = None,
                 index: str = None,
                 secure: bool = False,
                 auto_create: bool = False,
                 index_config: dict = None,
                 **kwargs):
        """
        :param host ES节点主机名（暂不支持多节点） 默认本机
        :param port ES HTTP端口，默认值9200
        :param username 用户名
        :param password 用户密码 如果为空则表示不需要验证
        :param index 默认操作的索引 支持在具体方法中指定不同索引
        :param secure 是否为HTTPS协议 默认false
        :param auto_create 是否自动创建索引 默认false
        :param index_config 索引配置 如果需要显式创建索引则需要提供
        """
        self.url = f"{'https' if secure else 'http'}://{host}:{port}"
        if password:
            self.auth = HTTPBasicAuth(username, password)
        else:
            self.auth = None
        self.index = index
        if index and auto_create and not self.index_exists(index):
            if self.index_create(index_config, index):
                print('index created: ', index)

    def index_exists(self, index: str = None):
        """检查索引是否存在"""
        index = index or self.index
        res = requests.head(f'{self.url}/{index}', auth=self.auth)
        if res.status_code == 200:
            return True
        if res.status_code == 404:
            return False
        print("Error: ", res.status_code, res.text)
        return False

    def index_create(self, config: dict, index: str = None):
        """创建索引"""
        index = index or self.index
        res = requests.put(f'{self.url}/{index}', auth=self.auth, json=config)
        if res.status_code == 200:
            return True
        if res.status_code == 404:
            return False
        print("Error: ", res.status_code, res.text)
        return False

    def search(self, query: dict = None,
               query_body: dict = None,
               fetch_size: int = 10,
               index: str = None,
               **kwargs):
        """搜索
        :param query 检索查询对象 对应 GET /_search  -d '{"query": xxx}'
        :param query_body 检索整体对象 对应 GET /_search -d `xxx` 默认为None
        :param fetch_size 指定返回数据条数 对应 size参数 默认为10
        :param index 指定索引 默认为None表示使用当前组件默认索引
        """
        index = index or self.index
        query_body = query_body or {}
        if query:
            query_body['query'] = query
        elif 'query' not in query_body:
            query_body['query'] = {"match_all": {}}
        if 'size' not in query_body:
            query_body['size'] = fetch_size
        print("ES search query_body:", query_body)
        res = requests.post(f'{self.url}/{index}/_search', auth=self.auth, json=query_body, **kwargs)
        if res.status_code != 200:
            print("Error:", res.text)
            return

        res = res.json()

        if 'hits' not in res or 'hits' not in res['hits']:
            print('ERROR', res)
            return

        hits = res['hits']['hits']
        for hit in hits:
            # print(hit)
            doc = hit.get('_source') or {}
            doc['_id'] = hit['_id']
            doc['_score'] = hit['_score']
            if 'fields' in hit:
                doc.update(hit['fields'])
            yield doc

    def semantic_search(self,
                        query_vector: list,
                        field: str = 'vector',
                        fetch_size: int = 10,
                        index: str = None,
                        **kwargs):
        """基于ES的向量化检索"""
        index = index or self.index
        query_body = {
            'knn': {
                "field": field,
                "query_vector": query_vector,
                "k": fetch_size,
                "num_candidates": 20
            },
            'size': fetch_size
        }
        print("ES search query_body:", query_body)
        res = requests.post(f'{self.url}/{index}/_search', auth=self.auth, json=query_body, **kwargs)
        if res.status_code != 200:
            print("Error:", res.text)
            return None

        res = res.json()

        if 'hits' not in res or 'hits' not in res['hits']:
            print('ERROR', res)
            return None

        hits = res['hits']['hits']
        results = []
        for hit in hits:
            # print(hit)
            doc = hit.get('_source') or {}
            doc['_id'] = hit['_id']
            doc['_score'] = hit['_score']
            if 'fields' in hit:
                doc.update(hit['fields'])
            results.append(doc)
        return results

    def hybrid_search(self,
                      text: str,
                      query_vector: list,
                      text_field: str = 'text',
                      vector_field: str = 'vector',
                      fetch_size: int = 10,
                      index: str = None,
                      **kwargs):
        """基于ES的混合检索"""
        index = index or self.index
        query_body = {
            'knn': {
                "field": vector_field,
                "query_vector": query_vector,
                "k": fetch_size,
                "num_candidates": 20
            },
            'query': {
                "match_phrase": {
                    text_field: {
                        "query": text,
                        "minimum_should_match": 1
                    }
                }
            },
            'size': fetch_size
        }
        print("ES search query_body:", query_body)
        res = requests.post(f'{self.url}/{index}/_search', auth=self.auth, json=query_body, **kwargs)
        if res.status_code != 200:
            print("Error:", res.text)
            return None

        res = res.json()

        if 'hits' not in res or 'hits' not in res['hits']:
            print('ERROR', res)
            return None

        hits = res['hits']['hits']
        results = []
        for hit in hits:
            # print(hit)
            doc = hit.get('_source') or {}
            doc['_id'] = hit['_id']
            doc['_score'] = hit['_score']
            if 'fields' in hit:
                doc.update(hit['fields'])
            results.append(doc)
        return results

    def scroll(self, query: dict = None,
               query_body: dict = None,
               batch_size: int = 10,
               fetch_size: int = 10000,
               index: str = None,
               _scroll: str = "1m",
               **kwargs):
        """滚动方式进行查询，方便对数据进行导出
        :param query 检索查询对象 对应 GET /_search  -d '{"query": xxx}'
        :param query_body 检索整体对象 对应 GET /_search -d `xxx` 默认为None
        :param batch_size 每个批次返回数据量 对应size参数 默认10
        :param fetch_size 指定返回的全部数据条数
        :param index 指定索引 默认为None表示使用当前组件默认索引
        :param _scroll 指定_scroll参数，会影响scroll会话保持时间 默认为1m，表示1分钟
        """
        index = index or self.index
        query_body = query_body or {}
        if query:
            query_body['query'] = query
        elif 'query' not in query_body:
            query_body['query'] = {"match_all": {}}
        if 'size' not in query_body:
            query_body['size'] = batch_size
        print("ES scroll query_body:", query_body)
        scroll_id = None
        total = 0
        while True:
            if scroll_id:
                # 后续请求
                url = f'{self.url}/_search/scroll'
                res = requests.post(url, auth=self.auth, json={'scroll': _scroll, 'scroll_id': scroll_id}, **kwargs)
            else:
                # 第一次请求 scroll
                url = f'{self.url}/{index}/_search?scroll={_scroll}'
                res = requests.post(url, auth=self.auth, json=query_body, **kwargs)

            if res.status_code != 200:
                print("Error:", res.text)
                break

            res = res.json()

            if 'hits' not in res or 'hits' not in res['hits']:
                print('ERROR', res)
                continue

            if '_scroll_id' in res:
                scroll_id = res['_scroll_id']

            hits = res['hits']['hits']
            for hit in hits:
                doc = hit.get('_source') or {}
                doc['_id'] = hit['_id']
                yield doc

            total += len(hits)

            if len(hits) < batch_size or 0 < fetch_size <= total:
                break

        if scroll_id:
            # clear scroll
            url = f'{self.url}/_search/scroll'
            requests.delete(url, auth=self.auth, json={'scroll_id': scroll_id})

    def exists(self, _id, index: str = None, **kwargs):
        """判断指定_id数据是否存在"""
        index = index or self.index
        if isinstance(_id, dict):
            _id = _id.get("_id") or _id.get("id")
        url = f'{self.url}/{index}/_doc/{_id}?_source=_id'
        res = requests.get(url, auth=self.auth)
        if res.status_code == 200:
            return res.json().get("found") is True
        return False

    def get(self, _id, index: str = None, **kwargs):
        """获取指定_id数据"""
        index = index or self.index
        if isinstance(_id, dict):
            _id = _id.get("_id") or _id.get("id")
        url = f'{self.url}/{index}/_doc/{_id}'
        res = requests.get(url, auth=self.auth)
        if res.status_code == 200:
            _doc = res.json()
            _source = _doc.get('_source')
            _source['_id'] = _doc.get('_id')
            return _source
        return None

    def delete(self, _id, index: str = None, **kwargs):
        """删除指定_id数据"""
        index = index or self.index
        if isinstance(_id, dict):
            _id = _id.get("_id") or _id.get("id")
        url = f'{self.url}/{index}/_doc/{_id}'
        res = requests.delete(url, auth=self.auth)
        return res.status_code == 200

    def upsert(self, items: dict or list, index: str = None, **kwargs):
        """插入或更新数据，基于bulk方式提交 支持批量"""
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
