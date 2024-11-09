import requests
from wikidata_filter.iterator.base import JsonIterator


class Local(JsonIterator):
    """本地部署的向量化模型 用于对文本字段生成向量"""
    def __init__(self, api_base: str, field: str, target_key: str = '_embed'):
        self.api_base = api_base
        self.field = field
        self.target_key = target_key

    def on_data(self, data: dict or None, *args):
        if self.field not in data:
            print("Warning! No such field:", self.field)
            return data

        text = data[self.field]
        if not text:
            print("Empty text, ignore")
            return data

        res = requests.post(self.api_base, json={'text': text})
        if res.status_code == 200:
            data[self.target_key] = res.json()
        else:
            print("Error to request:", self.api_base, res.text)

        return data
