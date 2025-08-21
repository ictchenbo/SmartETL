import requests
from smartetl.processor.base import DictProcessorBase, Any


class Local(DictProcessorBase):
    """本地部署的向量化模型 用于对文本字段生成向量"""
    def __init__(self, api_base: str, key: str, target_key: str = '_embed'):
        self.api_base = api_base
        self.key = key
        self.target_key = target_key

    def on_data(self, data: dict, *args):
        if self.key in data and isinstance(data[self.key], str):
            text = data[self.key]
            if not text:
                print("Empty text, ignore")
                return data

            res = requests.post(self.api_base, json={'text': text})
            if res.status_code == 200:
                data[self.target_key] = res.json()
            else:
                print("Error to request:", self.api_base, res.text)

        return data
