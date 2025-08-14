"""
中国科学院计算所天玑团队自主研发训练的大模型服务

注意，接口与OpenAI不兼容
"""
import sys
import requests
from .base import LLM


class GoGPT(LLM):
    def __init__(self, api_base: str, key: str, **kwargs):
        super().__init__(api_base, key=key, **kwargs)

    def request_service(self, query: str):
        try:
            res = requests.post(self.api_base, json={'prompt': query})
            if res.status_code == 200:
                return res.json()['response'].strip()
            print(res.text, file=sys.stderr)
            return None
        except:
            print("Request Error")
            if self.ignore_errors:
                return None
            raise Exception(f"Access {self.api_base} Error!")
