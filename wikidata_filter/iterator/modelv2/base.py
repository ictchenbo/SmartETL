import sys
import os
import json
import requests
import traceback
from wikidata_filter.util.prompt import template


class Model:
    """
    模型基础类。定义模型（包括大模型）调用协议，但不关心具体参数
    """
    def __init__(self,
                 api_base: str,
                 api_key: str = None,
                 proxy: str = None,
                 ignore_errors: bool = True,
                 **kwargs
                 ):
        """
        :param api_base 服务地址，必须
        :param api_key API的Key
        :param proxy 调用代理，形式：system 或 http(s)://username:password@host:port
        :param ignore_errors 是否忽略错误 如果为False且调用发生错误则抛出异常，默认True
        :param model 模型名字 如'gpt-4o' 可用的模型需要查看提供模型服务的平台的说明
        :param temperature 模型温度参数
        :param topk 模型topk参数
        :param topp 模型topp参数
        """
        self.api_base = api_base
        self.url = f'{self.api_base}/chat/completions'
        self.api_key = api_key
        self.proxy = None
        if proxy:
            if proxy.lower() == "system":
                self.proxy = {
                    "http": os.environ.get("HTTP_PROXY"),
                    "https": os.environ.get("HTTPS_PROXY")
                }
            else:
                self.proxy = {
                    "http": proxy,
                    "https": proxy
                }
        self.ignore_errors = ignore_errors
        self.args_base = dict(**kwargs)

    def invoke(self, data: dict, stream: bool = False, **kwargs):
        """执行HTTP-POST请求"""
        json_data = dict(**self.args_base, **data)
        headers = {
            'content-type': 'application/json'
        }
        if self.api_key:
            headers['Authorization'] = 'Bearer ' + self.api_key
        print(f"requesting Model(api_base={self.api_base}, model={data.get('model')})")
        try:
            res = requests.post(self.url, headers=headers, json=json_data, proxies=self.proxy, stream=stream)
            if res.status_code == 200:
                if stream:
                    return res.iter_lines()
                else:
                    return res.json()
            print(res.text, file=sys.stderr)
            return None
        except Exception as ex:
            traceback.print_exc()
            print("Request Error")
            if self.ignore_errors:
                return None
            raise Exception(f"Access {self.url} Error!")


class LLMModel(Model):
    """大模型基础类。定义大模型调用(chat)及返回值解析，统一输入为query，不负责query构造"""
    def __init__(self,
                 api_base: str,
                 api_key: str = None,
                 prompt: str = None,
                 proxy: str = None,
                 ignore_errors: bool = True,
                 model: str = None,
                 **kwargs
                 ):
        super().__init__(api_base, api_key=api_key, proxy=proxy, ignore_errors=ignore_errors, model=model, **kwargs)
        self.prompt = template(prompt)

    def chat(self, data: str or dict, stream: bool = False, **params):
        params["messages"] = [
            {
                "role": "user",
                "content": self.prompt(data),
            }
        ]

        res = super().invoke(params, stream)
        if stream:
            result = []
            for chunk in res:
                if chunk:
                    line = chunk.decode("utf-8")
                    if line.startswith("data: {"):  # 过滤掉非数据行
                        json_data = json.loads(line[5:])  # 去掉 "data: " 前缀
                        piece_data = json_data["choices"][0].get("delta")
                        text_piece = piece_data.get("content") or piece_data.get("reasoning_content")
                        if text_piece is None:
                            continue
                        print(text_piece, end='', flush=True)
                        v = piece_data.get("content")
                        if v:
                            result.append(text_piece)
            return ''.join(result)
        else:
            return res['choices'][0]['message']['content']
