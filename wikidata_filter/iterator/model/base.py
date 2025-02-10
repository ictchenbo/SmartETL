import sys
import os
import requests
import traceback

from wikidata_filter.iterator.base import JsonIterator


class LLM(JsonIterator):
    """
    开放大模型（兼容OpenAI接口）的处理算子 输入必须为dict或str
    """
    def __init__(self,
                 api_base: str,
                 key: str = None,
                 api_key: str = None,
                 proxy: str = None,
                 ignore_errors: bool = True,
                 model: str = None,
                 prompt: str = None,
                 target_key: str = '_llm',
                 **kwargs
                 ):
        """
        :param api_base 服务地址，必须
        :param key 输入的字段名，如果为None，表示整个输入作为大模型请求参数，否则，提取该字段的值
        :param api_key API的Key
        :param proxy 调用代理，形式：system 或 http(s)://username:password@host:port
        :param ignore_errors 是否忽略错误 如果为False且调用发生错误则抛出异常，默认True
        :param model 模型名字 如'gpt-4o' 可用的模型需要查看提供模型服务的平台的说明
        :param prompt 提示模板
        :param placeholders 占位符列表
        """
        if key is None:
            print("Warning: key is None, use the whole input as the parameter of LLM api call")
        self.key = key
        self.api_base = api_base
        self.api_key = api_key
        self.prompt_template = prompt
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
        self.target_key = target_key or '_llm'
        args_base = dict(**kwargs)
        args_base['model'] = model
        args_base['stream'] = False
        self.args_base = args_base

    def request_service(self, query: str):
        """执行HTTP-POST请求"""
        url = f'{self.api_base}/chat/completions'
        data = dict(**self.args_base)
        data['messages'] = [
                {
                    "role": "user",
                    "content": query
                }
            ]
        headers = {
            'content-type': 'application/json'
        }
        if self.api_key:
            headers['Authorization'] = 'Bearer ' + self.api_key
        proxies = self.proxy or {}
        print(f"requesting LLM(api_base={self.api_base}, model={data.get('model')})")
        try:
            res = requests.post(url, headers=headers, json=data, proxies=proxies)
            if res.status_code == 200:
                return res.json()['choices'][0]['message']['content']
            print(res.text, file=sys.stderr)
            return None
        except Exception as ex:
            traceback.print_exc()
            print("Request Error")
            if self.ignore_errors:
                return None
            raise Exception(f"Access {url} Error!")

    def on_data(self, row, *args):
        if isinstance(row, dict):
            if self.key:
                val = row.get(self.key)
            else:
                print("Warning: got dict data but the value field not specified!")
                return row
        elif isinstance(row, str):
            if self.key:
                print("Warning: got str, the specified field ignored")
            val = row
        else:
            print("Warning: input data type not supported! Must be dict or str")
            return row
        query = self.prompt_template.replace('{data}', val) if self.prompt_template else val
        # print(query)
        result = self.request_service(query)
        # print(result)
        if result:
            if self.key is None:
                return result
            else:
                row[self.target_key] = result
                return row
        return row
