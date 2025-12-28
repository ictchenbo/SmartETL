import json
import requests
from types import GeneratorType

from smartetl.util.prompt import template


def invoke_v1(data: str or dict,
              api_base: str = None,
              prompt: str = None,
              variables: list = ['data'], **kwargs):
    """原GoGPT模型服务接口 不支持流式
    :param data 待处理的数据
    :param api_base 服务地址，必须
    :param prompt 提示模板
    :param variables 提示模板中的变量列表 根据这个变量列表进行替换
    :param kwargs 其他参数 请参考具体服务文档
    """
    query_temp = template(prompt, variables=variables)
    query = query_temp(data)
    print('Query:', query)
    # time_start = time.time()
    res = requests.post(api_base, json={"prompt": query}, proxies={})
    # time_end = time.time()
    # print('access', base, 'using', time_end - time_start)
    if res.status_code != 200:
        print('Error', res.text)
        return None
    return res.json()["response"]


def invoke_v2(data: str or dict,
              api_base: str = None,
              local_api: bool = False,
              prompt: str = None,
              variables: list = ['data'],
              model: str = "Qwen2.5-32B-Instruct",
              api_key: str = None,
              stream: bool = False,
              remove_think: bool = False, 
              extra_args: dict = None,
              **kwargs):
    """兼容OpenAI接口的大模型服务调用
    :param data 待处理的数据
    :param api_base 服务地址，必须
    :param local_api 是否为本地API
    :param prompt 提示模板
    :param variables 提示模板中的变量列表 根据这个变量列表进行替换
    :param model 模型名称
    :param api_key API KEY
    :param stream 是否流式 默认False
    :param remove_think 是否移除思考内容
    :param extra_args 大模型额外参数 请参考具体模型平台文档
    :param kwargs 其他参数
    """
    query_temp = template(prompt, variables=variables)
    query = query_temp(data)
    data = {
        "model": model,
        "stream": stream,
        "messages": [
            {
                "role": "user",
                "content": query
            }
        ]
    }
    # other parameters
    if extra_args:
        data.update(extra_args)
    headers = {
        'content-type': 'application/json'
    }

    if api_key:
        headers['Authorization'] = 'Bearer ' + api_key
    res = requests.post(f'{api_base}/chat/completions', headers=headers, json=data, stream=stream)
    if res.status_code != 200:
        return None

    if local_api:
        llm_result = parse_result_local(res, stream=stream, remove_think=remove_think)
    else:
        llm_result = parse_result(res, stream=stream, remove_think=remove_think)

    if isinstance(llm_result, GeneratorType) and not stream:
        return list(llm_result)[-1]
    return llm_result


def parse_result(res, stream: bool = False, remove_think: bool = False):
    if stream is True:
        for chunk in res.iter_lines():
            if not chunk:
                continue
            # print(chunk)
            line = chunk.decode('utf8')
            if not line.startswith("data: {"):
                continue
            json_data = json.loads(line[5:])
            choices = json_data.get("choices")
            if not choices:
                continue
            piece_data = choices[0].get("delta")
            v = piece_data.get("content")
            text_piece = piece_data.get("reasoning_content")
            if not remove_think and text_piece:
                yield text_piece
            if v:
                yield v
    else:
        output = res.json()
        msg = output['choices'][0]['message']
        if msg.get('reasoning_content'):
            if not remove_think:
                yield msg['reasoning_content']
        yield msg['content']


def parse_result_local(res, stream: bool = False, remove_think: bool = False):
    if stream is True:
        think_status = 0
        for chunk in res.iter_lines():
            if not chunk:
                continue
            line = chunk.decode('utf8')
            if not line.startswith("data: {"):
                continue
            json_data = json.loads(line[5:])
            choices = json_data.get("choices")
            if not choices:
                continue
            piece_data = choices[0].get("delta")
            v = piece_data.get("content")
            if not v:
                continue
            if remove_think and think_status < 2:
                # FIX本地部署的大模型接口将思维链通过content字段输出，位于<think></think>中间
                if '<think>' in v:
                    think_status = 1
                    yield v[:v.find('<think>')]
                elif '</think>' in v:
                    think_status = 2
                    yield v[v.find('</think>')+8:]
                elif think_status != 1:
                    yield v
            else:
                yield v
    else:
        output = res.json()
        msg = output['choices'][0]['message']
        text = msg.get('content') or ''
        if remove_think and '</think>' in text:
            text = text[text.rfind('</think>') + 8:].strip()
        yield text


def print_result(data):
    if isinstance(data, GeneratorType):
        for row in data:
            print(row, end='')
        print()
    else:
        print(data)


def invoke_v3(messages: list,
              api_base: str = None,
              local_api: bool = False,
              model: str = "Qwen2.5-32B-Instruct",
              api_key: str = None,
              stream: bool = False,
              remove_think: bool = False, **kwargs):
    """兼容OpenAI接口的大模型服务调用 直接提供消息内容
    :param messages 给大模型的消息
    :param api_base 服务地址，必须
    :param local_api 是否为本地API
    :param model 模型名称
    :param api_key
    :param stream
    :param remove_think
    :param kwargs 其他参数 请参考具体模型平台文档
    """
    data = {
        "model": model,
        "stream": stream,
        "messages": messages
    }
    # other parameters
    data.update(kwargs)
    headers = {
        'content-type': 'application/json'
    }

    if api_key:
        headers['Authorization'] = 'Bearer ' + api_key
    res = requests.post(f'{api_base}/chat/completions', headers=headers, json=data, stream=stream)
    if res.status_code != 200:
        return None

    if local_api:
        llm_result = parse_result_local(res, stream=stream, remove_think=remove_think)
    else:
        llm_result = parse_result(res, stream=stream, remove_think=remove_think)

    if isinstance(llm_result, GeneratorType) and not stream:
        return list(llm_result)[-1]
    return llm_result
