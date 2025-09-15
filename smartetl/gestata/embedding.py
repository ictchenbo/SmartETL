import requests
import traceback


def text_v1(text: str,
            api_base: str = 'http://10.208.62.156:8001/embed',
            lang: str = "zh",
            **kwargs):
    """简单文本embedding接口"""
    payload = {"text": text, "lang": lang}
    try:
        response = requests.post(api_base, json=payload)
        if response.status_code == 200:
            res = response.json()
            return res
        else:
            print(f"Error: {response.status_code}", response.text)
    except:
        print("Error when access", api_base)

    return []


def text_v2(text: str,
            api_base: str = 'http://10.208.62.156:6008/v1/embeddings',
            model_name: str = 'bge-large-en-v1.5',
            api_key: str = 'sk-aaabbbcccdddeeefffggghhhiiijjjkkk',
            **kwargs):
    """兼容OpenAI的embedding接口，支持选择不同模型"""

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "input": [text],
        "model": model_name
    }
    try:
        response = requests.post(api_base, headers=headers, json=payload)
        if response.status_code == 200:
            res = response.json()
            return res['data'][0]['embedding']
        else:
            print(f"Error: {response.status_code}", response.text)
    except:
        traceback.print_exc()
        print("Error when access", api_base)

    return []


def text_ali_opensearch(text: str or list[str],
                        endpoint: str = None,
                        api_key: str = None,
                        model_name: str = "ops-text-embedding-002",
                        **kwargs):
    """阿里云OpenSearch的embedding接口，支持选择不同模型"""
    url = f'https://{endpoint}/v3/openapi/workspaces/default/text-embedding/{model_name}'

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    json_data = {
        'input': text if isinstance(text, list) else [text],
        'input_type': 'query'
    }

    res = requests.post(url, headers=headers, json=json_data)

    if res.status_code == 200:
        result = res.json()['result']['embeddings']
        vectors = [e['embedding'] for e in result]
        return vectors if isinstance(text, list) else vectors[0]

    return None


def text_v2_batch(rows: list,
                  source_key: str = 'text',
                  target_key: str = 'embed',
                  api_base: str = 'http://10.208.63.29:6008/v1/embeddings',
                  model_name: str = 'bge-large-en-v1.5',
                  api_key: str = 'sk-aaabbbcccdddeeefffggghhhiiijjjkkk',
                  **kwargs):
    """基于兼容OpenAI的embedding接口，进行批量向量化"""

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "input": [row.get(source_key) for row in rows],
        "model": model_name
    }
    try:
        response = requests.post(api_base, headers=headers, json=payload)
        if response.status_code == 200:
            data = response.json()['data']
            assert len(data) == len(rows), "elements number not match"
            for i in range(len(rows)):
                rows[i][target_key] = data[i]['embedding']
        else:
            print(f"Error: {response.status_code}", response.text)
    except:
        print("Error when access", api_base)

    return rows


def image_v1(data: str or bytes,
             api_base: str = 'http://10.208.63.29:8001/embed_image',
             **kwargs):
    """图文多模态的图片embedding接口"""
    if isinstance(data, bytes):
        files = {"file": ("image.jpg", data, "image/jpg")}
    else:
        files = {"file": (data, open(data, "rb"), "image/jpg")}
    try:
        response = requests.post(api_base, files=files)
        if response.status_code == 200:
            res = response.json()
            return res
        else:
            print(f"Error: {response.status_code}", response.text)
    except:
        print("Error when access", api_base)

    return []


def image_text_v1(text: str, api_base: str = 'http://10.208.63.29:8001/embed_image', **kwargs):
    """图文多模态的文本embedding接口"""
    try:
        response = requests.post(api_base, data={"text": text})
        if response.status_code == 200:
            res = response.json()
            return res
        else:
            print(f"Error: {response.status_code}", response.text)
    except:
        print("Error when access", api_base)

    return []


if __name__ == '__main__':
    print(text_v2('I love China'))
