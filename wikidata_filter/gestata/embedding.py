import requests


def text_v1(text: str,
            api_base: str = 'http://10.208.63.29:8001/embed',
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
            api_base: str = 'http://10.208.63.29:6008/v1/embeddings',
            model_name: str = 'bge-large-en-v1.5',
            **kwargs):
    """基于闫强封装的embedding接口，支持选择不同模型"""
    headers = {
        "Authorization": "Bearer sk-aaabbbcccdddeeefffggghhhiiijjjkkk",
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
        print("Error when access", api_base)

    return []


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
