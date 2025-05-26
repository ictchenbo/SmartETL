import requests


def invoke(prompt: str,
           api_base: str):
    data = {
        "model": "Qwen2.5-32B-Instruct",
        "prompt": prompt,
        "max_tokens": 1024,
        "temperature": 0
    }
    res = requests.post(api_base, json=data)
    if res.status_code == 200:
        output = res.json()
        return output['choices'][0]['text']
    return None
