"""
jina.ai see https://jina.ai/reader/
"""
from wikidata_filter.util.http import text, json


def reader(url: str, *args, api_key: str = None, **kwargs):
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    _url = f'https://r.jina.ai/{url}'

    return text(_url, headers=headers)


def search(query: str, *args, api_key: str = None, **kwargs):
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    _url = f'https://s.jina.ai/{query}'
    return text(_url, headers=headers)


def grounding(statement: str, *args, api_key: str = None, **kwargs):
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    json_data = {"statement": statement}
    res = json('https://g.jina.ai/', json=json_data, headers=headers)
    return res.get("data") if res else None
