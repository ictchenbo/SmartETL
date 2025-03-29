"""arXiv相关算子"""
import requests
import xmltodict

ARXIV_API_BASE = "http://export.arxiv.org/api/query"
ARXIV_BASE = "http://arxiv.org"


def search(topic: str, max_results: int = 50, search_field: str = "all"):
    """基于arXiv API的论文搜索"""
    query = {
        "search_query": f"{search_field}:{topic}",
        "max_results": max_results,
        "sortBy": "lastUpdatedDate",
        "sortOrder": "descending"
    }
    res = requests.get(ARXIV_API_BASE, params=query)
    # 注意，xmltodict.parse()非流式，仅适合小文件
    doc = xmltodict.parse(res.text)
    feed = doc.get("feed")
    papers = feed.get("entry")
    for paper in papers:
        _id = paper["id"]
        _id = _id[_id.rfind('/')+1:]
        paper["_id"] = _id
        paper["url_pdf"] = f"{ARXIV_BASE}/pdf/{_id}"
        paper["url_html"] = f"{ARXIV_BASE}/html/{_id}"

    return papers
