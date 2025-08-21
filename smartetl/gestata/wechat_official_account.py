"""
基于极致了数据进行微信公众号数据采集接入（需要注册获取api_key，同时充值）
详情查看：https://www.dajiala.com/main/
"""
import json
import requests
from typing import Dict, Optional

# 常量定义
BASE_URL = "https://www.dajiala.com/fbmain/monitor/v3/post_history"
ARTICLE_DETAIL_URL = "https://www.dajiala.com/fbmain/monitor/v3/article_detail"


def news(account: dict, api_key: str = None, fetch_size: int = 100, max_pages: int = 20):
    """爬取一个公众号的新闻列表"""
    article_count = 0
    current_page = 1
    name = account.get('name')
    link = account.get('link')
    while article_count < fetch_size and current_page <= max_pages:
        # 获取列表
        page_data = get_article_history(current_page, account_name=name, account_link=link, api_key=api_key)
        if not page_data:
            break

        article_list = page_data.get("data", [])
        if not article_list:
            break

        # 获取详情
        batch = []
        for article in article_list:
            url = article.get("url")

            if not url:
                continue

            detail = get_article_detail(article["url"], api_key=api_key)
            if not detail:
                continue

            batch.append(detail)

        if not batch:
            break

        yield batch

        article_count += len(batch)
        current_page += 1


def get_article_history(page: int, account_name: str = None, account_link: str = None, api_key: str = None) -> Optional[Dict]:
    """获取公众号历史文章列表"""
    payload = {
        "biz": "",
        "url": account_link or "",
        "name": account_name or "",
        "page": page,
        "key": api_key,
        "verifycode": ""
    }

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(BASE_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == 0:
            return data
        print("获取文章详情失败：", data.get('code'), data.get('msg'))
    except requests.exceptions.RequestException as e:
        print(f"获取历史文章失败: {e}")
        return None
    except json.JSONDecodeError:
        print("历史文章响应解析失败")
        return None


def get_article_detail(url: str, api_key: str = None) -> Optional[Dict]:
    """获取单篇文章详情"""
    try:
        params = {
            "url": url,
            "key": api_key,
            "mode": "2"
        }
        response = requests.get(ARTICLE_DETAIL_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == 0:
            return data
        print("获取文章详情失败：", data.get('code'), data.get('msg'))
        return None
    except requests.exceptions.RequestException as e:
        print(f"获取文章详情失败: {e}")
        return None
    except json.JSONDecodeError:
        print("文章详情响应解析失败")
        return None
