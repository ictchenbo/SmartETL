"""
新闻网页的解析处理算子
"""
import os
import re
import traceback
from urllib.parse import urljoin, urlsplit, parse_qs
import requests

from smartetl.util.dates import normalize_time
from smartetl.integrations.gne import GeneralNewsExtractor
from .html import BeautifulSoup, HtmlExtractor

article_classes = ['article-body', 'content', 'news-content', 'post-content', 'entry', "post-image", "imgboxa",
                  "pageImg main", "article-image-in-body", "relative", "pt-4 md:pt-10"]
article_tags = ['article']
article_ids = ['content']


def extract_article(source: str):
    """ 从新闻HTML中提取网页"""
    # 使用BeautifulSoup解析HTML
    soup = BeautifulSoup(source, 'lxml')

    # 尝试多种方法来定位正文部分
    article = None

    # 尝试查找常见的class名
    for class_name in article_classes:
        article = soup.find('div', class_=class_name)
        if article:
            # print(f"通过class找到article区域: {class_name}")
            break

    # 如果还是没有找到article，尝试寻找article标签
    if not article:
        for tag in article_tags:
            article = soup.find(tag)
            if article:
                break
                # print(f"通过<{tag}>标签找到正文部分")

    # 如果还是没有找到，尝试寻找id='content'的标签
    if not article:
        for _id in article_ids:
            article = soup.find(id=_id)
            if article:
                break
                # print("通过id='content'找到正文部分")

    return article


def clean_filename(url):
    """ 清理URL中的无效字符，生成合法的文件名 """
    # 如果URL中有查询参数，尝试从中提取实际的文件URL
    if '?' in url:
        query_params = parse_qs(urlsplit(url).query)
        if 'url' in query_params:
            url = query_params['url'][0]

    # 获取文件名并清理非法字符
    path = urlsplit(url).path
    filename = os.path.basename(path)
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', filename)  # 清除非法字符
    return filename


def get_extension_from_url(img_url):
    """ 从 URL 提取有效的文件扩展名，去掉查询参数部分 """
    # 使用正则表达式提取扩展名
    match = re.search(r'\.(jpg|jpeg|png|gif|bmp|webp|avif|ico|apng|svg)(?=\?|$)', img_url.lower())
    if match:
        return match.group(0)


def images(article: dict,
           url_key: str = "url",
           html_key: str = "html",
           **kwargs):
    """ 从正文部分提取图片、描述和上下文 """
    url = article[url_key]
    html = article[html_key]
    e_article = extract_article(html)
    if not e_article:
        return
    # 查找所有的img标签，包括可能的data-src属性
    img_tags = e_article.find_all('img')
    for img in img_tags:
        img_url = img.get('src') or img.get('data-src')  # 获取图片URL（可能存储在data-src属性中）
        if not img_url:
            continue
        # TODO 如果是base64编码的图片数据
        if img_url.startswith('data:image'):
            continue

        img_url = urljoin(url, img_url)  # 转换为绝对路径

        # 获取图片的描述信息
        img_desc = img.get('alt') or img.get('title') or img.get("figcaption class") or img.get(
            "wp-caption-text")  # 从alt或title属性获取描述
        img_desc = img_desc if img_desc else "无描述"

        # 获取图片所在位置的上下文信息
        img_context = ""

        # 获取图片的父元素
        parent = img.find_parent()

        # 尝试获取图片所在段落或容器中的文本
        if parent:
            # 如果图片在一个段落中，尝试获取整个段落文本
            if parent.name == 'p' or parent.name == 'div':
                img_context = parent.get_text(strip=True)

            # 如果上下文为空，尝试从父元素的兄弟节点或父容器中获取更多信息
            if not img_context:
                # 只获取父元素之后的第一个兄弟节点的文本，避免重复信息
                siblings_text = ""
                for sibling in parent.find_all_next(limit=3):  # 限制最多获取3个后续兄弟节点
                    if sibling.name in ['p', 'div', 'span']:
                        siblings_text += sibling.get_text(strip=True) + " "
                img_context = siblings_text.strip()

        # 如果上下文为空，尝试获取图片前后的元素文本
        if not img_context:
            prev_element = img.find_previous(text=True)
            next_element = img.find_next(text=True)
            img_context = (prev_element or '') + " " + (next_element or '')

        # 去除上下文和描述重复的情况
        if img_desc in img_context:
            img_context = img_context.replace(img_desc, "").strip()

        img_name = clean_filename(img_url)
        ext = '.png'
        if '.' in img_name:
            ext = img_name[img_name.rfind('.'):].lower()
        # get_extension_from_url(img_url) or '.png'

        yield {
            "url": img_url,
            "name": img_name,
            "ext": ext,
            "desc": img_desc if img_desc else "无描述",
            "context": img_context.strip() if img_context else "无上下文"
        }


def wrap_snippet(html: str):
    return f'<html><body>{html}</body></html>'


def extract(html: str, doc: dict = None, is_snippet: bool = False):
    """从HTML中提取元数据 并与doc进行合并后返回。注意：时间提取的顺序"""
    if is_snippet:
        html = wrap_snippet(html)
    doc = doc or {}
    my_extractor = HtmlExtractor(html)
    my_extractor.parse_meta()

    news = {}
    news['meta'] = my_extractor.meta
    news['keywords'] = my_extractor.find_value_from_meta("keywords")
    news['desc'] = my_extractor.find_value_from_meta("description")
    news['source'] = my_extractor.find_value_from_meta('site_name') or my_extractor.find_value_from_meta('source')
    news['author'] = my_extractor.find_value_from_meta('author')

    for key in ["source", "author", "keywords"]:
        if not news.get(key) and key in doc:
            news[key] = doc[key]

    title = my_extractor.find_value_from_meta("title") or doc.get("title") or my_extractor.get_title()
    if '|' in title:
        title = title[:title.find('|')].strip()
    news["title"] = title
    news["content"] = doc.get("content")

    pt = doc.get("publish_time") or doc.get("time")
    if pt:
        # TODO 对于非ISO格式的时间 如何判断时区？这里假设为UTC
        news['origin_publish_time'] = pt
        news['publish_time'] = normalize_time(pt)

    return news


gne_extractor = GeneralNewsExtractor()


def gne_extract(html: str, is_snippet: bool = False, **kwargs):
    """基于扩展的JNE组件进行新闻信息抽取"""
    if is_snippet:
        html = wrap_snippet(html)
    try:
        result = gne_extractor.extract(html)
    except:
        traceback.print_exc()
        result = {}
    return extract(html, result)


def constor_extract(html: str, api_base: str, request_timeout: int = 120, is_snippet: bool = False, **kwargs):
    """调用Constor组件服务进行新闻信息抽取"""
    if is_snippet:
        html = wrap_snippet(html)
    result = {}
    for i in range(3):
        try:
            r = requests.post(api_base, json=[html], timeout=request_timeout)
            if r.status_code == 200:
                result = r.json()[0]
            else:
                print(f'error response from gdelt parser. {r.status_code}:{r.text}')
        except:
            traceback.print_exc()
            print("Constor服务异常")
    return extract(html, result)
