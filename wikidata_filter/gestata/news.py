"""
提供新闻网页的基本处理，包括文章解析和图片解析
"""
import os
import re
from urllib.parse import urljoin, urlsplit, parse_qs

try:
    from bs4 import BeautifulSoup
except:
    raise ImportError("bs4 not installed")


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
