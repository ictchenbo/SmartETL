"""网页爬虫相关算子"""
from urllib.parse import urljoin, urlparse

try:
    from bs4 import BeautifulSoup
except:
    raise ImportError("bs4 not installed")


def extract_content(source: str):
    """提取页面标题和正文"""
    soup = BeautifulSoup(source, 'html.parser')

    title = soup.title.string if soup.title else ''

    # 多种策略提取正文
    article = soup.find('article')
    if article:
        text = ' '.join(article.stripped_strings)
        return {'title': title, 'content': text}

    main = soup.find('main')
    if main:
        text = ' '.join(main.stripped_strings)
        return {'title': title, 'content': text}

    divs = soup.find_all('div')
    best_div = None
    max_length = 0

    for div in divs:
        length = len(' '.join(div.stripped_strings))
        if length > max_length:
            max_length = length
            best_div = div

    if best_div and max_length > 100:
        text = ' '.join(best_div.stripped_strings)
        return {'title': title, 'content': text}

    body = soup.body
    if body:
        text = ' '.join(body.stripped_strings)
        return {'title': title, 'content': text}

    return {'title': title, 'content': ''}


def is_valid_url(url: str, domain: str) -> bool:
    """检查URL是否有效且属于同一域名"""
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return False

    if parsed.scheme not in ('http', 'https'):
        return False

    if parsed.netloc != domain:
        return False

    path = parsed.path.lower()
    if path.endswith(('.jpg', '.jpeg', '.png', '.gif', '.pdf', '.doc', '.docx',
                      '.xls', '.xlsx', '.ppt', '.pptx', '.mp3', '.mp4', '.avi',
                      '.mov', '.zip', '.rar', '.exe', '.dmg')):
        return False

    if '.' in path.split('/')[-1]:
        ext = path.split('.')[-1]
        if ext not in ('html', 'htm', 'php', 'asp', 'aspx', 'jsp'):
            return False

    return True


def extract_links(source: str, base_url: str):
    """提取页面的链接"""
    domain = urlparse(base_url).netloc
    links = set()
    soup = BeautifulSoup(source, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()

        if href.startswith('#'):
            continue

        full_url = urljoin(base_url, href)
        full_url = full_url.split('#')[0]

        if is_valid_url(full_url, domain):
            links.add(full_url)

    return links


def extract_from_html(source: str, base_url: str = None):
    """提取网页信息"""
    content = extract_content(source)
    links = extract_links(source, base_url)

    return {
        'url': base_url,
        'title': content['title'],
        'content': content['content'],
        'links': list(links)
    }


def extract(row: dict,
            content_key: str = "content",
            base_url_key: str = "url_html",
            **kwargs):
    if not row.get(content_key):
        print('没有html')
        return None
    html = row[content_key]
    base_url = row[base_url_key]

    return extract_from_html(html, base_url)
