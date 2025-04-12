"""arXiv相关算子"""
import requests
import xmltodict
import base64
from lxml.html import fromstring, HtmlElement, etree
from wikidata_filter.util.http import download_image


ARXIV_API_BASE = "http://export.arxiv.org/api/query"
ARXIV_BASE = "http://arxiv.org"


def search(topic: str, max_results: int = 50):
    """基于arXiv API的论文搜索"""
    if ':' not in topic:
        topic = 'all:' + topic
    params = {
        "search_query": topic,
        "max_results": max_results,
        "sortBy": "lastUpdatedDate",
        "sortOrder": "descending"
    }
    res = requests.get(ARXIV_API_BASE, params=params)
    # 注意，xmltodict.parse()非流式，仅适合小文件
    doc = xmltodict.parse(res.text)
    # print("---debug---", doc)
    feed = doc.get("feed")
    if "entry" not in feed:
        return []
    papers = feed.get("entry")
    for paper in papers:
        # print(paper)
        _id = paper["id"]
        _id = _id[_id.rfind('/')+1:]
        paper["_id"] = _id
        paper["url_pdf"] = f"{ARXIV_BASE}/pdf/{_id}"
        paper["url_html"] = f"{ARXIV_BASE}/html/{_id}"

    return papers


def join_para(s: list):
    s1 = [si.strip() for si in s]
    return '\n'.join(s1).strip()


def parse_figures(section, base_url: str):
    """
        基于arxiv官网HTML网页抽取论文中的图
        注意：网页布局可能发生变化，注意检查更新
    """
    section_images = section.xpath('.//figure[contains(@class, "ltx_figure")]')
    images = []
    for fig in section_images:
        fig_info = {}
        caption = fig.xpath('.//figcaption')
        if caption:
            fig_info['caption'] = caption[0].text_content().strip()

        img = fig.xpath('.//img')
        if img:
            url = img[0].get('src', '')
            fig_info['url'] = base_url + '/' + url
            data = download_image(fig_info['url'])
            if data:
                fig_info['data'] = base64.b64encode(data).decode('utf-8')
        images.append(fig_info)

    return images


def parse_tables(section):
    """
    基于arxiv官网HTML网页抽取论文中的表格
    注意：网页布局可能发生变化，注意检查更新
    """
    tables = section.xpath('.//figure[contains(@class, "ltx_table")]')
    ret = []
    for table in tables:
        table_info = {
            'caption': '',
            'rows': []
        }

        # 提取表格标题
        caption = table.xpath('figcaption')
        if caption:
            table_info['caption'] = caption[0].text_content().strip()

        table_e = table.xpath('.//table')
        if not table_e:
            continue

        rows = table_e[0].xpath('.//tr')
        for row in rows:
            cells = row.xpath('td | th')
            cell_list = []
            for cell in cells:
                cell_value = {
                    "v": cell.text_content().strip()
                }
                if cell.get("rowspan"):
                    cell_value["rowspan"] = int(cell.get("rowspan"))
                if cell.get("colspan"):
                    cell_value["colspan"] = int(cell.get("colspan"))
                cell_list.append(cell_value)

            table_info['rows'].append(cell_list)

        ret.append(table_info)

    return ret


def extract_from_html(source: str, base_url: str):
    """
    基于arxiv官网HTML网页抽取论文信息，参考：https://arxiv.org/html/2503.15454v3
    注意：网页布局可能发生变化，注意检查更新
    """
    tree = fromstring(source)

    # 初始化结果字典
    paper_info = {
        'title': '',
        'authors': [],
        'abstract': '',
        'sections': [],
        'appendices': [],
        'references': []
    }

    article = tree.xpath('//article[contains(@class, "ltx_document")]')
    if not article:
        return None

    article = article[0]

    # 1. 提取标题
    title_element = article.xpath('.//h1[contains(@class, "ltx_title")]')
    if title_element:
        paper_info['title'] = title_element[0].text_content().replace('Title:', '').strip()

    # 2. 提取作者
    authors_elements = article.xpath('.//span[contains(@class, "ltx_role_author")]')
    paper_info['authors'] = [author.text_content().strip() for author in authors_elements]

    # 3. 提取摘要
    abstract_element = article.xpath('.//div[contains(@class, "ltx_abstract")]')
    if abstract_element:
        paper_info['abstract'] = abstract_element[0].text_content().replace('Abstract', '').strip()

    # 4. 提取正文各章节
    sections = article.xpath('.//section[contains(@class, "ltx_section")]')
    for i, section in enumerate(sections):
        section_title = section.xpath('.//h2[contains(@class, "ltx_title_section")]//text()')
        section_content = section.xpath('.//div[contains(@class, "ltx_para")]//text()')

        title = join_para(section_title)
        content = join_para(section_content)
        if "Abstract" in title:
            paper_info['abstract'] = content
            continue

        paper_info['sections'].append({
            'title': title,
            'content': content,
            'figures': parse_figures(section, base_url),
            'tables': parse_tables(section)
        })

    # 5. 提取参考文献
    ref_sections = article.xpath('.//li[contains(@class, "ltx_bibitem")]')
    for ref_section in ref_sections:
        text = (ref_section.text_content()
                .replace('\n', ' ')
                .replace('\u00a0', ' ')
                .strip())
        paper_info['references'].append(text)

    return paper_info


def extract(row: dict,
            content_key: str = "content",
            base_url_key: str = "url_html",
            **kwargs):
    """
    对输入的arxiv论文字典对象进行解析（假设其包含html网页正文字段及其url字段）
    输出解析后的结果
    """
    html = row[content_key]
    base_url = row[base_url_key]
    return extract_from_html(html, base_url)


def from_meta(row: dict):
    """添加URL"""
    _id = row["id"]
    versions = row["versions"]
    latest_version = versions[-1]["version"]
    _id = _id + latest_version
    row["_id"] = _id
    row["url_pdf"] = f"{ARXIV_BASE}/pdf/{_id}"
    row["url_html"] = f"{ARXIV_BASE}/html/{_id}"
    return row
