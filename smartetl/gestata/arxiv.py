"""arXiv相关算子"""
import logging
from typing import Any, Dict
import time
import os
import json
import requests
from lxml.html import fromstring, HtmlElement, etree
from smartetl.util.http import image as download_image
from smartetl.util.dates import current_date
from smartetl.gestata.embedding import text_v2, image_v1
from smartetl.gestata.digest import base64
from smartetl.processor import Processor
from smartetl.database.elasticsearch import ES
from smartetl.database.qdrant import Qdrant
from smartetl.util.logger import ProductionLogger
from smartetl.util.split import simple


ARXIV_API_BASE = "http://export.arxiv.org/api/query"
ARXIV_BASE = "http://arxiv.org"
ARXIV_PDF = f'{ARXIV_BASE}/pdf'
ts_file = ".arxiv.ts"


def make_id(filename: str, keep_version: bool = True):
    """
    构造arXiv论文ID：
    arXiv有两种ID，详见https://info.arxiv.org/help/arxiv_identifier.html
    """
    name = filename[:filename.rfind('.')]
    if '/' in name:
        name = name.replace('/', ':')
    if not keep_version and 'v' in name:
        name = name[:name.rfind('v')]
    return 'arxiv:' + name


def search(topic: str, max_results: int = 50):
    """基于arXiv API的论文搜索"""
    import xmltodict
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


def parse_figures(section, base_url: str, image_format: str = None):
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
                if image_format == "base64":
                    fig_info['data'] = "data:image/jpeg;base64," + base64(data)
                else:
                    fig_info['data'] = data
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


def extract_from_html(source: str, base_url: str = None, image_format: str = None):
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
            'figures': parse_figures(section, base_url, image_format=image_format) if base_url else [],
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
            url_key: str = "url_html",
            image_key: str = None,
            image_format: str = None,
            **kwargs):
    """
    对输入的arxiv论文字典对象进行解析（假设其包含html网页正文字段及其url字段）
    输出解析后的结果
    """
    html = row.get(content_key)
    if not html:
        print('当前论文id没有html文件')
        return None
    if isinstance(html, bytes):
        html = html.decode('utf8')
    base_url = row.get(url_key)
    paper_info = extract_from_html(html, base_url, image_format=image_format)
    if image_key:
        images = []
        for section in paper_info.get("sections"):
            for figure in section.get("figures"):
                if "data" in figure:
                    images.append(figure['data'])
        row[image_key] = images
    return paper_info


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


def url4html(_id: str):
    return f"{ARXIV_BASE}/html/{_id}"


def url4pdf(_id: str):
    return f"{ARXIV_BASE}/pdf/{_id}"


class WriteCount(Processor):
    """
    当前处理的数量写入文件，防止程序中断从中断位置继续处理
    """

    def __init__(self, path):
        self.count = 0
        self.path = path
        try:
            with open(self.path, 'rb') as f:
                self.count = int(f.read().decode())
        except (FileNotFoundError, ValueError):
            self.count = 0

    def on_data(self, data: Any, *args):
        self.count += 1
        with open(self.path, 'wb') as f:
            f.write(str(self.count).encode())
        return data


class ArxivPro(Processor):
    """
    arxiv html页面数据处理类
    """

    def on_data(self, data: Any, *args):
        paper = data['paper']
        content = ""
        if paper:
            abstract = paper.get('abstract')
            content += f"{abstract}\n"
            sections = paper.get('sections')
            for section in sections:
                content += f"{section['content']}\n"
        data['content'] = content
        return data


class ArxivImage(Processor):
    """
    arxiv html页面数据处理类
    """

    def on_data(self, data: Any, *args):
        paper = data['paper']
        img_url_list = []
        if paper:
            sections = paper.get('sections')
            for section in sections:
                figures = section.get('figures')
                for image in figures:
                    img_url = image.get('url')
                    if img_url:
                        img_url_list.append(img_url)
        data['image_urls'] = img_url_list
        return data


class ArxivProcess(Processor):
    """
    arxiv html页面数据处理类
    """
    def __init__(self,
                 text_embedding_api: str,
                 image_embedding_api: str = None,
                 qd_config: Dict[str, Any] = None,
                 es_config: Dict[str, Any] = None,
                 **kwargs):
        self.text_embedding_api = text_embedding_api
        self.image_embedding_api = image_embedding_api
        self.headers = {}
        self.qdrant_writer = Qdrant(**qd_config)
        self.ESWriter = ES(**es_config)
        self.es_index = 'arxiv_extract_html_v2'
        self.logger = ProductionLogger(name="arxiv_pro", log_level=logging.DEBUG,
                                       log_file="logs/arxiv_pro.log",
                                       json_format=True,
                                       extra_fields={"app_version": "1.0.0", "environment": "production"})

    def on_data(self, data: Any, *args):
        paper = data['paper']
        try:
            _id = int(data['id'].replace('.', ''))
        except:
            return {}
        if paper:
            abstract = paper.get('abstract')
            self.embed_and_write2(_id=_id, content=abstract, collection='chunk_arxiv_abstract2505')
            sections = paper.get('sections')
            all_figures = []
            for index, section in enumerate(sections, start=1):
                figures = section.get('figures')
                if figures:
                    all_figures.extend(figures)
                title = section['title'].lower()
                content = section['content']
                collection = 'chunk_arxiv_discusss2505'
                if 'introduction' in title:
                    collection = 'chunk_arxiv_introduction2505'
                elif 'method' in title:
                    collection = 'chunk_arxiv_method2505'
                elif 'experiment' in title:
                    collection = 'chunk_arxiv_experiment2505'
                self.embed_and_write2(_id=_id, content=content, collection=collection)
                self.ESWriter.index_name = self.es_index
                self.ESWriter.write_batch(rows=[paper])
            if all_figures and self.image_embedding_api:
                self.image_embed_and_write(_id=_id, figures=all_figures)
        return data

    def embed_and_write2(self, _id: int, content: str, collection: str):
        """文本向量化"""
        emb_data = []
        for i, chunk in enumerate(simple(content)):
            emb_data.append({
                "id": int(f"{_id * 100}{i:02d}"),
                "content": chunk,
                "vector": text_v2(chunk, self.text_embedding_api)
            })
        self.qdrant_writer.collection = collection
        write_qdrant_msg = self.qdrant_writer.upsert(emb_data)
        self.logger.info(f"内容向量化写入qdrant库  {write_qdrant_msg}")

    def image_embed_and_write(self, _id: int, figures: list):
        """图片向量化"""
        emb_data = []
        for i, fig in enumerate(figures, start=1):
            if not fig.get('data'):
                continue
            data = fig.pop('data')
            caption = fig.get('caption')
            emb_data.append({
                "id": int(f"{_id * 100}{i:02d}"),
                "content": caption,
                "vector": image_v1(data, self.image_embedding_api)
            })
        self.qdrant_writer.collection = "figure_arxiv_2504_2"
        write_qdrant_msg = self.qdrant_writer.upsert(emb_data)
        self.logger.info(f"图片向量化写入qdrant库  {write_qdrant_msg}")


class Task:
    def __init__(self, start_month: int = None, end_month: int = None):
        self.month = start_month or 2501
        self.seq = 1
        if os.path.exists(ts_file):
            with open(ts_file, encoding="utf8") as fin:
                nums = json.load(fin)
                self.month = nums[0]
                self.seq = nums[1]
        self.end_month = end_month or int(current_date('%y%m'))
        print(f"from {self.month}.{self.seq} to {self.end_month}")

    def write_ts(self):
        row = [self.month, self.seq]
        with open(ts_file, "w", encoding="utf8") as out:
            json.dump(row, out)

    def __call__(self, *args, **kwargs):
        while self.month <= self.end_month:
            while self.seq < 30000:
                url = f'{ARXIV_PDF}/{self.month}.{self.seq:05d}'
                print("processing:", url)
                yield url
                self.seq += 1
                # 记录更新后的TS（即下次任务TS），如果被kill，下次启动不会重复处理
                self.write_ts()
                time.sleep(1)
            self.month += 1
            if self.month % 100 > 12:
                self.month += int(self.month/100)*100 + 101
            self.seq = 1
            self.write_ts()
