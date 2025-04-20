import logging
from typing import Any, Dict
import requests


from wikidata_filter.gestata.embedding import text_v2
from wikidata_filter.iterator import JsonIterator
from wikidata_filter.iterator.database import ESWriter
from wikidata_filter.iterator.database.qdrant import Qdrant
from wikidata_filter.iterator.model.embed import Local
from wikidata_filter.util.logger import ProductionLogger
from wikidata_filter.util.split import simple


def splicing_characters(string_data: str):
    import time
    time.sleep(7)
    # string_data = "2010.15768"
    return f"https://arxiv.org/html/{string_data}"


class WriteCount(JsonIterator):
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


class ArxivPro(JsonIterator):
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


class ArxivImage(JsonIterator):
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


class ArxivProcess(JsonIterator):
    """
    arxiv html页面数据处理类
    """

    def __init__(self, bge_large_zh: str, qdrant: Dict[str, Any] = None, es_config: Dict[str, Any] = None):

        self.embed_local = Local(api_base=bge_large_zh, key='content', target_key='vector')

        self.bge_large_zh = bge_large_zh  # 向量化服务ip
        self.headers = {}
        self.qdrant_writer = Qdrant(**qdrant, buffer_size=1)
        self.ESWriter = ESWriter(**es_config)
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
            self.embed_and_write2(index=1, _id=_id, content=abstract, collection='chunk_arxiv_abstract2505')
            sections = paper.get('sections')
            for index, section in enumerate(sections,start=1):
                figures = section.get('figures')
                if figures:
                    self.image_embed_and_write(_id=_id, index=index, figures=figures)
                title = section['title'].lower()
                content = section['content']
                collection = 'chunk_arxiv_discusss2505'
                if 'introduction' in title:
                    collection = 'chunk_arxiv_introduction2505'
                elif 'method' in title:
                    collection = 'chunk_arxiv_method2505'
                elif 'experiment' in title:
                    collection = 'chunk_arxiv_experiment2505'
                self.embed_and_write2(index=index, _id=_id, content=content, collection=collection)
                self.ESWriter.index_name = self.es_index
                es_data = [paper]
                self.ESWriter.write_batch(rows=es_data)
        return data

    def embed_and_write(self, index: int, _id: int, content: str, collection: str):
        """arxiv内容向量化方法"""
        chunks = simple(content=content, max_length=1000)
        for _i, chunk in enumerate(chunks):
            emb_data = {
                "id": int(f"{_id * 100}{index}{_i}"),
                "content": chunk
            }
            embed_data = self.embed_local.on_data(emb_data)
            self.qdrant_writer.collection = collection
            insert_data = [embed_data]
            write_qdrant_msg = self.qdrant_writer.write_batch(insert_data)
            self.logger.info(f"内容向量化写入qdrant库  {write_qdrant_msg}")
            print(write_qdrant_msg)

    def embed_and_write2(self, index: int, _id: int, content: str, collection: str):
        """arxiv内容向量化方法"""

        emb_data = {
            "id": int(f"{_id * 100}{index}"),
            "content": content
        }
        emb_content = text_v2(content)
        self.qdrant_writer.collection = collection

        emb_data['vector'] = emb_content
        insert_data = [emb_data]
        write_qdrant_msg = self.qdrant_writer.write_batch(insert_data)
        self.logger.info(f"内容向量化写入qdrant库  {write_qdrant_msg}")
        print(write_qdrant_msg)

    def image_embed_and_write(self, _id: int, index: int, figures: list):
        """
        图片向量化方法
        """
        if figures:
            for _i, fig in enumerate(figures, start=1):
                data = fig.get('data')
                caption = fig.get('caption')
                emb_data = {
                    "id": int(f"{_id * 100}{index}{_i}"),
                    "content": caption,
                }
                # 调用向量化方法
                emb_content = text_v2(data)
                emb_data['vector'] = emb_content
                self.qdrant_writer.collection = "figure_arxiv_2504_2"
                insert_data = [emb_data]
                print(self.qdrant_writer.collection )
                write_qdrant_msg = self.qdrant_writer.write_batch(insert_data)
                print(f"图片向量化写入qdrant库  {write_qdrant_msg}")
                self.logger.info(f"图片向量化写入qdrant库  {write_qdrant_msg}")
                if data:
                    fig.pop('data')

    def vectorization_method(self, _id, content):
        """"""
        res = requests.post(self.bge_large_zh, json={'text': content})
        if res.status_code == 200:
            vector_data = res.json()
            row = [{
                'id': _id,
                'vector': vector_data,
                'content': content
            }]

            return row
        else:
            print('向量化web服务请求失败')
            return None

    def insert_qdrant(self, vector_data, collection):
        """
        数据写入向量库
        """
        data = {
            "points": vector_data
        }
        res = requests.put(f'{self.qdrant_api_base}/collections/{collection}/points', json=data,
                           headers=self.headers)
        if res.status_code == 200:
            data = res.json()
            if data['status'] == 'ok':
                print(f'向量化入库成功: {collection}')
            else:
                print(f'向量化入库失败: {collection}')
