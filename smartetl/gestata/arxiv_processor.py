import logging
from typing import Any, Dict

from smartetl.util.split import simple
from smartetl.gestata.embedding import text_v2, image_v1
from smartetl.processor import Processor
from smartetl.database.elasticsearch import ES
from smartetl.database.qdrant import Qdrant
from smartetl.util.logger import ProductionLogger


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
