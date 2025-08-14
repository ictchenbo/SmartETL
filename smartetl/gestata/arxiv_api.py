"""arXiv相关算子"""
import arxiv
from tqdm import tqdm


def search(topic: str, max_papers: int = 50):
    """基于arXiv API的论文搜索"""
    _search = arxiv.Search(
        query=topic,
        max_results=max_papers,
        sort_by=arxiv.SortCriterion.Relevance
    )
    client = arxiv.Client()

    for i, result in enumerate(tqdm(client.results(_search), desc=f"正在下载主题 '{topic}' 的论文")):
        yield {
            "_id": result.get_short_id(),
            "entry_id": result.entry_id,
            "updated": str(result.updated),
            "published": str(result.published),
            "title": result.title,
            "authors": [author.name for author in result.authors],
            "summary": result.summary,
            "comment": str(result.comment),
            "journal_ref": str(result.journal_ref),
            "doi": str(result.doi),
            "primary_category": result.primary_category,
            "categories": result.categories,
            "links": [{"title": link.title, "href": link.href, "rel": link.rel} for link in result.links],
            "pdf_url": result.pdf_url
        }
