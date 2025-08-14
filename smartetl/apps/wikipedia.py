import re

from smartetl.util.files import open_file
from smartetl.iterator import JsonIterator


def xml_dump_to_json(input_file: str, iterator: JsonIterator, item_parser, item_depth=2):
    import xmltodict
    filename = input_file[input_file.rfind('/')+1:]
    filename = filename[filename.rfind('\\')+1:]
    site = filename.split('-')[0]

    def handle_doc(_, doc):
        row = item_parser(doc, site)
        if row is not None:
            iterator.on_data(row)
        return True

    with open_file(input_file) as stream:
        xmltodict.parse(stream, item_depth=item_depth, item_callback=handle_doc)


title_strip = ['Wikipedia：', '}}', '{{', ': ']


def abstract_xml_dump(input_file: str, iterator: JsonIterator):
    """
    将wikipedia的摘要dump数据导出为JSON
    source:
        https://dumps.wikimedia.org/zhwiki/latest/zhwiki-latest-abstract.xml.gz
        https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz
    """

    def doc_abstract(doc, site):
        title = doc['title'].strip()
        for strip_ch in title_strip:
            title = title.strip(strip_ch)
        title = re.sub(r'[\u200B-\u200D\uFEFF\xa0]', '', title).strip()
        row = {
            "title": title,
            "abstract": doc['abstract'],
            "url": doc["url"],
            "site": site
        }
        return row

    xml_dump_to_json(input_file, iterator, doc_abstract)


def page_xml_dump(input_file: str, iterator: JsonIterator):
    """
    将wikipedia的摘要dump数据导出为JSON
    source:
        https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
        https://dumps.wikimedia.org/zhwiki/latest/zhwiki-latest-pages-articles.xml.bz2
    """
    try:
        import wikitextparser as wtp
    except:
        raise "please install wikitextparser first!"

    def doc_page(doc, site):
        if "title" not in doc or 'revision' not in doc:
            return None
        title = doc['title']
        revision = doc['revision']
        if isinstance(revision, list):
            revision = revision[0]
        markup_text = revision['text']['#text']
        plain_text = wtp.remove_markup(markup_text)

        return {
            "title": title,
            "content": markup_text,
            "text": plain_text,
            "site": site
        }

    xml_dump_to_json(input_file, iterator, doc_page)
