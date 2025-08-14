import sys
from smartetl.apps.wikipedia import *
from smartetl.iterator import Chain, WriteJson, Count


def process_page(input_file: str, output_file: str):
    from smartetl.iterator.wikipedia import Abstract

    processor = Chain(Count(ticks=1000), Abstract(), WriteJson(output_file))
    # WikipediaHTML('data/zhwiki-html')
    page_xml_dump(input_file, processor)


def process_abstract(input_file: str, output_file: str):
    processor = Chain(Count(ticks=1000), WriteJson(output_file))
    abstract_xml_dump(input_file, processor)


if __name__ == '__main__':
    # input_file = r'zhwiki-latest-abstract.xml.gz'
    # input4 = r'data/zhwiki-latest-pages-articles.xml.bz2'
    # process_page(sys.argv[1], sys.argv[2])
    process_abstract(sys.argv[1], sys.argv[2])
