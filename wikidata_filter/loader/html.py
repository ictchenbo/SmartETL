# @Time : 2024/11/29 15:09
# @Author : chenbo
# @desc : HTML解析器
# 参考文档: https://www.crummy.com/software/BeautifulSoup/bs4/doc.zh/

import re
from typing import Iterable, Any

from wikidata_filter.loader.file import BinaryFile

# try:
#     import chardet
# except:
#     print("Error! you need to install chardet first: pip install chardet")
#     raise ImportError("chardet not installed")

try:
    from bs4 import BeautifulSoup
except:
    print("Error! you need to install bs4 first: pip install bs4")
    raise ImportError("bs4 not installed")


class HTML(BinaryFile):
    def __init__(self, input_file, encoding='utf8', **kwargs):
        super().__init__(input_file, auto_open=False)
        self.encoding = encoding

        # with open(input_file, 'rb') as f:
        #     encoding = chardet.detect(f.read(1000)).get('encoding', 'utf-8')

    def iter(self) -> Iterable[Any]:
        with open(self.input_file, "r", encoding=self.encoding) as f:
            text = f.read()
            soup = BeautifulSoup(text, 'html.parser')

            title = soup.title.string
            content = soup.body.text()
            if title:
                search_index = re.search(title, content).span()
                if search_index:
                    content = content[search_index[1]:]

            yield {
                "title": title,
                "html": text,
                "text": content
            }
