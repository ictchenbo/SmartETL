"""
读取pdf文件 按页输出文本内容

{'page_num': pageNum, "content": texts}

@dependencies pdfminer.six
"""
from typing import Iterable, Any
from smartetl.loader.file import BinaryFile

try:
    import pdfminer
except:
    print("Error! you need to install pdfminer first: pip install pdfminer.six")
    raise ImportError("pdfminer.six not installed")

from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.pdfpage import PDFPage, PDFTextExtractionNotAllowed
from pdfminer.converter import PDFPageAggregator


class PDF(BinaryFile):
    """基于pdfminer读取pdf文件"""
    def __init__(self, input_file: str, max_pages: int = 0, min_length: int = 10, **kwargs):
        super().__init__(input_file)
        self.max_pages = max_pages
        self.min_length = min_length

    def iter(self) -> Iterable[Any]:
        parser = PDFParser(self.instream)
        document = PDFDocument(parser)

        if not document.is_extractable:
            raise PDFTextExtractionNotAllowed

        resmag = PDFResourceManager()
        laparams = LAParams()
        device = PDFPageAggregator(resmag, laparams=laparams)
        interpreter = PDFPageInterpreter(resmag, device)

        pageNum = 0
        for page in PDFPage.create_pages(document):
            if 0 < self.max_pages <= pageNum:
                break
            interpreter.process_page(page)
            layout = device.get_result()
            texts = []
            for y in layout:
                if isinstance(y, LTTextBoxHorizontal):
                    text = y.get_text().strip()
                    if self.min_length > 0 and len(text) < self.min_length:
                        continue
                    texts.append(text)
            yield {'page': pageNum, 'content': texts}
            pageNum += 1
