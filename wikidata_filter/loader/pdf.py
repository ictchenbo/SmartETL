"""
读取pdf文件 按页输出文本内容

{'page_num': pageNum, "content": texts}

@dependencies pdfminer.six
"""
from typing import Iterable, Any
from wikidata_filter.loader.file import BinaryFile

try:
    import pdfminer
except:
    print("Error! you need to install pdfminer first: pip install pdfminer.six")
    raise ImportError("pdfminer.six not installed")

try:
    import pdfplumber
except:
    print("Error! you need to install pdfplumber first: pip install pdfplumber")
    raise ImportError("pdfplumber not installed")

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


class PDF2(BinaryFile):
    """
    解析处理pdf文件，返回统一树状结构json数据
    有两种模式可以选择，model='tree'时，可以把带有标题层级结构的论文格式的pdf文件解析
    成树状结构的json数据保存，model='json'时可以把特殊格式的pdf文件的内容按页提取出来，
    并且把每一页的表格内容提取出来并保存
    """

    def __init__(self, input_file: str, mode: str = "tree", **kwargs):
        super().__init__(input_file)
        self.mode = mode

    def iter(self):
        if self.mode == "tree":
            full_text = self.extract_pdf_text(self.input_file)
            tree = self.parse_text_structure(full_text)
            yield tree
        else:
            json = self.pdf_to_json(self.input_file)
            yield json
    def extract_pdf_text(self, pdf_path: str):
        """使用pdfplumber提取PDF文本内容"""
        with pdfplumber.open(pdf_path) as pdf:
            return "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])

    def parse_text_structure(self, text: str):
        """解析文本结构生成树状数据"""
        import re
        root = []
        # 记录当前各层级的节点结构
        current_hierarchy = {}
        #记录每一行的内容
        pending_content = []
        for line in text.split('\n'):
            line = line.strip()
            if not line:
                continue
            # 匹配标题（支持多级编号）
            title_match = re.match(r'^(\d+(?:\.\d+)*)\.?\s+(.*)', line)
            if title_match:
                title_num = title_match.group(1)
                title_text = title_match.group(0).strip()
                # 根据编号计算层级
                level = len(title_num.split('.'))
                # 结束当前内容块，将内容分配给当前节点
                self.finalize_content(pending_content, current_hierarchy)
                # 创建新节点
                node = {
                    "title": title_text,
                    "level": level,
                    "content": "",
                    "children": []
                }
                # 寻找父节点
                parent_level = level - 1
                parent = current_hierarchy.get(parent_level)
                # 将当前节点添加到父节点的children中
                if parent:
                    parent["children"].append(node)
                else:
                    root.append(node)  # 如果没有父节点，则添加到根节点
                # 更新当前层级
                current_hierarchy[level] = node
                # 清空更深层级的缓存
                for le in range(level + 1, 6):
                    current_hierarchy.pop(le, None)
                continue
            # 处理内容行
            pending_content.append(line)
        # 处理最后一段内容
        self.finalize_content(pending_content, current_hierarchy)
        return root

    def finalize_content(self, pending: list, hierarchy: dict):
        """合并多行内容并写入节点"""
        if not pending:
            return
        # 合并内容
        merged_content = '\n'.join(pending)
        current_node = hierarchy.get(max(hierarchy.keys(), default=0))
        if current_node:
            if current_node["content"]:
                current_node["content"] += '\n' + merged_content
            else:
                current_node["content"] = merged_content
        # 清空待处理内容
        pending.clear()

    def pdf_to_json(self, pdf_path: str):
        """
        PDF解析器,提取每一页pdf的内容和表格内容提取出来，
        """
        result = {
            "pages": [],
            "tables": []
        }
        try:
            # 每一页内容提取
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    # 优先提取表格并记录表格区域
                    tables = page.find_tables()
                    table_areas = []
                    extracted_tables = []
                    if tables:
                        for table_num, table in enumerate(tables, 1):
                            # 获取表格边界框
                            bbox = table.bbox
                            table_areas.append(bbox)
                            # 提取表格数据
                            extracted = page.crop(bbox).extract_table()
                            extracted_tables.append({
                                "page": page_num,
                                "table_number": table_num,
                                "data": extracted
                            })

                    # 创建排除表格区域的页面副本
                    clean_page = page
                    for area in table_areas:
                        clean_page = clean_page.outside_bbox(area)

                    # 优化文本提取参数
                    text = clean_page.extract_text()
                    result["pages"].append({
                        "page_number": page_num,
                        "text": text,
                    })
                    result["tables"].extend(extracted_tables)
            return result
        except Exception as e:
            print(f"解析错误: {str(e)}")
            return {}