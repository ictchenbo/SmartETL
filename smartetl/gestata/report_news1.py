"""
针对一种特定格式的word文件 解析其中的新闻标题与正文
格式参考：test_data/news/每日开源20241109第222期总第454期.docx'
"""
import re
from typing import Iterable, Any
from datetime import datetime

from docx import Document
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT


def is_empty_paragraph(paragraph):
    """检查段落是否为空行。"""
    return not paragraph.text.strip()


def is_toc_or_image(paragraph):
    """检查段落是否为目录或图片（简单过滤，可以根据实际情况增强）。"""
    if paragraph.style.name == 'TOC Heading' or 'Figure' in paragraph.style.name:
        return True
    return any(run.element.xpath('.//w:drawing') for run in paragraph.runs)


def is_centered_heading(paragraph):
    """检查段落是否为居中的大标题，并打印调试信息。"""
    alignment = paragraph.alignment
    print(f"Checking paragraph: '{paragraph.text[:30]}...' with alignment: {alignment}")

    # 检查段落是否居中对齐，并确保其不是空的段落
    return alignment == WD_PARAGRAPH_ALIGNMENT.CENTER and not is_empty_paragraph(paragraph)


def parse_created_time(file_name):
    """解析文件名中的时间作为created_time。"""
    date_match = re.search(r'(\d{8})', file_name)
    if date_match:
        date_str = date_match.group(1)
        return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    return None


def chunk_paragraphs(paragraphs):
    """根据空行对段落进行分块，保留段落对象以便进一步检查属性。"""
    chunks = []
    current_chunk = []

    for para in paragraphs:
        if is_empty_paragraph(para):
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
        elif not is_toc_or_image(para):
            current_chunk.append(para)

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def group_by_headings(chunks):
    """根据大标题分组，识别大标题和小标题，按要求进行分组。"""
    grouped_data = []
    current_section = None

    for chunk in chunks:
        if chunk:
            # 打印调试信息，显示当前块的内容
            # print(f"Processing chunk: {[para.text for para in chunk]}")
            inChunk_list = [para.text for para in chunk]

            if len(inChunk_list) == 3 and inChunk_list[2] != "内部资料 仅供参考":  # 大标题

                if current_section:
                    grouped_data.append(current_section)
                current_section = {"title": chunk[0].text, "nodes": []}  # 保存大标题的文本
                # print(f"Detected big title: {chunk[0].text}")
                title = chunk[1].text
                content = "\n".join([para.text for para in chunk[2:]])
                current_section["nodes"].append({
                    "title": title,
                    "content": content
                })
            else:  # 小标题和内容
                if current_section and len(chunk) > 1:

                    title = chunk[0].text  # 小标题
                    content = "\n".join([para.text for para in chunk[1:]])  # 小标题下的内容
                    if title != "检测微弱信号" and content != "为未来挑战做好准备":
                        current_section["nodes"].append({
                            "title": title,
                            "content": content
                        })

    if current_section:
        grouped_data.append(current_section)

    return grouped_data


def E(input_file: str) -> Iterable[Any]:
    """按照一种特定格式提取word文件中的新闻内容"""
    doc = Document(input_file)
    paragraphs = doc.paragraphs
    chunks = chunk_paragraphs(paragraphs)
    grouped_data = group_by_headings(chunks)

    # 获取文件名
    file_name = input_file.split('/')[-1]
    # 获取文章的发表时间
    created_time = parse_created_time(file_name)

    for group in grouped_data:
        topic = group['title']
        for item in group.get('nodes'):
            yield {
                "filename": file_name,
                "date": created_time,
                "topic": topic,
                "title": item["title"],
                "content": item["content"]
            }
