import re

section_keywords = [
    "abstract", "summary",
    "introduction", "background", "task formulation",
    "method", "related work",
    "experiments", "results", "discussion", "conclusion",
    "references", "appendix"

    "摘要", "引言", "背景", "相关工作",
    "方法", "实验", "结果", "讨论", "结论", "参考文献", "附录"
]

reference_keywords = [
    # 中文表述
    "参考文献", "参考书目", "引用文献", "引用来源",
    "参考资料", "参考材料", "引用资料", "文献目录",
    "引用书目", "参考著作", "引用著作", "文献列表",
    "参考引文", "引用引文", "参引文献", "参阅文献",
    "文献引用", "书目参考", "资料来源", "文献来源",
    "引用出处", "参考出处", "文献索引", "参考索引",

    # 英文表述
    "references", "bibliography", "citations",
    "cited works", "works cited", "sources",
    "literature cited", "reference list",
    "reference materials", "further reading",
    "additional resources", "acknowledgments",

    # 学术特殊格式
    "参考文献表", "参考文献列表", "引用文献表",
    "参考文献目录", "参考文献索引",

    # 其他相关表述
    "引文", "注释", "脚注", "尾注",
    "文后参考文献", "文中参考文献",
    "引用标注", "参考引证"
]


header_number_pattern = re.compile(r"^\d+\.\d+(\.\d+)*\.?|\d\.?")
# number_pattern = re.compile(r'^\d+')


def is_section_title(s: str):
    s = s.lower()
    for word in section_keywords:
        if s.find(word, 0, len(word) + 10) < 5:
            return True

    return False


def is_num(s: str):
    return bool(header_number_pattern.match(s))


def sentence_not_end(s: str):
    if s.isalpha() and len(s) < 6:
        return True
    return s.endswith(',') or s.endswith('-') or s.endswith(';')


def split(s: str):
    return s.split('\n')


def join(lines: list, sep='\n'):
    return sep.join(lines)


def p1(lines: list):
    result = []
    buffer = []
    for line in lines:
        if not line:
            if buffer:
                result.append(''.join(buffer))
                buffer.clear()
        else:
            buffer.append(line)
    return result


def p2(lines: list):
    result = [lines[0]]
    for line in lines[1:]:
        if is_num(result[-1]) or (not is_section_title(line) and sentence_not_end(result[-1])):
            result[-1] = result[-1] + line
        else:
            result.append(line)
    return result


def organize(lines: list):
    paper = {
        "title": lines[0],
        "authors": lines[1],
        "sections": []
    }

    i = 2
    title = None
    buffer = []

    def append_section():
        if not title:
            return
        if 'abstract' in title.lower():
            paper['abstract'] = '\n'.join(buffer)
            return
        if 'appendix' in title.lower():
            paper['appendix'] = list(buffer)
            return
        paper["sections"].append({
            "title": title,
            "content": list(buffer)
        })

    while i < len(lines):
        line = lines[i]
        if is_section_title(line):
            append_section()
            buffer.clear()
            title = line
        else:
            buffer.append(line)
        i += 1
    append_section()

    return paper


if __name__ == '__main__':
    print(is_num('1'))
    print(is_num('1.'))
    print(is_num('1.2'))
    print(is_num('1.2.'))
