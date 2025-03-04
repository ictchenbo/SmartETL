"""
针对Common Crawl格式（WARC/WAT/WET）提供解析

输入：WARC/WAT/WET gz文件
输出：每个网页一条记录
"""
from typing import Iterable, Any
import json
import chardet

from wikidata_filter.util.files import open_file
from wikidata_filter.loader.file import File


class CCBase(File):
    def __init__(self, input_file: str, **kwargs):
        self.input_file = input_file
        self.instream = open_file(input_file, **kwargs)

    def iter(self) -> Iterable[Any]:
        content = []
        for line in self.instream:
            if line == b'WARC/1.0\r\n':
                if content:
                    yield content
                    content.clear()
            else:
                content.append(line)
        if content:
            yield content


def parse_header_line(header: bytes):
    header = header.decode('ascii', errors='ignore').strip()
    pos = header.find(':')
    if pos > 0:
        return header[:pos], header[pos + 1:].strip()
    else:
        return None, header


def parse_header(headers: list):
    ret = {}
    for header in headers:
        k, v = parse_header_line(header)
        ret[k or '__'] = v
    return ret


charsets = ['utf-8', 'ascii', 'big5', 'iso-8859-1', 'gb2312', 'windows-1251']


def guess_charset(line: str):
    for c in charsets:
        if c in line:
            return c
    return None


def get_charset(content_type: str):
    if content_type is None:
        return None
    parts = content_type.split(';')
    for part in parts:
        part = part.strip()
        if part.startswith('charset='):
            return part.replace('charset=', '')

    return guess_charset(content_type.lower())


def parse_body(header: dict, body: list, default_charset='utf8'):
    if len(body) < 10:
        return ''
    content_type = header.get('Content-Type') or header.get('content-type') or header.get('Content-type')
    charset = get_charset(content_type)
    if charset is None:
        content = b''.join(body[:50])
        detect_res = chardet.detect(content)
        if detect_res.get("confidence", 0.1) >= 0.7:
            charset = detect_res.get('encoding')
    if charset and charset != "none":
        try:
            return ''.join([line.decode(charset, errors='ignore') for line in body])
        except Exception as e:
            print(e)
            charset = guess_charset(charset)
            if charset:
                return ''.join([line.decode(charset, errors='ignore') for line in body])
            return ''
    else:
        try:
            return ''.join([line.decode(default_charset, errors='ignore') for line in body])
        except Exception as e:
            print(e)
            return ''


def parse_id(v: str):
    if v:
        return v.replace('<urn:uuid:', '').replace('>', '')
    return v


def get_basic(warc_header: dict):
    return {
        '_id': parse_id(warc_header.get('WARC-Record-ID')),
        'date': warc_header.get('WARC-Date'),
        'url': warc_header.get('WARC-Target-URI'),
    }


class CCWARC(CCBase):
    """解析CC的WARC文件格式"""
    def __init__(self, input_file: str, **kwargs):
        super().__init__(input_file, **kwargs)

    def iter(self) -> Iterable[Any]:
        for content in super().iter():
            warc = []
            http_header = []
            http_body = []
            empty_line = 0
            for line in content:
                if empty_line >= 2:
                    http_body.append(line)
                elif line == b'\r\n':
                    empty_line += 1
                    continue
                else:
                    if empty_line == 0:
                        warc.append(line)
                    else:
                        http_header.append(line)
            header = parse_header(http_header)
            body = parse_body(header, http_body)
            if not body:
                continue
            warc_header = parse_header(warc)
            row = get_basic(warc_header)
            row['header'] = header
            row['body'] = body.strip()
            yield row


CC_HEADERS = {
    'WARC-Record-ID': '_id',
    'WARC-Date': 'date',
    'WARC-Target-URI': 'url'
}


class CCWET(CCBase):
    """解析CC的WET文件格式"""
    def __init__(self, input_file: str, **kwargs):
        super().__init__(input_file, **kwargs)

    def iter(self) -> Iterable[Any]:
        for content in super().iter():
            row = {}
            http_body = []
            empty_line = False
            for line in content:
                if empty_line:
                    http_body.append(line.decode('utf8', errors='ignore'))
                elif line == b'\r\n':
                    empty_line = True
                else:
                    k, v = parse_header_line(line)
                    if k and k in CC_HEADERS:
                        row[CC_HEADERS[k]] = v
            if '_id' in row:
                row['_id'] = parse_id(row['_id'])
            if http_body:
                row['title'] = http_body[0].strip()
            row['text'] = ''.join(http_body[1:]).strip()
            yield row


class CCWAT(CCBase):
    """解析CC的WAT文件格式"""
    def __init__(self, input_file: str, **kwargs):
        super().__init__(input_file, **kwargs)

    def iter(self) -> Iterable[Any]:
        for content in super().iter():
            for line in content:
                if line.startswith(b'{') and line.endswith(b'}\r\n'):
                    yield json.loads(line)
