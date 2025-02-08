import io
import os
from zipfile import ZipFile

from wikidata_filter.base import relative_path
from wikidata_filter.iterator.base import JsonIterator
from wikidata_filter.util.http import content as get_content
from wikidata_filter.util.files import get_lines

config_base = "config/gdelt"


def write_file(url: str, content, save_path: str):
    file_name = url.split('/')[-1]
    file_name = os.path.join(save_path, file_name)
    with open(file_name, 'wb') as fout:
        fout.write(content)
    print("Gdelt zip saved to", file_name)


def parse_csv(url: str, save_path: str = None):
    """解析GDELT CSV文件内容"""
    # 处理URL地址
    if url.startswith("http://") or url.startswith("https://"):
        content = get_content(url, most_times=30, ignore_error=True)
        if len(content) < 100:
            return
        if save_path:
            write_file(url, content, save_path)
        bytes_io = io.BytesIO(content)
        with ZipFile(bytes_io) as zipObj:
            filename = zipObj.filelist[0].filename
            with zipObj.open(filename) as f:
                for line in f:
                    line_s = line.decode("utf8").strip()
                    if line_s:
                        yield line_s.split('\t')
    else:
        # 作为本地文件处理
        with open(url, encoding="utf8") as fin:
            for line in fin:
                yield line.split('\t')


class SchemaBuilder:
    """GDELT数据结构定义 根据配置文件"""
    def __init__(self, filename: str):
        self.constructors = {}
        self.fields = []
        for line in get_lines(filename):
            if not line:
                continue
            field = line
            builder = str
            if ':' in line:
                pos = line.find(':')
                field = line[:pos]
                f_type = line[pos + 1:]
                if f_type.startswith('int'):
                    builder = int
                elif f_type.startswith('float'):
                    builder = float
            self.fields.append(field)
            self.constructors[field] = builder

    def init_value(self, field, val):
        if field not in self.constructors:
            return val
        builder = self.constructors[field]
        if val == '':
            if builder == int or builder == float:
                return None
        return builder(val)

    def as_dict(self, values: list) -> dict:
        return {f: self.init_value(f, val) for f, val in zip(self.fields, values)}


class Export(JsonIterator):
    """根据GDELT的Export和Mention结构定义 根据输入的URL或文件地址 自动下载（URL）或读取（本地文件）CSV.zip文件，处理成JSON格式"""
    event_builder = SchemaBuilder(relative_path(f'{config_base}/export.schema'))
    mention_builder = SchemaBuilder(relative_path(f'{config_base}/mention.schema'))

    def __init__(self, save_path: str = None):
        super().__init__()
        self.save_path = save_path
        self.return_multiple = True

    def join_schema(self, url: str, builder: SchemaBuilder) -> dict:
        for row in parse_csv(url, save_path=self.save_path):
            try:
                yield builder.as_dict(row)
            except:
                print('Error', row)
                pass

    def on_data(self, row: dict or None, *args):
        url = row
        if isinstance(row, list):
            url = url[0]
        if isinstance(url, dict):
            url = row.get("url")
        if 'export.CSV' in url:
            for row in self.join_schema(url, self.event_builder):
                yield row
        elif 'mentions.CSV' in url:
            for row in self.join_schema(url, self.mention_builder):
                yield row
