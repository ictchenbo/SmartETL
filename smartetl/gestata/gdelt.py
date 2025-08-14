"""
GDELT数据下载相关算子
"""
import io
import os
import datetime
import time
import json
from zipfile import ZipFile
from urllib.parse import urljoin

from smartetl.base import relative_path
from smartetl.util.http import content as get_content, text
from smartetl.util.files import get_lines

config_base = "config/gdelt"
base_url = 'http://data.gdeltproject.org/gdeltv2'
url_all_file = f"{base_url}/masterfilelist.txt"
url_latest_file = f"{base_url}/lastupdate.txt"
ZONE_DIFF = 8
DIFF_MINUTES = 15
DIFF_SECONDS = 60 * DIFF_MINUTES


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
        content = get_content(url, most_times=1, ignore_error=True)
        if len(content) < 100:
            return
        if save_path:
            write_file(url, content, save_path)
        if url.endswith('.zip'):
            with ZipFile(io.BytesIO(content)) as zipObj:
                filename = zipObj.filelist[0].filename
                with zipObj.open(filename) as f:
                    for line in f:
                        line_s = line.decode("utf8").strip()
                        if line_s:
                            yield line_s.split('\t')
        else:
            with open(content, encoding="utf8") as f:
                for line in f:
                    if line:
                        yield line.split('\t')
    else:
        # 作为本地文件处理
        if url.endswith('.zip'):
            with ZipFile(url) as zipObj:
                filename = zipObj.filelist[0].filename
                with zipObj.open(filename) as f:
                    for line in f:
                        line_s = line.decode("utf8").strip()
                        if line_s:
                            yield line_s.split('\t')
        else:
            with open(url, encoding="utf8") as f:
                for line in f:
                    if line:
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


event_builder = SchemaBuilder(relative_path(f'{config_base}/export.schema'))
mention_builder = SchemaBuilder(relative_path(f'{config_base}/mention.schema'))


def join_schema(url: str, builder: SchemaBuilder, save_path: str = None) -> dict:
    for row in parse_csv(url, save_path=save_path):
        try:
            yield builder.as_dict(row)
        except Exception as e:
            print('Error', row)
            raise e


def process_task(row: dict or str, url_key: str = 'url', save_path: str = None, **kwargs):
    """根据GDELT的Export和Mention结构定义 根据输入的URL或文件地址 自动下载（URL）或读取（本地文件）CSV.zip文件，处理成JSON格式"""
    url = row
    if isinstance(url, dict):
        url = row.get(url_key)
    if 'mentions.CSV' in url:
        for row in join_schema(url, mention_builder, save_path=save_path):
            yield row
    else:
        for row in join_schema(url, event_builder, save_path=save_path):
            yield row


class Task:
    """函数式类，每隔15分钟左右获取最新的GDELT事件列表，输出每一个下载处理任务"""
    ts_file = ".gdelt.ts"

    def __init__(self, *dates):
        use_dates = dates
        if os.path.exists(self.ts_file):
            with open(self.ts_file, encoding="utf8") as fin:
                use_dates = json.load(fin)
            print("timestamp file exists, using:", use_dates)
        else:
            print("starting from ", use_dates)
        self.ts = datetime.datetime(*use_dates)

    def write_ts(self):
        t = self.ts
        row = [t.year, t.month, t.day, t.hour, t.minute, t.second]
        with open(self.ts_file, "w", encoding="utf8") as out:
            json.dump(row, out)

    def update_ts(self):
        self.ts += datetime.timedelta(seconds=DIFF_SECONDS)

    def wait_or_continue(self):
        now = datetime.datetime.now() - datetime.timedelta(hours=ZONE_DIFF)
        if now > self.ts:
            diff = now - self.ts
            total_seconds = diff.days * 86400 + diff.seconds
            if total_seconds > DIFF_SECONDS:
                return True
        else:
            diff = self.ts - now
            total_seconds = diff.days * 86400 + diff.seconds
        seconds = max(total_seconds, DIFF_SECONDS)
        print(f"waiting for {seconds} seconds")
        time.sleep(seconds)
        return False

    def __call__(self, *args, **kwargs):
        while True:
            self.wait_or_continue()
            print("Processing:", self.ts.strftime('%m-%d %H:%M'))
            timestamp_str = self.ts.strftime('%Y%m%d%H%M%S')
            csv_zip_url = f'{base_url}/{timestamp_str}.export.CSV.zip'
            yield {
                "_id": timestamp_str,
                "url": csv_zip_url
            }
            self.update_ts()
            # 记录更新后的TS（即下次任务TS），如果被kill，下次启动不会重复处理
            self.write_ts()


def get_page_parse(url):
    content = text(url)
    if content:
        for row in content.split('\n'):
            parts = row.split()
            if len(parts) < 3:
                continue
            yield {
                "url": parts[2],
                "file_size": int(parts[0])
            }


def all_task():
    """
    获取GDELT全部更新列表 解析相关文件URL
    """
    return get_page_parse(url_all_file)


def latest_task(times_of_requests: int = 1):
    """
    获取GDELT最近更新文件列表 解析相关文件URL
    """
    forever = times_of_requests == 0
    run_times = 0
    while forever or run_times < times_of_requests:
        for row in get_page_parse(url_latest_file):
            yield row
        run_times += 1
        print(f"request for {run_times} times")


def all_zip(url: str = 'http://data.gdeltproject.org/events/index.html', is_url: bool = True):
    from bs4 import BeautifulSoup
    if is_url:
        html = text(url)
    else:
        html = url
    soup = BeautifulSoup(html, 'html.parser')
    # 查找所有 ul 下的 li 中的 a 标签
    links = soup.select('ul li a')

    # 打印所有 a 标签的 href 属性
    for link in links:
        href = link.get('href')
        if '.zip' in href:
            yield urljoin(url, href)


if __name__ == '__main__':
    for link in all_zip():
        print(link)
