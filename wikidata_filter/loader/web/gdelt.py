import datetime
import time
import os
import json

from wikidata_filter.loader.base import DataProvider
from wikidata_filter.util.http import text


base_url = 'http://data.gdeltproject.org/gdeltv2'
url_all_file = f"{base_url}/masterfilelist.txt"
url_latest_file = f"{base_url}/lastupdate.txt"
ZONE_DIFF = 8


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


class GdeltAll(DataProvider):
    """
    获取GDELT全部更新列表 解析相关文件URL
    """
    def iter(self):
        return get_page_parse(url_all_file)


class GdeltLatest(DataProvider):
    """
    获取GDELT最近更新文件列表 解析相关文件URL
    """
    def __init__(self, times_of_requests: int = 1):
        self.times_of_requests = times_of_requests

    def iter(self):
        forever = self.times_of_requests == 0
        run_times = 0
        while forever or run_times < self.times_of_requests:
            for row in get_page_parse(url_latest_file):
                yield row
            run_times += 1
            print(f"request for {run_times} times")


class GdeltTaskEmit(DataProvider):
    """每隔15分钟左右获取最新的GDELT事件列表，输出每一个事件"""
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
        self.ts += datetime.timedelta(minutes=15)

    def wait_or_continue(self):
        now = datetime.datetime.now() - datetime.timedelta(hours=ZONE_DIFF)
        if now > self.ts:
            diff = now - self.ts
            total_seconds = diff.days * 86400 + diff.seconds
            if total_seconds > 900:
                return True
        else:
            diff = self.ts - now
            total_seconds = diff.days * 86400 + diff.seconds
        seconds = max(total_seconds, 900)
        print(f"waiting for {seconds} seconds")
        time.sleep(seconds)
        return False

    def iter(self):
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


class GdeltLocal(DataProvider):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def iter(self):
        yield {
            "url": self.file_path,
            "file_size": -1
        }
