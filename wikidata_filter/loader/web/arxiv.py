"""产生arXiv下载链接"""
import time
import os
import json

from wikidata_filter.util.dates import current_date
from wikidata_filter.loader.base import DataProvider

base_url = "https://arxiv.org/pdf"


class ArXivTaskEmit(DataProvider):
    ts_file = ".arxiv.ts"

    def __init__(self, start_month: int = None, end_month: int = None):
        self.month = start_month or 2501
        self.seq = 1
        if os.path.exists(self.ts_file):
            with open(self.ts_file, encoding="utf8") as fin:
                nums = json.load(fin)
                self.month = nums[0]
                self.seq = nums[1]
        self.end_month = end_month or int(current_date('%y%m'))
        print(f"from {self.month}.{self.seq} to {self.end_month}")

    def write_ts(self):
        row = [self.month, self.seq]
        with open(self.ts_file, "w", encoding="utf8") as out:
            json.dump(row, out)

    def iter(self):
        while self.month <= self.end_month:
            while self.seq < 30000:
                url = f'{base_url}/{self.month}.{self.seq:05d}'
                print("processing:", url)
                yield url
                self.seq += 1
                # 记录更新后的TS（即下次任务TS），如果被kill，下次启动不会重复处理
                self.write_ts()
                time.sleep(1)
            self.month += 1
            if self.month % 100 > 12:
                self.month += int(self.month/100)*100 + 101
            self.seq = 1
            self.write_ts()
