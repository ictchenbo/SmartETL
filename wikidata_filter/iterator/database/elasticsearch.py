import json
from typing import Any

import requests
from wikidata_filter.iterator.base import DictProcessorBase
from wikidata_filter.iterator.aggregation import BufferedWriter

id_keys = ["_id", "id", "mongo_id"]


class ESWriter(BufferedWriter):
    """
    数据写入ES索引中
    """
    def __init__(self, host="localhost",
                 port=9200,
                 username=None,
                 password=None,
                 index=None,
                 buffer_size=1000, **kwargs):
        super().__init__(buffer_size=buffer_size)
        self.url = f"http://{host}:{port}"
        if password:
            self.auth = (username, password)
        else:
            self.auth = None
        self.index_name = index

    def write_batch(self, rows: list):
        header = {
            "Content-Type":  "application/json"
        }
        lines = []
        for row in rows:
            action_row = {}
            for key in id_keys:
                if key in row:
                    action_row["_id"] = row.pop(key)
                    break
            # row_meta = json.dumps({"index": action_row})
            row_meta = json.dumps({"index": action_row})
            row_data = json.dumps(row)
            lines.append(row_meta)
            lines.append(row_data)
        body = '\n'.join(lines)
        body += '\n'
        print(f"{self.url}/{self.index_name} bulk")
        res = requests.post(f'{self.url}/{self.index_name}/_bulk', data=body, headers=header, auth=self.auth)
        if res.status_code != 200:
            print("Warning, ES bulk load failed:", res.text)
            return False
        return True


class Delete(DictProcessorBase):
    """
    删除ES数据
    """
    def __init__(self, host="localhost",
                 port=9200,
                 username=None,
                 password=None,
                 index=None,
                 id_key: str = "_id", **kwargs):
        self.url = f"http://{host}:{port}"
        if password:
            self.auth = (username, password)
        else:
            self.auth = None
        self.index = index
        self.id_key = id_key

    def on_data(self, data: dict, *args):
        row_id = data.get(self.id_key)
        if row_id:
            res = requests.delete(f'{self.url}/{self.index}/_doc/{row_id}', auth=self.auth)
            if res.status_code == 200:
                print("Deleted:", row_id)
            else:
                print("Error:", res.json())
        return data
