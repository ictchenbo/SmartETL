"""
针对经济学人民调数据转换为GoIN的调查数据结构
数据来源：https://cdn.economistdatateam.com/2024-us-tracker/harris/data/polls/polltracker-polls.csv
"""
import re
from wikidata_filter.iterator.base import JsonIterator


def parse_int(v: str, default):
    v = v.replace(',', '')
    if re.match("\\d+", v):
        return int(v)
    return default


def parse_float(v: str, default):
    if re.match(r"[\d\\.]+", v):
        return float(v)
    return default


class PollData(JsonIterator):
    def __init__(self, label: str, poll_key: str = "national"):
        self.label = label
        self.poll_key = poll_key

    def on_data(self, row, *args):
        if row is None:
            return None
        key, values = row["key"], row["values"]
        if not values:
            return None
        info = values[0]
        results = {}
        for v in values:
            name = v['candidate_name'].split()[-1].strip()
            results[name] = parse_float(v['pct'], 0)

        return {
            "_id": f"{self.label}_{key}",
            "event_id": "us_president_2024",
            "name": "",
            "poll_time_start": info["start_date"],
            "poll_time_end": info["end_date"],
            "source": {
                "type": "机构",
                "name": info["pollster"],
                "sponsors": info["sponsors"],
                "url": "https://cdn.economistdatateam.com/2024-us-tracker/harris/data/polls/polltracker-polls.csv",
                "stat_info": {
                    "sample": parse_int(info["sample_size"], None),
                    "type": info["population"]
                }
            },
            "publish_time": info["end_date"],
            "results": {
                self.poll_key: results
            }
        }
