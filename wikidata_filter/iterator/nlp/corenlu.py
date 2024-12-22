# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

import requests
import random
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait

from wikidata_filter.util.dicts import reverse_dict


def map_self(v):
    return v


def map_dict(source: dict or list, mapping: dict):
    if isinstance(source, dict):
        return {mapping[pk]: pv for pk, pv in source.items() if pk in mapping}
    else:
        return [map_dict(item, mapping) for item in source]


corenlu_task_map = {
    "token": "_nlu_token",
    "sentences": "_nlu_sentences",
    "pos": "_nlu_pos",
    "ner": "_nlu_ner",
    "nel": "_nlu_nel",
    "relation": "_nlu_relation",
    "event": "_nlu_event",
    "keywords": "_nlu_keywords",
    "topic": "_nlu_topic",
    "sentiment": "_nlu_sentiment",
    "translate": "_nlu_translate",
    "opinion": "_nlu_opinion"
}

corenlu_task_map_reverse = reverse_dict(corenlu_task_map)

ner_type_mapping = {
    "per": "human",
    "loc": "geographic_entity",
    "fac": "geographic_entity",
    "gpe": "geographic_entity",
    "org": "organization",
    "norp": "organization"
}


def normal_type(t: str):
    t = t.lower()
    return ner_type_mapping.get(t) or t


def get_ner(res: dict):
    items = []
    for _, v in res.items():
        for vi in v:
            if vi[1] != 'O' and len(vi[0]) > 1:
                entity_type = normal_type(vi[1])
                items.append({
                    "name": vi[0],
                    "type": entity_type
                })
    return items


nel_mapping = {
    "content": "name",
    "entity_id": "id",
    "location": "location"
}


def get_nel(res: dict):
    items = []
    for _, v in res.items():
        items.extend(map_dict(v, nel_mapping))
    return items


event_mapping = {
    "event_type": "type",
    "event_subtype": "subtype",
    "event_trigger": "trigger",
    "argument_list": "argument",
    "entity_list": "entity",
    "time_list": "time",
    "location_list": "location"
}
event_args = ["argument", "entity", "time", "location"]
argument_mapping = {
    "argument_content": "name",
    "entity_type": "type",
    "argument_role": "role"
}


def get_event(res):
    items = []
    for v in res:
        v2 = map_dict(v, event_mapping)
        for vi in v2:
            for arg in event_args:
                if arg in vi:
                    vi[arg] = map_dict(vi[arg], argument_mapping)
                    for arg_i in vi[arg]:
                        arg_i["type"] = normal_type(arg_i["type"])
        items.extend(v2)
    return items


def sent2text_sentiment(sentiment, threshold1=0.6, threshold2=0.7):
    if type(sentiment) == str:
        return sentiment
    elif type(sentiment) != dict:
        raise TypeError('sentiment type must in [str, dict].')
    sentiment_list = [i[1] for i in sentiment.values()]
    total = len(sentiment_list)
    if total == 0:
        return ''
    sentiment_count = Counter(sentiment_list)
    if len(sentiment_count) == 1:
        return list(sentiment_count.keys())[0]
    for i in sentiment_count.keys():
        if ' ' in i:
            for j in ['Weakly Positive', 'Strongly Positive', 'Neutral',
                      'Weakly Negative', 'Strongly Negative']:
                if j not in sentiment_count:
                    sentiment_count[j] = 0
            break
    else:
        for j in ['Positive', 'Neutral', 'Negative']:
            if j not in sentiment_count:
                sentiment_count[j] = 0
    if len(sentiment_count) == 5:
        if total - sentiment_count['Neutral'] == 0:
            return 'Neutral'
        elif (sentiment_count['Weakly Positive'] + sentiment_count[
            'Strongly Positive']) / (
                total - sentiment_count['Neutral']) > threshold1:
            if sentiment_count['Strongly Positive'] / (
                    sentiment_count['Strongly Positive']
                    + sentiment_count['Weakly Positive']) > threshold2:
                return 'Strongly Positive'
            else:
                return 'Weakly Positive'
        elif (sentiment_count['Weakly Negative'] + sentiment_count[
            'Strongly Negative']) / (
                total - sentiment_count['Neutral']) > threshold1:
            if sentiment_count['Strongly Negative'] / (
                    sentiment_count['Strongly Negative']
                    + sentiment_count['Weakly Negative']) > threshold2:
                return 'Strongly Negative'
            else:
                return 'Weakly Negative'
        else:
            return 'Neutral'
    elif len(sentiment_count) == 3:
        if total - sentiment_count['Neutral'] == 0:
            return 'Neutral'
        elif sentiment_count['Positive'] / (
                total - sentiment_count['Neutral']) > threshold1:
            return 'Positive'
        elif sentiment_count['Negative'] / (
                total - sentiment_count['Neutral']) > threshold1:
            return 'Negative'
        else:
            return "Neutral"
    else:
        raise ValueError('sentimet labels num must in [3, 5].')


result_extractors = {
    "_nlu_ner": get_ner,
    "_nlu_nel": get_nel,
    "_nlu_event": get_event,
    "_nlu_sentiment": sent2text_sentiment
}


def load(all_docs: list, parse_config):
    """
    调用CoreNLU服务进行文档解析
    :param all_docs: 全部文档
    :param parse_config:
    :return:
    """
    tasks = parse_config.get("tasks")
    corenlu_tasks = [corenlu_task_map[task] for task in tasks if task in corenlu_task_map]
    nlu_service = parse_config.get("service")
    timeout = parse_config.get("timeout", 3 * 60)
    lang = parse_config.get("lang", "zh")

    max_length = parse_config.get("max_length", 0)  # 单篇文档最大长度（字符数） TODO 最大单词数？最大句子数？

    def choose_service():
        if isinstance(nlu_service, list):
            return random.choice(nlu_service)
        return nlu_service

    def parse_field(docs, field):
        docs = [doc for doc in docs if field in doc and doc[field]]
        if max_length > 0:
            parse_data = [doc[field][:max_length] for doc in docs if doc[field]]
        else:
            parse_data = [doc[field] for doc in docs if doc[field]]
        if not parse_data:
            raise Exception("content 为空")
        params = {
            "lang": lang,
            "tasks": corenlu_tasks,
            "data": parse_data
        }

        res = requests.post(url=choose_service(), json=params, timeout=timeout)
        if res.status_code == 200:
            result = res.json()['result']
            print(result)
            field_target = f"_corenlu_{field}"
            # 原始结果附加到 _corenlu_content 字段上
            for task, task_results in result.items():
                if task not in corenlu_task_map_reverse:
                    continue
                target = corenlu_task_map_reverse[task]
                for k, doc in enumerate(docs):
                    if field_target in doc:
                        doc[field_target][target] = task_results[k]
                    else:
                        doc[field_target] = {
                            target: task_results[k]
                        }
            # 抽取结果到doc顶层
            for task in corenlu_tasks:
                if task not in result:
                    continue
                task_results = result[task]
                target = corenlu_task_map_reverse[task]
                for k, doc in enumerate(docs):
                    # print(task_results[k])
                    extractor = result_extractors.get(task) or map_self
                    doc[f"_{target}_"] = extractor(task_results[k])
        else:
            # log().info("corenlu parse error: " + res.text)
            # print("corenlu parse error: " + res.text)
            # repeat_error(docs, parse_config)
            pass

    max_docs = parse_config.get("max_docs", 20)  # 单次请求最多文章数量
    parse_fields = parse_config.get("parse_fields", ["content"])  # 解析的字段
    parallels = parse_config.get("parallels", 1)

    all_tasks = []
    pool = ThreadPoolExecutor(parallels)  # 线程池

    for field in parse_fields:
        if max_docs > 0:
            slices = [all_docs[start: start + max_docs] for start in range(0, len(all_docs), max_docs)]
            for slice in slices:
                all_tasks.append(pool.submit(parse_field, slice, field))
        else:
            all_tasks.append(pool.submit(parse_field, all_docs, field))

    wait(all_tasks)
