import json
import logging
import time
from typing import Iterable, Any
import requests

from .base import Database


def get_logger(name: str):
    lgr = logging.getLogger(name)
    lgr.setLevel(logging.INFO)

    sh = logging.StreamHandler()
    fmt = logging.Formatter(fmt='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                            datefmt='%Y/%m/%d %H:%M:%S')
    sh.setFormatter(fmt)
    lgr.addHandler(sh)
    return lgr


class WebConsumer(Database):
    """
    基于HTTP接口的Kafka简单消费者，持续拉取指定话题数据
    """
    logger = get_logger("KafkaConsumer")

    def __init__(self,
                 api_base: str = 'localhost:62100',
                 group_id: str = None,
                 username: str = 'kafka',
                 user_auth: str = None,
                 topics: list = None,
                 auto_init: bool = False,
                 poll_timeout: int = 300,
                 poll_metabytes: int = 10,
                 poll_wait: int = 10,
                 connect_timeout: int = 30,
                 **kwargs
                 ):
        self.username = username
        self.base_uri = f'{api_base}/consumers/{username}'
        self.group_id = group_id
        self.topics = topics
        self.auto_init = auto_init
        self.headers1 = {
            'Content-Type': 'application/vnd.kafka.avro.v2+json',
            'Authorization': user_auth
        }
        self.headers2 = {
            'Accept': 'application/vnd.kafka.avro.v2+json',
            'Authorization': user_auth
        }
        self.poll_params = {
            'timeout':  poll_timeout*1000,
            'max_bytes': poll_metabytes*1024*1024
        }
        self.poll_wait = poll_wait
        self.poll_timeout = poll_timeout
        self.connect_timeout = connect_timeout

    def create(self):
        data = {
            'name': self.group_id,
            'format': 'avro',
            'auto.offset.reset': 'earliest',
            'auto.commit.enable': 'true'
        }
        try:
            r = requests.post(self.base_uri, data=json.dumps(data), headers=self.headers1, timeout=self.connect_timeout)
            if r.status_code == 200:
                self.logger.info("消费者创建成功")
                print(r.json())
                return True
            else:
                self.logger.error("Error to create consumer: " + r.text)
        except:
            self.logger.error("Kafka消费者创建成功发生错误，请检查")
        return False

    def subscribe(self, topics: list):
        data = {'topics': topics}
        try:
            r = requests.post(f'{self.base_uri}/instances/{self.group_id}/subscription',
                              data=json.dumps(data), headers=self.headers1, timeout=self.connect_timeout)
            if r.status_code == 200:
                self.logger.info(f'消息订阅成功：{topics}')
                print(r.json())
                return True
            else:
                self.logger.error("Error to subscribe: " + r.text)
        except:
            self.logger.error("Kafka消息订阅发生错误，请检查")
        return False

    def poll(self):
        try:
            r = requests.get(f'{self.base_uri}/instances/{self.group_id}/records',
                             params=self.poll_params, headers=self.headers2, timeout=self.poll_timeout)
            if r.status_code == 200:
                return r.json()
            else:
                # logger.error(r.text)
                return []
        except:
            self.logger.error("请求Kafka发生错误，请检查")
            return None

    def scroll(self) -> Iterable[Any]:
        if self.auto_init:
            assert len(self.topics) > 0, "topics should not be empty"
            self.create()
            self.subscribe(self.topics)
        while True:
            res = self.poll()
            if res is None:
                time.sleep(self.connect_timeout)
                continue

            if not res:
                self.logger.warning("no data, wait for 10 seconds")
                time.sleep(self.poll_wait)
                continue
            num = 0
            for item in res:
                if 'value' not in item:
                    continue
                v = item.pop('value')
                v['kafka-info'] = item
                yield v
                num += 1
            self.logger.info(f"Got {num} rows")


wait_time = [0, 5, 10, 15, 20, 30]


class TimedConsumer(Database):
    """自动重试的Kafka消费者，超时重新创建和订阅"""
    logger = get_logger("TimedConsumer")

    def __init__(self, api_base: str = 'localhost:62100',
                 group_id: str = None,
                 topics: list = ['news'],
                 username: str = 'kafka',
                 user_auth: str = None,
                 max_wait_times=14,
                 **kwargs):
        assert len(topics) > 0, "topics should not be empty"
        self.consumer = WebConsumer(api_base, group_id, username, user_auth, topics=topics, **kwargs)
        self.topics = topics
        self.max_wait_times = max_wait_times

    def task_init(self):
        self.consumer.create()
        self.consumer.subscribe(self.topics)

    def scroll(self) -> Iterable[Any]:
        no_data_count = 0
        do_init = True

        while True:
            if do_init:
                self.task_init()
                do_init = False
            res = self.consumer.poll()
            # 网络异常
            if res is None:
                time.sleep(60)
                do_init = True
                no_data_count = 0
                continue
            count = 0
            for item in res:
                if 'value' not in item:
                    continue
                v = item.pop('value')
                v['kafka-info'] = item
                count += 1
                yield v

            self.logger.info(f"Got {count} rows")

            sleep_time = wait_time[no_data_count % len(wait_time)]

            if count == 0:
                # 队列为空 则进行等待
                self.logger.info(f'no documents in this batch, retry in {sleep_time} seconds({no_data_count}).')
                no_data_count += 1

                if no_data_count >= self.max_wait_times:
                    # 等待超过一定次数 重新初始化连接（服务端似乎有BUG 有时候分明有数据也返回空 可能是恰好选择了没有数据的broker）
                    print("Timeout, redo task-init")
                    no_data_count = 0
                    do_init = True
            else:
                no_data_count = 0

            if sleep_time > 0:
                time.sleep(sleep_time)
