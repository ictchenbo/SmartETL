import json
from typing import Iterable, Any
from confluent_kafka import Consumer, KafkaException, KafkaError
from wikidata_filter.loader.base import DataProvider


class KafkaConsumer(DataProvider):
    """
    从kafka指定主题中消费数据
    """

    def __init__(self,
                 kafka_ip: str = None,
                 kafka_topic: str = None,
                 group_id: str = None,
                 **kwargs
                 ):

        self.topic = kafka_topic
        self.conf = {
            'bootstrap.servers':  kafka_ip,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # 从最早的消息开始读取
            'enable.auto.commit': False,  # 手动提交偏移量
            # 增加最大轮询间隔
            'max.poll.interval.ms': 600000,  # 10分钟
        }
        # 创建消费者实例
        self.consumer = Consumer(self.conf)
    def iter(self) -> Iterable[Any]:
        # 订阅主题
        self.consumer.subscribe([self.topic])
        print(f"Starting Kafka consumer, listening to topic {self.topic}...")
        while True:
            # 轮询消息，超时时间为1秒
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # 到达分区末尾
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            # 成功接收到消息
            try:
                value = json.loads(msg.value().decode('utf-8'))
                print(f"Received message: key={msg.key()}")
                # 处理消息后手动提交偏移量
                self.consumer.commit(msg)
                yield value

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue

