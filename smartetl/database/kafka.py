"""
基于confluent_kafka的Kafka数据库组件
"""
import json

try:
    from confluent_kafka import Consumer, KafkaException, KafkaError
    from confluent_kafka import Producer
except:
    raise ImportError("kafka not installed!")

from .base import Database


def delivery_report(err, msg):
    """回调函数，用于确认消息是否成功发送"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


class Kafka(Database):
    def __init__(self, host: str,
                 topic: str,
                 group_id: str = 'smartetl-consumer',
                 max_bytes: int = 10485760,
                 **kwargs):
        """
        Kafka数据库工具
        :param host brokerIP地址
        :param topic 写入或读取的消息队列名称
        :param group_id 消费者组
        :param max_bytes 定义写入消息的最大字节数 默认10M
        """
        self.host = host
        self.topic = topic
        self.group_id = group_id
        self.max_bytes = max_bytes

    def scroll(self, **kwargs):
        conf = {
            'bootstrap.servers': self.host,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',  # 从最早的消息开始读取
            'enable.auto.commit': False,  # 手动提交偏移量
            # 增加最大轮询间隔
            'max.poll.interval.ms': 600000,  # 10分钟
        }
        # 创建消费者实例
        consumer = Consumer(conf)
        # 订阅主题
        consumer.subscribe([self.topic])
        print(f"Starting Kafka consumer, listening to topic {self.topic}...")
        while True:
            # 轮询消息，超时时间为1秒
            msg = consumer.poll(1.0)
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
                consumer.commit(msg)
                yield value

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue

    def upsert(self, items: dict or list, **kwargs):
        if isinstance(items, dict):
            items = [items]
        conf = {
            'bootstrap.servers': self.host,
            'client.id': 'python-producer',
            'security.protocol': 'plaintext',  # 明确指定明文协议
            'api.version.request': 'true',
            'message.max.bytes': self.max_bytes
        }
        producer = Producer(conf)
        try:
            for row in items:
                # 确保所有 bytes 数据被转换为字符串
                processed_row = {}
                for key, value in row.items():
                    if isinstance(value, bytes):
                        processed_row[key] = value.decode('utf-8')
                    else:
                        processed_row[key] = value
                id = row['id']
                producer.produce(
                    self.topic,
                    key=str(id),
                    value=json.dumps(processed_row),
                    callback=delivery_report
                )

            # 触发任何待处理的交付报告回调
            producer.poll(0)
            # 等待所有消息被发送
            producer.flush()
        except KeyboardInterrupt:
            print("\nProducer interrupted")
        finally:
            print("Producer closed")
        return True
