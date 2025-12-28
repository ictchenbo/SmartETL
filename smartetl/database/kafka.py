"""
基于confluent_kafka的Kafka数据库组件
"""
import json

try:
    from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
    from confluent_kafka.admin import AdminClient, NewTopic
except:
    raise ImportError("kafka not installed!")

from .base import Database


default_topic_config = {
    'retention.ms': '604800000',  # 7天
    'cleanup.policy': 'delete',
    'compression.type': 'producer'
}


def delivery_report(err, msg):
    """回调函数，用于确认消息是否成功发送"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


class Commit:
    def __init__(self, msg, consumer):
        self.msg = msg
        self.consumer = consumer

    def __call__(self, *args, **kwargs):
        self.consumer.commit(self.msg)


class Kafka(Database):
    def __init__(self, host: str,
                 topic: str = None,
                 group_id: str = 'smartetl-consumer',
                 max_bytes: int = 10485760,
                 auto_create: bool = False,
                 partitions: int = 3,
                 topic_config: dict = None,
                 **kwargs):
        """
        Kafka数据库工具
        :param host brokerIP地址
        :param topic 写入或读取的消息队列名称
        :param group_id 消费者组
        :param max_bytes 定义写入消息的最大字节数 默认10M
        :param auto_create 是否自动创建主题 （主要用于写数据）
        :param partitions 新建主题的分区数
        :param topic_config 新建主题的配置
        """
        self.host = host
        self.topic = topic
        self.group_id = group_id
        self.max_bytes = max_bytes
        self.producer = None
        if auto_create and topic is not None and topic not in self.list_topics():
            self.create_topic(topic, partitions=partitions, config=topic_config)

    def create_topic(self, name: str, partitions: int = 3, replication_factor: int = 1, config: dict = None):
        if not config:
            this_config = default_topic_config
        else:
            this_config = dict(**default_topic_config)
            this_config.update(config)

        new_topic = NewTopic(
            topic=name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=this_config
        )
        conf = {
            'bootstrap.servers': self.host
        }
        admin_client = AdminClient(conf)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()  # 触发异常（如果创建失败）
                print(f"✅ Topic '{topic}' 创建成功")
            except Exception as e:
                print(f"❌ 创建 Topic '{topic}' 失败: {e}")

    def list_topics(self):
        conf = {
            'bootstrap.servers': self.host,
            'request.timeout.ms': 10000
        }
        admin_client = AdminClient(conf)
        cluster_metadata = admin_client.list_topics(timeout=10)
        ret = []
        for topic_name in cluster_metadata.topics.keys():
            if not topic_name.startswith('__'):
                ret.append(topic_name)
        return ret
    
    def delete_topics(self, topics: list = None):
        conf = {
            'bootstrap.servers': self.host,
            'request.timeout.ms': 10000
        }
        admin_client = AdminClient(conf)
        admin_client.delete_topics(topics or [self.topic])
        return True

    def scroll(self, topic: str = None, auto_commit: bool = False, **kwargs):
        conf = {
            'bootstrap.servers': self.host,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',  # 从最早的消息开始读取
            'enable.auto.commit': auto_commit,  # 是否自动提交偏移量
            # 增加最大轮询间隔
            'max.poll.interval.ms': 600000,  # 10分钟
        }
        topic = topic or self.topic
        # 创建消费者实例
        consumer = Consumer(conf)
        # 订阅主题
        consumer.subscribe([topic])
        print(f"Starting Kafka consumer, listening to topic {topic}...")
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

            try:
                value = json.loads(msg.value().decode('utf-8'))
                if not auto_commit:
                    yield value, Commit(msg, consumer)
                else:
                    yield value

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue
    
    def create_producer(self):
        conf = {
            'bootstrap.servers': self.host,
            'client.id': 'python-producer',
            'security.protocol': 'plaintext',  # 明确指定明文协议
            'api.version.request': 'true',
            'message.max.bytes': self.max_bytes
        }
        self.producer = Producer(conf)

    def upsert(self, items: dict or list, topic: str = None, **kwargs):
        if isinstance(items, dict):
            items = [items]

        if not self.producer:
            self.create_producer()

        topic = topic or self.topic
        try:
            for row in items:
                # 确保所有 bytes 数据被转换为字符串
                processed_row = {}
                for key, value in row.items():
                    if isinstance(value, bytes):
                        processed_row[key] = value.decode('utf-8')
                    else:
                        processed_row[key] = value

                id = row.get('_id') or row.get('id')

                while True:
                    try:
                        self.producer.produce(
                            topic,
                            key=str(id),
                            value=json.dumps(processed_row),
                            callback=delivery_report
                        )
                        break
                    except BufferError:
                        self.producer.poll(0.1)

                # 触发任何待处理的交付报告回调
                self.producer.poll(0)
            # 等待所有消息被发送
            self.producer.flush(5)
            self.producer.poll(0)
        except KeyboardInterrupt:
            print("\nProducer interrupted")
        finally:
            print("Producer closed")
        return True
