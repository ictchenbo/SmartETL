import json
from confluent_kafka import Producer
from wikidata_filter.iterator.buffer import BufferedWriter


class KafkaWriter(BufferedWriter):
    """
    数据写入kafka索引中
    """

    def __init__(self, kafka_ip, kafka_topic, buffer_size=3, **kwargs):
        super().__init__(buffer_size=buffer_size)
        self.kafka_ip = kafka_ip
        self.kafka_topic = kafka_topic

    def write_batch(self, rows: list):

        conf = {
            'bootstrap.servers': self.kafka_ip,
            'client.id': 'python-producer',
            'security.protocol': 'plaintext',  # 明确指定明文协议
            'api.version.request': 'true',
            'message.max.bytes': 10485760,  # 10MB，必须 <= Broker 的 message.max.bytes

        }
        producer = Producer(conf)
        try:
            for row in rows:
                # 确保所有 bytes 数据被转换为字符串
                processed_row = {}
                for key, value in row.items():
                    if isinstance(value, bytes):
                        processed_row[key] = value.decode('utf-8')
                    else:
                        processed_row[key] = value
                id = row['id']
                producer.produce(
                    self.kafka_topic,
                    key=str(id),
                    value=json.dumps(processed_row),
                    callback=self.delivery_report
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

    def delivery_report(self, err, msg):
        """回调函数，用于确认消息是否成功发送"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
