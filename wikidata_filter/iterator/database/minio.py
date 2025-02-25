from typing import Any
from io import BytesIO

try:
    from minio import Minio
    from minio.error import S3Error
except:
    raise ImportError("minio not installed! Run: pip install minio")

from wikidata_filter.iterator.base import DictProcessorBase


class Write(DictProcessorBase):
    """MinIO系统，将数据流程数据以指定格式写入指定桶和指定的文件。
        其中key_key指定文件名的字段；value_key指定写入文件数据所在字段，该字段含义由value_type指定。
        value_type=file表示是本地文件路径；value_type=json表示将数据以json写入文件；value_type=str表示将数据以简单字符串写入文件。
    """
    def __init__(self, host: str = "localhost", port: int = 9000,
                 access_key="minioadmin",
                 secret_key="minioadmin",
                 secure: bool = False,
                 bucket: str = "",
                 key_key: str = "_id",
                 value_key: str = "_value",
                 value_type: str = "file",
                 encoding: str = "utf8", **kwargs):
        """
        :param host
        :param port
        :param access_key
        :param secret_key
        :param secure=False
        :param bucket
        :param key_key="object_key" 指定对象ID所在的列
        :param value_key="object_value" 指定对象值所在的列
        :param value_type="file" 指定对象值类型 'file' 表示指定的文件路径（读取文件内容并写入MinIO） 'bytes' 指定直接内容（直接写入MinIO） 'json'指定将对象进行JSON序列化后写入MinIO
        """
        assert value_type in ["file", "bytes", "str", "json"], f"value_type({value_type}) not supported"
        self.minio_client = Minio(
            f'{host}:{port}',
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket = bucket
        self.key_key = key_key
        self.value_key = value_key
        self.value_type = value_type
        self.encoding = encoding

        # 确保 MinIO 中的存储桶存在
        if not self.minio_client.bucket_exists(bucket):
            self.minio_client.make_bucket(bucket)

    def on_data(self, data: dict, *args):
        object_key = data[self.key_key]
        object_value = data[self.value_key]

        if self.value_type == 'file':
            self.minio_client.fput_object(self.bucket, object_key, object_value)
        else:
            bytes_val = object_value
            if self.value_type == "json":
                import json
                bytes_val = json.dumps(object_value, ensure_ascii=False).encode(self.encoding)
            elif self.value_type == "str":
                bytes_val = object_value.encode(self.encoding)

            buffer = BytesIO(bytes_val)
            self.minio_client.put_object(self.bucket, object_key, buffer, len(bytes_val))

        print("object saved:", object_key)

        return data
