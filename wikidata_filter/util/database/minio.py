from .base import Database
from io import BytesIO

try:
    from minio import Minio
    from minio.error import S3Error
except:
    raise ImportError("minio not installed! Run: pip install minio")


class MinIO(Database):
    def __init__(self, host: str = "localhost",
                 port: int = 9000,
                 access_key="minioadmin",
                 secret_key="minioadmin",
                 secure: bool = False,
                 bucket: str = "",
                 auto_create: bool = False, **kwargs):
        """
        :param host
        :param port
        :param access_key
        :param secret_key
        :param secure=False
        :param bucket
        """
        self.minio_client = Minio(
            f'{host}:{port}',
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket = bucket
        self.kwargs = kwargs

        if auto_create and bucket and not self.minio_client.bucket_exists(bucket):
            self.minio_client.make_bucket(bucket)

    def upsert(self, items: dict or list, **kwargs):
        """将数据以指定格式写入指定桶和指定的文件。
            其中key_key指定文件名的字段；value_key指定写入文件数据所在字段，该字段含义由value_type指定。
            value_type=file表示是本地文件路径；value_type=json表示将数据以json写入文件；value_type=str表示将数据以简单字符串写入文件。
        """
        merged_kwargs = dict(**self.kwargs, **kwargs)
        if isinstance(items, list):
            for item in items:
                self.on_data(item, **merged_kwargs)
        else:
            self.on_data(items, **merged_kwargs)

    def on_data(self, data: dict,
                key_key: str = "_id",
                value_key: str = "_value",
                value_type: str = "file",
                encoding: str = "utf8", **kwargs):
        """
        :param data 待处理数据
        :param key_key="object_key" 指定对象ID所在的列
        :param value_key="object_value" 指定对象值所在的列
        :param value_type="file" 指定对象值类型 'file' 表示指定的文件路径（读取文件内容并写入MinIO） 'bytes' 指定直接内容（直接写入MinIO） 'json'指定将对象进行JSON序列化后写入MinIO
        :param encoding 文本文件的编码
        """
        object_key = data[key_key]
        object_value = data[value_key]

        if value_type == 'file':
            self.minio_client.fput_object(self.bucket, object_key, object_value)
        else:
            bytes_val = object_value
            if value_type == "json":
                import json
                bytes_val = json.dumps(object_value, ensure_ascii=False).encode(encoding)
            elif value_type == "str":
                bytes_val = object_value.encode(encoding)

            buffer = BytesIO(bytes_val)
            self.minio_client.put_object(self.bucket, object_key, buffer, len(bytes_val))

        print("object saved:", object_key)

        return data

    def get(self, _id, **kwargs):
        if isinstance(_id, str):
            try:
                return self.minio_client.get_object(self.bucket, _id).read()
            except:
                return None
        else:
            try:
                return [self.minio_client.get_object(self.bucket, _i).read() for _i in _id]
            except:
                return None

    def exists(self, _id, **kwargs):
        try:
            # 尝试获取对象元数据
            self.minio_client.stat_object(self.bucket, _id)
            return True
        except S3Error as err:
            if err.code == "NoSuchKey":
                return False
            else:
                print(f"发生错误: {err}")
        return False

    def delete(self, ids, **kwargs):
        if isinstance(ids, str):
            try:
                self.minio_client.remove_object(self.bucket, ids)
            except:
                pass
        else:
            for _id in ids:
                try:
                    self.minio_client.remove_object(self.bucket, ids)
                except:
                    pass

    def scroll(self, return_data: bool = False, prefix: str = None, recursive: bool = True, **kwargs):
        for obj in self.minio_client.list_objects(self.bucket, prefix=prefix, recursive=recursive, **kwargs):
            item = {
                "name": obj.object_name,
                "size": obj.size
                # TODO more fields?
            }
            if return_data:
                item["data"] = self.get(obj.object_name)

            yield item
