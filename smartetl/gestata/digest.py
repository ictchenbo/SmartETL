import hashlib
import base64 as __base64


def md5_simple(text: str, bits: int = 16) -> str:
    """生成MD5"""
    hash_object = hashlib.md5(text.encode("utf8"))
    return hash_object.hexdigest()[:bits]


def md5(data: bytes):
    return hashlib.md5(data).hexdigest()


def sha256(data: bytes):
    return hashlib.sha256(data).hexdigest()


def sha256_id(data: bytes, length: int = 8):
    # 取前8字节转换为无符号64位整数
    digest = hashlib.sha256(data).digest()
    _id = int.from_bytes(digest[:length], byteorder='big', signed=False)
    return _id


def pad20(_id: int or str, length: int = 20):
    s_id = str(_id)
    while len(s_id) < length:
        s_id = '0' + s_id
    return s_id


def base64(data: bytes):
    return __base64.b64encode(data).decode('utf-8')


if __name__ == "__main__":
    print(sha256("a.jpg".encode("utf8")))
    print(sha256("b.jpg".encode("utf8")))
    print(len(md5("a.jpg".encode("utf8"))))
    _id = sha256_id("a.jpg".encode("utf8"))
    print(pad20(_id))
