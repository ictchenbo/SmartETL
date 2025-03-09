import hashlib


def md5(text: str, bits: int = 16) -> str:
    """生成MD5"""
    hash_object = hashlib.md5(text.encode("utf8"))
    return hash_object.hexdigest()[:bits]
