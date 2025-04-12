import requests


def extract(row: dict,
            api_base: str = None,
            name_key: str = "name",
            content_key: str = "content",
            method: str = "auto",
            response_content: str = "markdown",
            **kwargs):
    """
    基于MinerU服务（封装）抽取文件（支持pdf/word等），按指定格式返回（默认markdown）
    :param row 待处理的dict记录
    :param api_base 自封装的MinerU服务地址
    :param name_key 待抽取的文件的名称字段，默认为`name`
    :param content_key 待抽取的文件内容（bytes）或文件名
    :param method 抽取的方法，支持text/ocr/auto，默认为auto，表示自动识别
    :param response_content 返回内容类型，支持markdown/json，默认为markdown
    """
    content = row[content_key]
    assert isinstance(content, bytes) or isinstance(content, str), f"content field `{content_key}`must be bytes or str"

    filename = row.get(name_key, 'auto_file')

    if isinstance(content, bytes):
        files = {'file': (filename, content)}
    else:
        with open(content, 'rb') as reader:
            files = {'file': (filename, reader.read())}

    data = {
        'method': method,
        'response_content': response_content
    }

    response = requests.post(api_base, files=files, data=data)
    response_data = response.json()
    return response_data['data']
