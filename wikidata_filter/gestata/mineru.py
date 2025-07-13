import requests
import traceback


def extract(row: dict,
            api_base: str = "http://10.60.1.148:6200/api/file/_extract",
            name_key: str = "name",
            content_key: str = "content",
            method: str = "auto",
            response_content: str = "md",
            **kwargs):
    """
    基于MinerU服务（封装）抽取文件（支持pdf/word等），按指定格式返回（默认markdown）
    :param row 待处理的dict记录
    :param api_base 自封装的MinerU服务地址
    :param name_key 待抽取的文件的名称字段，默认为`name`
    :param content_key 待抽取的文件内容（bytes）或文件名
    :param method 抽取的方法，支持text/ocr/auto，默认为auto，表示自动识别
    :param response_content 返回内容类型，支持md/json，默认为md
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
    try:
        response = requests.post(api_base, files=files, data=data)
        response_data = response.json()
        return response_data['data']['extract_data']
    except:
        traceback.print_exc()
        return None


if __name__ == '__main__':
    content = extract({"content": "../../test_data/files/test.pdf"})
    with open("result.md", "w", encoding="utf8") as fout:
        fout.write(content)
