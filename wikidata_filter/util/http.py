import requests
import time
from PIL import Image
from io import BytesIO


def req(url,
        method='get',
        most_times: int = 1,
        ignore_error: bool = False,
        wait_time: int = 30,
        content_type: str = "*",
        **kwargs):
    """通用HTTP请求方法"""
    for i in range(most_times):
        try:
            print("fetching:", url)
            res = requests.request(method, url, allow_redirects=True, **kwargs)
            res.raise_for_status()
            if content_type == "*" or content_type in res.headers.get('Content-Type', ''):
                return res
            return None
        except:
            print('Network error')
        if i < most_times - 1:
            time.sleep(wait_time)
    print(f'Tried for {most_times}, Failure')
    if not ignore_error:
        raise Exception("Too many failures, exit!")
    return None


def head(url, **kwargs):
    """head请求，获取响应头"""
    res = req(url, method='head', **kwargs)
    if res is not None:
        return res.headers
    return None


def content(url: str, method='get', **kwargs):
    """HTTP请求，获取响应字节"""
    res = req(url, method=method, **kwargs)
    if res is not None:
        return res.content
    return b''


def text(url, method='get', **kwargs):
    """HTTP请求，获取响应文本"""
    res = req(url, method=method, **kwargs)
    if res is not None:
        return res.text
    return ''


def json(url, method='get', **kwargs):
    """HTTP请求，获取JSON结果"""
    res = req(url, method=method, **kwargs)
    if res is not None:
        return res.json()
    return None


def download_image(url: str, *args, min_width: int = 10, min_height: int = 10, **kwargs):
    try:
        c = content(url, **kwargs)
        # 使用PIL来打开图片
        img = Image.open(BytesIO(c))
    except:
        print("Open image error:", url)
        return None

    width, height = img.size
    # print(f"下载的图片尺寸: {width}x{height}")
    # 判断图片尺寸是否符合要求
    if width < min_width or height < min_height:
        print(f"图片尺寸过小: {url}")
        return None
    return c
