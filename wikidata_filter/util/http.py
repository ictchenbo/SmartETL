import requests
import time
from PIL import Image
from io import BytesIO


def content(url, most_times=1, ignore_error=False, **kwargs):
    for i in range(most_times):
        try:
            res = requests.get(url, kwargs)
            if res.status_code == 200:
                return res.content
            print('Error to Get File', url, res.status_code, res.text)
        except:
            print('Network error')
        if i < most_times - 1:
            # wait 1 min
            time.sleep(60)
    print(f'Tried for {most_times}, Failure')
    if not ignore_error:
        raise Exception("Too many failures, exit!")
    return b""


def req(url, method='get', json: dict = None, **kwargs):
    if json is not None:
        return requests.request(method, url, json=json, **kwargs)

    return requests.request(method, url, **kwargs)


def text(url, method='get', **kwargs):
    res = req(url, method=method, **kwargs)
    if res.status_code == 200:
        return res.text
    return None


def json(url, method='get', **kwargs):
    res = req(url, method=method, **kwargs)
    if res.status_code == 200:
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
