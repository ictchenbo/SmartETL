import os
from io import BytesIO
import requests
import time


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


def image(url: str, *args,
          min_size: int = 2048,
          min_width: int = 10,
          min_height: int = 10,
          max_width: int = 2000,
          min_ratio: float = 0.4,
          **kwargs):
    """下载图片 并判断图片大小是否符合要求"""
    try:
        from PIL import Image
    except ImportError as e:
        print("PIL not installed")
        raise e

    try:
        c = content(url, **kwargs)
    except:
        print("Failed to download image:", url)
        return None

    if len(c) < min_size:
        print("image file size too small")
        return None

    try:
        img = Image.open(BytesIO(c))
    except:
        print("Failed to open image:", url)
        return None

    width, height = img.size
    # 判断图片尺寸是否符合要求
    if width < min_width or height < min_height or width >= max_width:
        print("image size too small or too big:", url)
        return None
    if height / width < min_ratio:
        print("height/width ratio too low:", url)
        return None
    return c


def download(url: str, filename: str, save_path: str = None, **kwargs):
    """下载大文件并保存到本地"""
    if save_path:
        filename = os.path.join(save_path, filename)
    try:
        # 发起请求，设置 stream=True 以启用流式下载
        with requests.get(url, stream=True, **kwargs) as response:
            response.raise_for_status()  # 检查请求是否成功

            total_size = int(response.headers.get('content-length', 0))  # 获取文件总大小
            downloaded_size = 0

            with open(filename, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):  # 每次读取 8KB
                    if chunk:  # 过滤掉空的 chunk
                        file.write(chunk)
                        downloaded_size += len(chunk)

                        # 可选：显示下载进度
                        if total_size > 0:
                            percent = (downloaded_size / total_size) * 100
                            print(f"\r下载进度: {downloaded_size}/{total_size} bytes ({percent:.2f}%)", end='')

            print(f"\n文件已下载完成: {filename}")
            return True

    except requests.exceptions.RequestException as e:
        print(f"下载失败: {e}")
        return False
