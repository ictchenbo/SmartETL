from PIL import Image
from io import BytesIO

from wikidata_filter.iterator.mapper import Map
from wikidata_filter.util.http import content


class Download(Map):
    """下载图片"""
    def __init__(self, min_width: int = 10, min_height: int = 10, **kwargs):
        super().__init__(self, **kwargs)
        self.min_width = min_width
        self.min_height = min_height

    def __call__(self, url: str, *args, **kwargs):
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
        if width < self.min_width or height < self.min_height:
            print(f"图片尺寸过小: {url}")
            return None
        return c
