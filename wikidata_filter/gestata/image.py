import os
from wikidata_filter.gestata.digest import pad20


def add_meta(image: dict, store_key='store_path'):
    """输入图片文件名"""
    if 'filename' not in image and 'url' in image:
        image['filename'] = image['url'][image['url'].rfind('/')+1:]
    filename = image.get("filename") or ""
    if '.' in filename:
        suffix = filename[filename.rfind('.')+1:].lower() or 'jpg'
    else:
        suffix = 'jpg'
    image["image_type"] = suffix
    image["size"] = len(image.get("data", []))
    if '_id' in image:
        sid = pad20(image['_id'])
        image['sid'] = sid
        image[store_key] = '/'.join([sid[:3], sid[3:6], sid]) + '.' + suffix

    return image


def cut(input_image_path: str, output_image_path: str, left: int = 0, right: int = 0, top: int = 0, bottom: int = 0):
    from PIL import Image
    original_image = Image.open(input_image_path)
    width, height = original_image.size
    box = (left, top, width - left - right, height - top - bottom)
    res_image = chunk = original_image.crop(box)
    res_image.save(output_image_path)


def split(input_image_path: str, output_folder: str, chunk_height: int = 500, offset: int = 0):
    """
    将高图片垂直切分成多个固定高度的小图片

    参数:
        input_image_path: 输入图片路径
        output_folder: 输出文件夹路径
        chunk_height: 每个小图片的固定高度(像素)
    """
    from PIL import Image
    # 确保输出文件夹存在
    os.makedirs(output_folder, exist_ok=True)

    # 打开原始图片
    original_image = Image.open(input_image_path)
    width, height = original_image.size

    # 计算需要切分成多少个小图片
    num_chunks = (height - offset) // chunk_height
    if (height - offset) % chunk_height != 0:
        num_chunks += 1

    print(f"将图片切分成 {num_chunks} 个小图片，每个高度为 {chunk_height} 像素")

    # 切分并保存小图片
    for i in range(num_chunks):
        # 计算当前切片的y坐标
        y_start = offset + i * chunk_height
        y_end = min(offset + (i + 1) * chunk_height, height)

        # 定义切片区 (left, upper, right, lower)
        box = (0, y_start, width, y_end)

        # 切分图片
        chunk = original_image.crop(box)

        # 生成输出文件名
        base_name = os.path.splitext(os.path.basename(input_image_path))[0]
        output_path = os.path.join(output_folder, f"{base_name}_part_{i + 1}.png")

        # 保存切分后的小图片
        chunk.save(output_path)
        print(f"已保存: {output_path}")

    original_image.close()
    print("图片切分完成!")


def find_paragraph_breaks(image, min_gap=20):
    """
    查找段落之间的空白行作为分界点
    """
    import numpy as np

    gray = image.convert('L')
    img_array = np.array(gray)
    height, width = img_array.shape

    # 查找空白行(所有像素值大于阈值)
    blank_rows = []
    threshold = 240  # 空白行的阈值，可根据图片调整

    for y in range(height):
        if np.all(img_array[y] > threshold):
            blank_rows.append(y)

    # 找出段落之间的分界点(连续空白行中的中间位置)
    breaks = []
    if not blank_rows:
        return breaks

    start = blank_rows[0]
    for i in range(1, len(blank_rows)):
        if blank_rows[i] - blank_rows[i - 1] > 1:  # 不连续的空白行
            mid = (start + blank_rows[i - 1]) // 2
            if (mid - start) >= min_gap:  # 只有足够大的空白才认为是段落分隔
                breaks.append(mid)
            start = blank_rows[i]

    # 添加最后一个分界点
    mid = (start + blank_rows[-1]) // 2
    if (mid - start) >= min_gap:
        breaks.append(mid)

    return breaks


def split_by_paragraphs(input_image_path, output_folder, min_chunk_height=500, max_chunk_height=2000, start_num=1):
    """
    基于段落空白进行智能切分
    """
    from PIL import Image

    os.makedirs(output_folder, exist_ok=True)
    original_image = Image.open(input_image_path)
    width, height = original_image.size

    # 查找段落分界点
    breaks = find_paragraph_breaks(original_image)
    breaks.append(height)  # 添加图片底部作为最后一个分界点

    print(f"找到段落分界点: {breaks}")

    # 确保每个切分块在最小和最大高度之间
    adjusted_breaks = [0]
    for br in breaks:
        if br - adjusted_breaks[-1] < min_chunk_height:
            continue  # 跳过太小的分段
        elif br - adjusted_breaks[-1] > max_chunk_height:
            # 如果段落太大，按最大高度切分
            num_splits = (br - adjusted_breaks[-1]) // max_chunk_height
            for i in range(1, num_splits + 1):
                adjusted_breaks.append(adjusted_breaks[-1] + max_chunk_height)
        else:
            adjusted_breaks.append(br)

    # 切分图片
    for i in range(len(adjusted_breaks) - 1):
        y_start = adjusted_breaks[i]
        y_end = adjusted_breaks[i + 1]

        box = (0, y_start, width, y_end)
        chunk = original_image.crop(box)

        base_name = os.path.splitext(os.path.basename(input_image_path))[0]
        output_path = os.path.join(output_folder, f"{base_name}_part_{start_num + i}.png")
        chunk.save(output_path)
        print(f"已保存: {output_path} (高度: {y_end - y_start}像素)")

    original_image.close()
    print("图片切分完成!")


if __name__ == "__main__":
    input_image = "../../data/images/long-1.png"  # 替换为你的图片路径
    output_dir = "../../test/long-image"  # 输出文件夹
    chunk_size = 1400  # 每个小图片的高度(像素)

    # split(input_image, output_dir, chunk_size, offset=850)
    # cut(input_image, "../../test/cut.png", left=550, top=850)
    # cut("../../data/images/long-2.png", "../../test/cut2.png", left=550)

    # split_by_paragraphs("../../test/cut.png", "../../test/long-image", min_chunk_height=1000, max_chunk_height=2000)
    split_by_paragraphs("../../test/cut2.png", "../../test/long-image", min_chunk_height=1000, max_chunk_height=2000, start_num=23)
