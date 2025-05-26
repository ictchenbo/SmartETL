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
