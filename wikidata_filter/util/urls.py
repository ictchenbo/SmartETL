HTTP = "http://"
HTTPS = "https://"


def host_from_url(url: str, with_proto=True):
    """从url中提取host部分"""
    if url.startswith(HTTP):
        proto = HTTP
        url = url[len(HTTP):]
    elif url.startswith(HTTPS):
        proto = HTTPS
        url = url[len(HTTPS):]
    else:
        proto = HTTP

    pos = url.find('/')
    if pos > 0:
        url = url[:pos]

    return proto + url if with_proto else url


def website_favicon(url: str, proxy: str = None, output_format: str = "base64", **kwargs):
    """获取站点的favicon图片"""
    import requests
    src = f'{url}/favicon.ico'
    proxies = {}
    if proxy:
        proxies['https'] = proxy
        proxies['http'] = proxy
    res = requests.get(src, proxies=proxies, **kwargs)
    if res.status_code == 200:
        content = res.content
        if output_format == "base64":
            import base64
            base64_data = base64.b64encode(content).decode('utf-8')
            return base64_data
        return content
    else:
        return None
