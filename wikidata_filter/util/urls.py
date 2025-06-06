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


def path_from_url(url: str, result: str = "all"):
    if url.startswith(HTTP):
        url = url[len(HTTP):]
    elif url.startswith(HTTPS):
        url = url[len(HTTPS):]
    elif '://' in url:
        url = url[url.find('://')+3:]
    path = '/'
    if '/' in url:
        path = url[url.find('/'):]
    if result == 'all':
        return path
    path = path[1:]
    if path.endswith('/'):
        path = path[:-1]
    if result == 'first':
        return path.split('/')[0]
    else:
        return path.split('/')[-1]


if __name__ == "__main__":
    print(path_from_url("https://arxiv.org/pdf/2501.00001", result="last"))
