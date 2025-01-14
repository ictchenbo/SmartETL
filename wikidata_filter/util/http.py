import requests


def content(url, most_times=5, **kwargs):
    for i in range(most_times):
        try:
            res = requests.get(url, kwargs)
            if res.status_code == 200:
                return res.content
            print('Error to Get File', url, res.status_code, res.text)
        except:
            print('Network error')
    print(f'Tried for {most_times}, Failure')
    raise Exception("Too many failures, exit!")


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
