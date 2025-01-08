import html
import re
import requests


def _chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class GdeltParser:
    def __init__(self, api_base: str):
        self.parser_uri = api_base
        self.title_pattern = '(?<=<title>)(.*?)(?=</title>)'
        self.title_ext_version = 1
        self.paragraph_version = 2
        # self.gdelt_website_info = load_site_info_clickhouse()

    def _parse_data(self, data: list):
        r = requests.post(self.parser_uri, json=data, timeout=60)
        if str(r.status_code).startswith('2'):
            res = r.json()
            return res
        else:
            print(f'error response from gdelt parser. {r.status_code}:{r.text}')
            return [{} for _ in data]

    def parse_batch_data(self, data: list):
        result = []
        for batch in _chunks(data, 5):
            result.extend(self._parse_data(batch))
        if len(result) != len(data):
            print(f'input data length {len(data)}, but output data length {len(result)}.')
        return result

    def add_content(self, data_list: list):
        html_list = [d['html'] for d in data_list]
        result = self.parse_batch_data(html_list)
        if len(result) == len(data_list):
            for i, data in enumerate(data_list):
                # 移除HTML
                data.pop("html")
                # 添加site-info
                # data['site-info'] = self.get_website_info(data['url'])
                data['content'] = result[i]
                # 用正则表达式抽取title
                title_ext = self.extract_title(html_list[i])
                data['content']['title_ext'] = title_ext
                # data['_title_ext_version'] = self.title_ext_version
                # 给content进行分段
                # if 'content' in data['content'] and is_not_blank(data['content']['content']):
                #     data['content']['content_paragraphs'] = split_lines(data['content']['content'])
                # data['_paragraph_version'] = self.paragraph_version

    def extract_title(self, html_cont: str):
        match = re.search(self.title_pattern, html_cont)
        if match:
            text = html.unescape(match.group(0))
            title_list = list(map(lambda s: s.strip(), re.split(' - | \| | – | — ', text)))
            long_title = str(max(title_list, key=len))
            long_index = title_list.index(long_title)
            if long_index == 0:
                return long_title
            pos = text.index(long_title) + len(long_title)
            return text[:pos]
        return None


if __name__ == '__main__':
    parser = GdeltParser()
    test_cont = '<title>‘I love my job’ | CATA bus operator Jay Staley shares past seven months living, working in ' \
                'State College | Penn State, State College News | collegian.psu.edu</title> '
    print(parser.extract_title(test_cont))
