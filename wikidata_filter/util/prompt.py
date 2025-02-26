

def template(temp: str = None):
    def _format1(data: str or dict):
        if isinstance(data, dict):
            query = temp
            for key, val in data.items():
                query = query.replace('{'+key+'}', str(val))
                # query = query.format(**{key: val})
        else:
            query = temp.format(data=data)
        return query

    def _format2(data: str or dict):
        return data

    return _format1 if temp else _format2


class Config:
    """根据配置确定每个参数如何填充"""
    def __init__(self, templ: str, **kwargs):
        self.templ = templ
        self.configs = kwargs

    def __call__(self, data: dict, *args, **kwargs):
        format_data = {}
        for key, format_type in self.configs:
            val = data.get(key)
            if format_type == 'chunk_list' and isinstance(val, list):
                all_chunks = []
                for i, chunk in enumerate(val):
                    all_chunks.append(f'<chunk>{i + 1}: ' + chunk + '</chunk>')
                format_data[key] = '\n'.join(all_chunks)
            elif format_type == 'chunk':
                format_data[key] = f'<chunk>' + str(val) + '</chunk>'
            else:
                format_data[key] = str(val)

        return self.templ.format(**format_data)
