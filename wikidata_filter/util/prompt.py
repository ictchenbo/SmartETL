

def template(temp: str = None):
    def _format1(data: str or dict):
        if isinstance(data, dict):
            query = temp
            for key, val in data.items():
                query = query.format(**{key: val})
        else:
            query = temp.format(data=data)
        return query

    def _format2(data: str or dict):
        return data

    return _format1 if temp else _format2
