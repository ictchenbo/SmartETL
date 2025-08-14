"""POC相关处理算子"""
import yaml

chunk_temp = '''漏洞名称: {name}
漏洞描述: {desc}

漏洞POC（HTTP协议，NUCLEI-YAML格式）:
```yaml
{code}
```
'''


def simple_chunk(poc: dict):
    name = poc.get('name')
    desc = poc.get('description')
    http = {'http': poc.get('code')}
    code = yaml.dump(http, allow_unicode=True, sort_keys=False, indent=2)
    return chunk_temp.format(name=name, desc=desc, code=code)
