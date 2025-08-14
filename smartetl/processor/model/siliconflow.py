"""
Siliconflow大模型服务调用算子
访问https://siliconflow.cn/zh-cn/models获取可用的模型的名称
"""
from .base import LLM


class Siliconflow(LLM):
    """Siliconflow平台大模型封装算子"""
    def __init__(self,
                 key: str,
                 model: str,
                 api_key: str,
                 api_base: str = "https://api.siliconflow.cn/v1",
                 **kwargs):
        """Siliconflow平台大模型封装算子

        :param api_key API的Key 必须
        :param key 输入的字段名，如果为None，表示整个输入作为大模型请求参数，否则，提取该字段的值
        :param model 模型名称 必须
        """
        super().__init__(api_base, api_key=api_key, key=key, model=model)
