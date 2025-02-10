"""
DeepSeek大模型服务调用算子
访问https://api-docs.deepseek.com/zh-cn/
"""
from .base import LLM


class DeepSeek(LLM):
    """月之暗面（Kimi）大模型封装算子"""
    def __init__(self,
                 api_key: str,
                 key: str,
                 api_base: str = "https://api.deepseek.com",
                 model: str = "deepseek-chat", **kwargs):
        """
                :param api_key API的Key 必须
                :param key 输入的字段名，如果为None，表示整个输入作为大模型请求参数，否则，提取该字段的值
                :param ignore_errors 是否忽略错误 如果为False且调用发生错误则抛出异常，默认True
                :param prompt 提示模板，与field搭配使用 用{data}来绑定输入数据
        """
        super().__init__(api_base, key=key, api_key=api_key, model=model, **kwargs)
