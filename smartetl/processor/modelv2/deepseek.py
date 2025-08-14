"""
DeepSeek大模型服务调用算子
访问https://api-docs.deepseek.com/zh-cn/
"""
from .base import LLMModel


class DeepSeek(LLMModel):
    """DeepSeek大模型（官网）封装算子"""
    def __init__(self,
                 api_key: str,
                 api_base: str = "https://api.deepseek.com",
                 model: str = "deepseek-chat", **kwargs):
        """
                :param api_key API的Key 必须
                :param key 输入的字段名，如果为None，表示整个输入作为大模型请求参数，否则，提取该字段的值
                :param ignore_errors 是否忽略错误 如果为False且调用发生错误则抛出异常，默认True
                :param prompt 提示模板，与field搭配使用 用{data}来绑定输入数据
        """
        super().__init__(api_base, api_key=api_key, model=model, **kwargs)


class DeepSeek_LKEAP(LLMModel):
    """DeepSeek大模型（腾讯云）封装算子"""
    def __init__(self,
                 api_key: str,
                 api_base: str = "https://api.lkeap.cloud.tencent.com/v1",
                 model: str = "deepseek-r1", **kwargs):
        """
                :param api_key API的Key 必须
                :param key 输入的字段名，如果为None，表示整个输入作为大模型请求参数，否则，提取该字段的值
                :param ignore_errors 是否忽略错误 如果为False且调用发生错误则抛出异常，默认True
                :param prompt 提示模板，与field搭配使用 用{data}来绑定输入数据
        """
        super().__init__(api_base, api_key=api_key, model=model, **kwargs)
