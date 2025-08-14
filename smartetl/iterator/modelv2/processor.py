from smartetl.iterator.mapper import Map
from smartetl.iterator.modelv2.base import LLMModel
from smartetl.util.prompt import template


class Processor(Map):
    """大模型调用处理节点"""
    def __init__(self,
                 model: LLMModel,
                 prompt: str = None,
                 key: str = None,
                 target_key: str = None,
                 stream: bool = False,
                 **kwargs
                 ):
        """
        :param model 模型
        :param prompt 提示模板
        :param key 输入的字段名，如果为None，表示整个输入作为大模型请求参数，否则，提取该字段的值
        :param target_key 目标字段
        :param stream 是否流式请求
        """
        super().__init__(self, key=key, target_key=target_key, **kwargs)
        assert model is not None, "model is None"
        self.model = model
        self.stream = stream
        if prompt and isinstance(prompt, str):
            self.prompt = template(prompt)
        else:
            self.prompt = prompt

    def __call__(self, query: str or dict, **kwargs):
        data = self.prompt(query) if self.prompt else query
        return self.model.chat(data, stream=self.stream, **kwargs)
