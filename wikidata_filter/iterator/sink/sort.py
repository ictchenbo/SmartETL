from wikidata_filter.iterator.sink.base import Collect


class Sort(Collect):
    """对收集后的数据进行排序后"""
    def __init__(self, keyfunc=None, key: str = None, reverse: bool = False, mode="single"):
        """
        :param keyfunc 排序键函数（优先）
        :param key 指定用于排序的字段名（假设待排序数据为dict）
        :param reverse 是否倒序（默认False）
        :param mode 输出模式 stop表示不传递；bypass 通过模式；batch 批量模式；single 单条模式（默认）
        """
        super().__init__(mode=mode)
        self.key = keyfunc
        if not keyfunc and key:
            self.key = lambda r: r.get(key)
        self.reverse = reverse

    def data_finished(self):
        self.buffer.sort(key=self.key, reverse=self.reverse)
