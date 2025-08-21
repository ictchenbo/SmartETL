import os

from smartetl.component_manager import ComponentManager, LOADER_MODULE, PROCESSOR_MODULE
from smartetl.loader.base import Loader
from smartetl.processor.base import Processor


def setup_logging(print_mode: str = 'keep', **kwargs):
    """"""
    import builtins
    import logging
    from smartetl.util.logger import ProductionLogger

    default_config = dict(name="smartetl", log_level=logging.DEBUG, log_file='smartetl.log')
    default_config.update(kwargs)

    system_logger = ProductionLogger(**default_config)

    def my_info(*args, **kwargs):
        system_logger.info('\t'.join([str(v) for v in args]))

    def my_debug(*args, **kwargs):
        system_logger.debug('\t'.join([str(v) for v in args]))

    def my_warning(*args, **kwargs):
        system_logger.warning('\t'.join([str(v) for v in args]))

    def my_error(*args, **kwargs):
        system_logger.error('\t'.join([str(v) for v in args]))

    def my_print(*args, **kwargs):
        builtins.original_print(*args, **kwargs)
        my_info(*args, **kwargs)

    builtins.debug = my_debug
    builtins.info = my_info
    builtins.warning = my_warning
    builtins.error = my_error

    # 备份原始 print
    builtins.original_print = builtins.print

    # 替换全局 print
    if print_mode == 'replace':
        builtins.print = my_info
    elif print_mode == 'merge':
        builtins.print = my_print


class Flow:
    """流程类 表示一个处理流程 包括基础变量、数据加载节点、处理节点"""
    comp_mgr = ComponentManager()

    def __init__(self, flow: dict, *args, loader: str = None, processor: str = None, **kwargs):
        args_num = int(flow.get('arguments', '0'))
        assert len(args) >= args_num, f"no enough arguments! {args_num} needed!"
        self.name = flow.get('name')
        self.limit = None
        if 'limit' in flow:
            self.limit = int(flow.get('limit'))

        # init context
        self.init_base_envs(args, kwargs)
        # init consts
        self.init_consts(flow.get('consts') or {})

        # 启动格式化日志
        if flow.get('logging'):
            setup_logging(**flow['logging'])

        # init nodes
        self.init_nodes(flow.get('nodes') or {})

        # init loader, maybe None
        if loader is not None:
            if isinstance(loader, str):
                self.loader = self.comp_mgr.init_node(loader, label=LOADER_MODULE)
            else:
                assert isinstance(loader, Loader), "loader must be instance of DataProvider"
                self.loader = loader
        else:
            self.loader = self.comp_mgr.init_node(flow.get('loader'), label=LOADER_MODULE)

        if processor is not None:
            if isinstance(processor, str):
                self.processor = self.comp_mgr.init_node(processor, label=PROCESSOR_MODULE)
            else:
                assert isinstance(processor, Processor), "processor must be instance of JsonIterator"
                self.processor = processor
        else:
            self.processor = self.comp_mgr.init_node(flow.get('processor'), label=PROCESSOR_MODULE)

    def init_base_envs(self, args: tuple or list, kwargs: dict):
        """初始化基础变量 包括命令行参数"""
        self.comp_mgr.register_var('argc', len(args))
        self.comp_mgr.register_var('args', list(args))
        for i in range(len(args)):
            self.comp_mgr.register_var(f'arg{i + 1}', args[i])
        for k, v in kwargs.items():
            self.comp_mgr.register_var(f'__{k}', v)

    def __const__(self, val):
        """递归初始化常量值 如果值以$开头 表示引用环境变量"""
        if isinstance(val, tuple) or isinstance(val, list):
            return [self.__const__(vi) for vi in val]
        if isinstance(val, dict):
            return {k: self.__const__(vi) for k, vi in val.items()}
        if isinstance(val, str):
            if val.startswith("$"):
                # consts的字符串变量如果以$开头 则获取环境变量
                return os.environ.get(val[1:])
            elif val.startswith('eval(') and val.endswith(')'):
                return self.comp_mgr.eval(val[5:-1])
        return val

    def init_consts(self, consts_def: dict):
        """初始化常量 如果值以$开头 表示引用环境变量"""
        for k, val in consts_def.items():
            val = self.__const__(val)
            self.comp_mgr.register_var(k, val)

    def init_nodes(self, nodes_def: dict):
        """初始化节点 支持普通节点、loader节点和非流程节点"""
        for k, expr in nodes_def.items():
            expr = expr.strip()

            # 特殊逻辑 支持nodes中初始化loader组件
            label = None
            if k.startswith("loader"):
                label = LOADER_MODULE

            node = self.comp_mgr.init_node(expr, label=label)
            self.comp_mgr.register_var(k, node)
