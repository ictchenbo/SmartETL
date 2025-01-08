import os
from wikidata_filter.component_manager import ComponentManager, LOADER_MODULE, PROCESSOR_MODULE


class Flow:
    """流程类 表示一个处理流程 包括基础变量、数据加载节点、处理节点"""
    comp_mgr = ComponentManager()

    def __init__(self, flow: dict, *args, **kwargs):
        args_num = int(flow.get('arguments', '0'))
        assert len(args) >= args_num, f"no enough arguments! {args_num} needed!"
        loader_def = flow.get('loader')
        processor_def = flow.get('processor')
        assert loader_def and processor_def, "loader and processor must both exist"

        self.name = flow.get('name')

        # init context
        self.init_base_envs(args, kwargs)
        # init consts
        self.init_consts(flow.get('consts') or {})

        # init nodes
        self.init_nodes(flow.get('nodes') or {})

        # init loader, maybe None
        self.loader = self.comp_mgr.init_node(loader_def, label=LOADER_MODULE)

        # init processor, maybe None
        self.processor = self.comp_mgr.init_node(processor_def, label=PROCESSOR_MODULE)

    def init_base_envs(self, args: tuple or list, kwargs: dict):
        """初始化基础变量 包括命令行参数"""
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
        if isinstance(val, str) and val.startswith("$"):
            # consts的字符串变量如果以$开头 则获取环境变量
            return os.environ.get(val[1:])
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
