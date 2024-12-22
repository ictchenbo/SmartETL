import os
from wikidata_filter.component_manager import ComponentManager, LOADER_MODULE, PROCESSOR_MODULE


class Flow:
    comp_mgr = ComponentManager()

    def __init__(self, flow: dict, *args, **kwargs):
        args_num = int(flow.get('arguments', '0'))
        assert len(args) >= args_num, f"no enough arguments! {args_num} needed!"

        self.name = flow.get('name')

        # init context
        self.init_base_envs(*args, **kwargs)
        # init consts
        self.init_consts(flow.get('consts') or {})

        # init nodes
        self.init_nodes(flow.get('nodes') or {})

        # init loader, maybe None
        self.loader = self.comp_mgr.init_node(flow.get('loader'), label=LOADER_MODULE)

        # init processor, maybe None
        self.processor = self.comp_mgr.init_node(flow.get('processor'), label=PROCESSOR_MODULE)

    def init_base_envs(self, *args, **kwargs):
        for i in range(len(args)):
            self.comp_mgr.register_var(f'arg{i + 1}', args[i])
        for k, v in kwargs.items():
            self.comp_mgr.register_var(f'__{k}', v)

    def init_consts(self, consts_def: dict):
        for k, val in consts_def.items():
            if isinstance(val, str) and val.startswith("$"):
                # consts的字符串变量如果以$开头 则获取环境变量
                val = os.environ.get(val[1:])
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
