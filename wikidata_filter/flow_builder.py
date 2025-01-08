import os
import yaml
from wikidata_filter.flow import Flow
from wikidata_filter.util.dicts import merge_dicts


def abs_path(path: str):
    path = os.path.abspath(path)
    return path.replace('\\', '/')


class FlowBuilder:
    @staticmethod
    def check_flow(flow_def: dict):
        return True

    @staticmethod
    def load_yaml(flow_file: str, all_files: set, encoding: str = 'utf8') -> dict:
        """递归加载flow文件 允许多层继承"""
        assert os.path.exists(flow_file), f"No such flow file: {flow_file}"

        print('loading YAML flow from', flow_file)
        flow_def = yaml.load(open(flow_file, encoding=encoding), Loader=yaml.FullLoader)
        all_files.add(abs_path(flow_file))

        # Loading base flows
        base_flows = []
        if "from" in flow_def:
            base_flows = flow_def.pop("from") or []
            if isinstance(base_flows, str):
                base_flows = [base_flows]

        if not base_flows:
            return flow_def

        target = {}
        for base_flow in base_flows:
            # assert abs_path(base_flow) not in all_files, "Flow定义出现循环引用！"
            if abs_path(base_flow) in all_files:
                continue
            base = FlowBuilder.load_yaml(base_flow, all_files, encoding=encoding)
            merge_dicts(target, base)

        # merge this
        merge_dicts(target, flow_def)

        return target

    @staticmethod
    def from_file(flow_file: str, *args, encoding: str = 'utf8', **kwargs):
        """基于yaml流程文件构造流程"""
        flow_def = FlowBuilder.load_yaml(flow_file, set(), encoding=encoding)
        return Flow(flow_def, *args, **kwargs)

    @staticmethod
    def from_cmd(name, *args, loader: str = None, processor: str = None, **kwargs):
        """基于命令行参数构造流程"""
        flow_def = {
            "name": f"cli flow {name}",
            "arguments": len(args),
            "loader": loader,
            "processor": processor
        }
        return Flow(flow_def, *args, **kwargs)
