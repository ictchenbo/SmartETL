import os.path

from wikidata_filter.flow_builder import FlowBuilder
from wikidata_filter.flow_engine import run_flow


if __name__ == '__main__':
    import argparse
    import json
    # 创建解析器对象
    parser = argparse.ArgumentParser(description="SmartETL: a simple but strong ETL framework")

    # 添加位置参数
    parser.add_argument("filename", type=str, default=None, help="yaml流程定义文件，或者流程名字")

    # 添加可选参数
    parser.add_argument("-i", "--input", type=str, default=None, help="直接提供流程输入数据")
    parser.add_argument("--json", default=False, action="store_true", help="将--input参数提供的输入数据作为json加载，默认为纯文本")
    parser.add_argument("--loader", default=None, help="指定Loader表达式")
    parser.add_argument("--processor", default=None, help="指定Processor表达式")

    # 解析参数
    args, unknown = parser.parse_known_args()
    input_data = args.input
    if input_data and args.json is True:
        input_data = json.loads(input_data)

    filename = args.filename

    if args.loader and args.processor:
        flow = FlowBuilder.from_cmd(filename, *unknown, loader=args.loader, processor=args.processor)
    elif os.path.exists(filename):
        flow = FlowBuilder.from_file(filename, *unknown)
    else:
        parser.print_help(__file__)
        print("either filename or loader+processor should be provided")
        exit(1)

    run_flow(flow, input_data=input_data)
