import sys
import signal
from typing import Any
from types import GeneratorType
from wikidata_filter.loader import DataProvider
from wikidata_filter.loader.base import Array, String
from wikidata_filter.iterator.base import Message, JsonIterator

from wikidata_filter.flow import Flow


process_status = {
    "stop": 0
}


def handle_sigint(signum, frame):
    process_status["stop"] += 1
    if process_status["stop"] > 1:
        print("强制退出")
        sys.exit(0)


def run(data_provider: DataProvider, processor: JsonIterator):
    # 注册信号处理程序
    signal.signal(signal.SIGINT, handle_sigint)

    print(f"Run flow: \nloader: {data_provider}\nprocessor: {processor}")
    print("------------------------")
    processor.on_start()

    # 注意，由于iterator.on_data可能包含yield，因此仅调用iterator.on_data(data)可能不会真正执行
    def execute(data: Any, *args):
        res = processor.__process__(data)
        if isinstance(res, GeneratorType):
            for _ in res:
                pass

    for item in data_provider.iter():
        execute(item)
        if process_status["stop"] > 0:
            print("\n接收到 Ctrl+C 信号，正在优雅退出...")
            processor.on_complete()
            sys.exit(0)

    data_provider.close()

    execute(Message.end())

    processor.on_complete()
    print("------------------------")


def run_flow(flow: Flow, input_data=None):
    print('starting flow:', flow.name)
    # 根据命令行参数构造loader
    if input_data:
        if isinstance(input_data, str):
            _loader = String(input_data)
        elif isinstance(input_data, list):
            _loader = Array(input_data)
        else:
            _loader = Array([input_data])
        flow.loader = _loader
    assert flow.loader is not None, "loader为空！可通过yaml文件或命令行参数进行配置"
    run(flow.loader, flow.processor)
