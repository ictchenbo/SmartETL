import sys
import signal
from typing import Any
from types import GeneratorType
from smartetl.loader import DataProvider
from smartetl.processor.base import Message, Processor

from smartetl.flow import Flow


process_status = {
    "stop": 0
}


def handle_sigint(signum, frame):
    process_status["stop"] += 1
    # 连续按3次 Ctrl+C 可强制退出
    if process_status["stop"] >= 3:
        print("强制退出")
        sys.exit(0)


def run(data_provider: DataProvider, processor: Processor, limit: int = None):
    # 注册信号处理程序
    signal.signal(signal.SIGINT, handle_sigint)

    print(f"Run flow: \nloader: {data_provider}\nprocessor: {processor}")
    print("------------------------")
    processor.on_start()

    def execute(data: Any, *args):
        try:
            res = processor.__process__(data)
        except:
            return
        # 注意：包含yield的函数调用仅返回迭代器，而不会执行函数
        if isinstance(res, GeneratorType):
            for _ in res:
                pass
    row_count = 0
    for item in data_provider.iter():
        execute(item)
        row_count += 1
        if limit and row_count >= limit:
            print('\n达到limit限制，停止加载数据...')
            break
        if process_status["stop"] > 0:
            print("\n接收到 Ctrl+C 信号，正在优雅退出...")
            processor.on_complete()
            sys.exit(0)

    data_provider.close()

    execute(Message.end())

    processor.on_complete()
    print("------------------------")


def run_flow(flow: Flow):
    print('starting flow:', flow.name)
    assert flow.loader is not None, "loader为空！可通过yaml文件或命令行参数进行配置"
    run(flow.loader, flow.processor, limit=flow.limit)
