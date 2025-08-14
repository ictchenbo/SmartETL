from smartetl.processor.base import Processor
from smartetl.gestata.wikidata import read_dump


def run(infile: str, iterator: Processor, parallels: int = 1, parallel_runner: str = "multi_thread"):
    """ 主函数
    :param infile: 输入文件
    :param iterator 迭代器
    :param parallels 并发数
    :param parallel_runner 并发方法  multi_thread/multi_process
    """
    dump_loader = read_dump(infile)

    def process_item(val):
        iterator.on_data(val)

    iterator.on_start()
    if parallels > 1:
        if parallel_runner == "multi_thread":
            from concurrent.futures import ThreadPoolExecutor
            pool = ThreadPoolExecutor(max_workers=parallels)
            for item in dump_loader:
                pool.submit(process_item, (item,))
            pool.shutdown()
        else:
            import multiprocessing
            for item in dump_loader:
                sub_process = multiprocessing.Process(target=process_item, args=(item,))
                sub_process.start()
    else:
        for item in dump_loader:
            process_item(item)

    iterator.on_complete()
