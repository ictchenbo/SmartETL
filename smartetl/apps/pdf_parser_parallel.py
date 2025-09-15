import time
import traceback
from queue import Queue, PriorityQueue
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Optional
import shutil

from smartetl.gestata.mineru import extract
from smartetl.database.kafka import Kafka
from smartetl.util.sets import from_text
from smartetl.util.files import exists


kafka = Kafka(host='10.60.1.148:9092')
source_topic = 'arxiv_pdf_0911'
target_topic = 'arxiv_pdf_md_0911'
pdf_back_dir = '/data/datax/papers/arxiv/pdf-errors'


class Resource:
    """资源类，包含性能参数（处理一条任务所需时间，单位：秒）"""
    def __init__(self, rid):
        self.rid = rid
        self.process_time = 0
        self.failures = 0

    def __str__(self):
        return f"Resource-{self.rid}(speed={self.process_time:.1f}s)"

    def __lt__(self, other):
        # 堆根据处理时间排序：时间越短，优先级越高
        return self.process_time < other.process_time


class ResourceManager:
    """资源池管理器（带优先级）"""
    def __init__(self, resource_list: list):
        self.total_available = len(resource_list)
        self.available_resources = PriorityQueue(maxsize=self.total_available)

        for i in range(self.total_available):
            self.available_resources.put((0, Resource(resource_list[i])))

    def get_resource(self, timeout=None) -> Resource:
        return self.available_resources.get(timeout=timeout)[1]

    def release_resource(self, resource):
        self.available_resources.put((resource.process_time, resource))


def task(row: dict, resource: Resource):
    start_time = time.time()
    extract(row, api_base=resource.rid, name_key='filename', data_key='filepath', md_key='md')
    if row.get('md'):
        resource.process_time += time.time() - start_time
        # 抽取成功 放入目标队列
        kafka.upsert(row, topic=target_topic)
        return True
    else:
        error = row.get('ERROR', '')
        if error and "CUDA" not in error:
            print(f"[BAD PDF] move {row['filepath']} to {pdf_back_dir}")
            shutil.move(row['filepath'], pdf_back_dir)
            return True
        # 抽取失败 重新放入源队列中
        print("WARNING: fail to process, put back task")
        kafka.upsert(row, topic=source_topic)
        # 对该资源予以惩罚
        resource.process_time += 1000
        resource.failures += 1
        time.sleep(10)
        return False
        # 如果失败次数太多 则丢弃该资源
        # if resource.failures >= 5:
        #     print("WARNING: too many failures, drop this resource: ", resource.rid)
        #     resource_manager.total_available -= 1
        #     return


def worker(row: dict, commit_func, resource: Resource, resource_manager: ResourceManager):
    try:
        if task(row, resource):
            commit_func()
    finally:
        try:
            resource_manager.release_resource(resource)
            print(f"[RELEASE] Resource {resource.rid} released.")
        except Exception as e:
            print(f"Failed to release resource {resource.rid}: {e}")


def log_future_exception(future: Future):
    """Log exceptions from futures that were submitted."""
    if future.exception():
        print(f"Task failed with exception: {future.exception()}")


def run(service_list: list):
    resource_manager = ResourceManager(service_list)
    max_workers = max(len(service_list)*2, 20)
    executor = ThreadPoolExecutor(max_workers=max_workers)

    try:
        for row, commit in kafka.scroll(topic=source_topic):
            print("[TASK]", row)
            filepath = row.get('filepath')
            if not filepath or not exists(filepath):
                print("WARNING: file not found: ", filepath)
                commit()
                continue

            print("Waiting for a resource...")
            try:
                # ⚠️ IMPORTANT: Add timeout! Avoid indefinite blocking.
                resource = resource_manager.get_resource(timeout=60)
                print(f"[ALLOC] {resource.rid} assigned. Total time: {resource.process_time} sec")
                future = executor.submit(worker, row, commit, resource, resource_manager)
                future.add_done_callback(lambda f: log_future_exception(f))

            except Exception as e:
                print(f"Failed to get resource: {e}")
    finally:
        executor.shutdown(wait=True)


if __name__ == '__main__':
    import sys
    service_list = list(from_text(sys.argv[1]))
    print("#MinerU Instance: ", len(service_list))
    while True:
        try:
            run(service_list)
        except:
            traceback.print_exc()
        time.sleep(600)
