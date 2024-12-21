from typing import Any

from wikidata_filter.iterator.base import JsonIterator


class Node(JsonIterator):
    def on_data(self, data: Any, *args):
        print('node2.Node', data)
        return data