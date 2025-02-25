from wikidata_filter.util.database.qdrant import Qdrant as QdrantClient
from wikidata_filter.iterator.mapper import Map


class Qdrant(Map):
    def __init__(self, host: str = 'localhost',
                 port: int = 6333,
                 api_key=None,
                 collection: str = "chunks",
                 fetch_size: int = 5,
                 **kwargs):
        super().__init__(self, **kwargs)
        self.client = QdrantClient(host=host, port=port, api_key=api_key)
        self.collection = collection
        self.fetch_size = fetch_size

    def __call__(self, query_vector: list, *args, **kwargs):
        return self.client.search(self.collection, query_vector=query_vector, limit=self.fetch_size)
