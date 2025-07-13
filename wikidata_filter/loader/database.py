from typing import Iterable, Any

from wikidata_filter.loader.base import DataProvider
from wikidata_filter.util.database.base import Database
from wikidata_filter.gestata.dbops import tables


class Scroll(DataProvider):
    """按照条件进行数据库表扫描读取。使用方式：`database.Scroll(db, ...) `"""
    def __init__(self, db: Database, **kwargs):
        self.db = db
        self.kwargs = kwargs

    def iter(self) -> Iterable[Any]:
        return self.db.scroll(**self.kwargs)


class Tables(DataProvider):
    """获取数据库的表。使用方式：`database.Tables(db, ...) `"""
    def __init__(self, db: Database, *databases, columns: bool = False, **kwargs):
        self.db = db
        self.databases = databases
        self.columns = columns

    def iter(self) -> Iterable[Any]:
        return tables(self.db, *self.databases, columns=self.columns)
