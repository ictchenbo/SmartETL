
class Database:
    """数据库的基类"""

    def list_tables(self, database: str = None):
        """列出所有的表"""
        pass

    def desc_table(self, table: str, database: str = None):
        """获取指定表的列"""
        pass

    def upsert(self, items: dict or list, **kwargs):
        """以upsert模式写入数据"""
        pass

    def scroll(self, **kwargs):
        """扫描数据库"""
        pass

    def exists(self, _id, **kwargs):
        """判断制定记录是否存在"""
        pass

    def delete(self, ids, **kwargs):
        """删除制定的标识或记录（单条或列表）"""
        pass

    def close(self):
        pass
