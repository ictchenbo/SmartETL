from .base import Database


class Redis(Database):
    def exists(self, _id, **kwargs):
        return False
