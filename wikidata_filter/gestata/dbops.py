from wikidata_filter.util.database.base import Database


def tables(db: Database, *database_list, columns: bool = False):
    if database_list:
        for database in database_list:
            for table in db.list_tables(database):
                yield {
                    "name": table,
                    "columns": db.desc_table(table, database) if columns else [],
                    "database": database
                }
    else:
        for table in db.list_tables():
            yield {
                "name": table,
                "columns": db.desc_table(table) if columns else []
            }


def search(db: Database, **kwargs):
    return db.search(**kwargs)


def scroll(db: Database, **kwargs):
    return db.scroll(**kwargs)


def upsert(row: dict or list, db: Database, **kwargs):
    db.upsert(row, **kwargs)
    return row


def delete(_id, db: Database, **kwargs):
    """调用db对象，执行删除方法"""
    return db.delete(_id, **kwargs)


def exists(_id, db: Database, **kwargs) -> bool:
    return db.exists(_id, **kwargs)
