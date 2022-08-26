import pymongo


def get_component(db, name = None, version = None):
    """
    If no version is specified, it'll get the latest.
    """
    query = {}
    if name is not None:
        query["name"] = name

    if version is not None:
        query["version"] = version
    return db.components.find_one(
        query, sort = [["version", -1], ["_id", -1]])
