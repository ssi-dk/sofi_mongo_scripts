import pymongo

from .utils import get_connection, date_now


def save_component(data_dict, connection_name = "default"):
    """COPIED FROM BIFROSTLIB. Insert sample dict into mongodb.
    Return the dict with an _id element"""
    connection = get_connection(connection_name)
    db = connection.get_database()
    now = date_now()
    data_dict["metadata"] = data_dict.get("metadata", {'created_at': now})
    data_dict["metadata"]["updated_at"] = now
    components_db = db.components  # Collection name is samples
    if "_id" in data_dict:
        data_dict = components_db.find_one_and_update(
            filter = {"_id": data_dict["_id"]},
            update = {"$set": data_dict},
            # return new doc if one is upserted
            return_document = pymongo.ReturnDocument.AFTER,
            upsert = True  # This might change in the future # insert the document if it does not exist
        )
    else:
        data_dict = components_db.find_one_and_update(
            filter = data_dict,
            update = {"$setOnInsert": data_dict},
            # return new doc if one is upserted
            return_document = pymongo.ReturnDocument.AFTER,
            upsert = True  # insert the document if it does not exist
        )
    return data_dict


def get_component(name = None, version = None, connection_name = "default"):
    """
    If no version is specified, it'll get the latest.
    """
    connection = get_connection(connection_name)
    db = connection.get_database()
    query = {}
    if name is not None:
        query["name"] = name

    if version is not None:
        query["version"] = version
    return db.components.find_one(
        query, sort = [["version", -1], ["_id", -1]])
