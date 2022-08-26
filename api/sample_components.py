import pymongo
from bson.objectid import ObjectId

def save_sample_component(data_dict, connection_name = "default"):
    """COPIED FROM BIFROSTLIB. Insert sample dict into mongodb.
    Return the dict with an _id element"""
    connection = get_connection(connection_name)
    db = connection.get_database()
    sample_components_db = db.sample_components
    now = date_now()
    data_dict["metadata"] = data_dict.get("metadata", {'created_at': now})
    data_dict["metadata"]["updated_at"] = now
    if "_id" in data_dict:
        data_dict = sample_components_db.find_one_and_update(
            filter = {"_id": data_dict["_id"]},
            update = {"$set": data_dict},
            # return new doc if one is upserted
            return_document = pymongo.ReturnDocument.AFTER,
            # This might change in the future. It doesnt make much sense with our current system.
            upsert = True
            # Import relies on this to be true.
            # insert the document if it does not exist
        )

    else:
        search_fields = {
            "sample._id"   : data_dict["sample"]["_id"],
            "component._id": data_dict["component"]["_id"],
        }
        data_dict = sample_components_db.find_one_and_update(
            filter = search_fields,
            update = {
                "$set": data_dict
            },
            # return new doc if one is upserted
            return_document = pymongo.ReturnDocument.AFTER,
            upsert = True  # insert the document if it does not exist
        )

    return data_dict


def get_sample_component_by_id(sample_component_id, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    return db.sample_components.find_one({"_id": ObjectId(sample_component_id)})

def find_sample_component_ids_by_sample_id(db, sample_component_id, connection_name = "default"):
    if isinstance(sample_component_id, ObjectId):
        return db.sample_components.find({"sample._id": sample_component_id})
    return db.sample_components.find({"sample._id": ObjectId(sample_component_id)})


def delete_sample_component_by_id(sample_component_id, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    if isinstance(sample_component_id, ObjectId):
        return db.sample_components.delete_one({"_id": sample_component_id})
    return db.sample_components.delete_one({"_id": ObjectId(sample_component_id)})
