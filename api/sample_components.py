import pymongo
from bson.objectid import ObjectId

def get_sample_component_by_id(db, sample_component_id, connection_name = "default"):
    return db.sample_components.find_one({"_id": ObjectId(sample_component_id)})

def find_sample_component_ids_by_sample_id(db, sample_component_id, connection_name = "default"):
    if isinstance(sample_component_id, ObjectId):
        return db.sample_components.find({"sample._id": sample_component_id})
    return db.sample_components.find({"sample._id": ObjectId(sample_component_id)})


def delete_sample_component_by_id(db, sample_component_id, connection_name = "default"):
    if isinstance(sample_component_id, ObjectId):
        return db.sample_components.delete_one({"_id": sample_component_id})
    return db.sample_components.delete_one({"_id": ObjectId(sample_component_id)})
