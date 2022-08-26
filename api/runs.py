import pymongo
from bson.objectid import ObjectId

from .utils import get_connection, date_now


def check_run_name(name, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    # Fastest.
    run = db.runs.find({"name": name}).explain().get('executionStats', None).get('nReturned', 0)
    return run != 0


def get_run_list(run_type = None, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    if run_type is None:
        query = {}
    elif isinstance(run_type, list):
        query = {"$or": [{"type": x} for x in run_type]}
    else:
        query = {"type": run_type}
    runs = list(db.runs.find(query,
                             {"name"   : 1,
                              "_id"    : 1,
                              "samples": 1}).sort([['metadata.created_at', pymongo.DESCENDING]]))
    return runs


def get_last_runs(run, n, runtype, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()

    run = db.runs.find_one({"name": run})
    run_date = run.get("metadata", {}).get("created_at")

    if run_date is not None:
        if runtype is not None:
            query = {"metadata.created_at": {
                "$lte": run_date}, "type" : runtype}
        else:
            query = {"metadata.created_at": {"$lte": run_date}}
    else:
        if runtype is not None:
            query = {"type": runtype}
        else:
            query = {}
    return list(db.runs.find(query,
                             {"name": 1, "samples": 1}).sort([['metadata.created_at', pymongo.DESCENDING]]).limit(n))


def get_run(run_name, connection_name = "default"):
    # Return only one run or None.
    connection = get_connection(connection_name)
    db = connection.get_database()
    return db.runs.find_one({"name": run_name})


def get_runs(run_name, connection_name = "default"):
    # Return a list of runs or None.
    connection = get_connection(connection_name)
    db = connection.get_database()
    return db.runs.find({"name": run_name})


def get_run_by_id(run_id, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    return db.runs.find_one({"_id": ObjectId(run_id)})


def delete_run_by_id(run_id, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    return db.runs.delete_one({"_id": ObjectId(run_id)})


def get_comment(run_id, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    return db.runs.find_one(
        {"_id": run_id}, {"Comments": 1})


def set_comment(run_id, comment, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    ret = db.runs.find_one_and_update(
        {"_id": run_id}, {"$set": {"Comments": comment}})
    if ret is not None:
        return 1
    else:
        return 0


def create_virtual_run(name, ip, samples, connection_name = "default"):
    """
    Create virtual run. No files, only in db.
    Name is string
    Samples is list of dict with name and id
    """

    # Verify run doesn't exist
    if check_run_name(name = name, connection_name = connection_name):
        raise ValueError()

    event = [ip, "Create " + ",".join([str(x["_id"]) for x in samples])]

    # Convert sample list
    samples = [{"name": s["name"], "_id": ObjectId(s["_id"])} for s in samples]

    run = {
        "name"      : name,
        "samples"   : samples,
        "components": [],
        "metadata"  : {
            "created_at"    : date_now(),
            "updated_at"    : date_now(),
            "schema_version": 2.0,
            "modified_by"   : [event]
        },
        "type"      : "virtual"
    }
    connection = get_connection(connection_name)
    db = connection.get_database()
    rid = db.runs.insert_one(run).inserted_id
    return rid


def add_samples_to_virtual_run(name, ip, samples, connection_name = "default"):
    """
    Adds samples to virtual run, check that samples don't exist in the run before.
    """
    connection = get_connection(connection_name)
    db = connection.get_database()
    run = db.runs.find_one({"name": name, "type": "virtual"})
    if run is None:
        raise ValueError("Virtual run not found")
    run_samples = run["samples"]
    # Convert sample list
    samples = [{"_id": ObjectId(s["_id"]), "name": s["name"]} for s in samples]  # HB: strip payload from samples arg object
    for s in samples:
        if s not in run_samples:
            run_samples.append(s)
    event = [ip, "Add " + ",".join([str(x["_id"]) for x in samples])]
    db.runs.find_one_and_update({"name": name}, {
        "$push": {
            "metadata.modified_by": event,
        },
        "$set" : {"samples": run_samples}
    })


def remove_samples_from_virtual_run(name, ip, sample_ids, connection_name = "default"):
    """
    Adds samples to virtual run, check that samples don't exist in the run before.
    """
    connection = get_connection(connection_name)
    db = connection.get_database()
    run = db.runs.find_one({"name": name, "type": "virtual"})
    if run is None:
        raise ValueError("Virtual run not found")
    run_new_samples = [s for s in run["samples"]
                       if str(s["_id"]) not in sample_ids]
    if len(run_new_samples) == 0:
        run_new_type = "virtual-deleted"
    else:
        run_new_type = run["type"]

    event = [ip, "Remove " + ",".join(sample_ids)]

    db.runs.find_one_and_update({"name": name}, {
        "$push": {
            "metadata.modified_by": event,
        },
        "$set" : {
            "samples": run_new_samples,
            "type"   : run_new_type
        }
    })
