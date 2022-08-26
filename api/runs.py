import pymongo
from bson.objectid import ObjectId


def check_run_name(db, name):
    # Fastest.
    run = db.runs.find({"name": name}).explain().get('executionStats', None).get('nReturned', 0)
    return run != 0


def get_run_list(db, run_type = None):
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


def get_last_runs(db, run, n, runtype):

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


def get_run(db, run_name):
    # Return only one run or None.
    return db.runs.find_one({"name": run_name})


def get_runs(db, run_name):
    # Return a list of runs or None.
    return db.runs.find({"name": run_name})


def get_run_by_id(db, run_id):
    return db.runs.find_one({"_id": ObjectId(run_id)})


def delete_run_by_id(db, run_id):
    return db.runs.delete_one({"_id": ObjectId(run_id)})


def get_comment(db, run_id):
    return db.runs.find_one(
        {"_id": run_id}, {"Comments": 1})


def set_comment(db, run_id, comment):
    ret = db.runs.find_one_and_update(
        {"_id": run_id}, {"$set": {"Comments": comment}})
    if ret is not None:
        return 1
    else:
        return 0
