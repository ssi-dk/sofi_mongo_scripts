import re
import pymongo
from bson.objectid import ObjectId


def get_all_samples(db, connection_name = "default"):
    pipeline = [{'$project': {'_id'      : '$_id',
                              'name'     : '$name',
                              'read1_md5': '$categories.paired_reads.summary.read1_md5'}}]
    return list(db.samples.aggregate(pipeline))


def get_allele_profiles(db, sample_id_list: list = None, schema_name: str = 'enterobase_senterica_cgmlst', connection_name = "default"):
    pipeline = [
        {'$match': {
            "$and": [
                {'categories.cgmlst.summary.allele_qc': 'PASS'},
                {'categories.cgmlst.report.chewiesnake.run_metadata.database_information.scheme_name': schema_name}
            ]}
        },
        {'$project': {'_id'           : '$_id',
                      'name'          : '$name',
                      'display_name'  : '$display_name',
                      'hashid'        : '$categories.cgmlst.summary.hashid',
                      'allele_profile': '$categories.cgmlst.report.chewiesnake.allele_profile'}
         }
    ]
    if sample_id_list is not None:
        pipeline[0]['$match']['$and'].insert(0, {'_id': {'$in': sample_id_list}})
    return list(db.samples.aggregate(pipeline))


def get_samples(db, sample_id_list, projection = None, connection_name = "default"):
    if projection is None:
        projection = {}
    return list(db.samples.find({"_id": {"$in": sample_id_list}}, projection))


def get_sample(db, sample_id, connection_name = "default"):
    return db.samples.find_one({"_id": sample_id})


def get_sample_by_id(db, sample_id):
    return db.samples.find_one({"_id": ObjectId(sample_id)})


def delete_sample_by_id(db, sample_id, connection_name = "default"):
    return db.samples.delete_one({"_id": ObjectId(sample_id)})


def get_sample_runs(db, sample_ids, connection_name = "default"):
    """
    Returns runs that contain a given sample
    """
    return list(db.runs.find({"samples": {"$elemMatch": {"_id": {"$in": sample_ids}}}}))


def get_read_paths(db, sample_ids, connection_name = "default"):
    return list(db.samples.find({"_id": {"$in": list(map(lambda x: ObjectId(x), sample_ids))}},
                                {"reads": 1, "name": 1}))


def get_assemblies_paths(db, sample_ids, connection_name = "default"):
    return list(db.sample_components.find({
        "sample._id"    : {"$in": list(map(lambda x: ObjectId(x), sample_ids))},
        "component.name": "assemblatron"
    }, {"path": 1, "sample": 1}))


def get_sample_QC_status(db, last_runs, connection_name = "default"):
    samples = [sample
               for run in last_runs
               for sample in run["samples"]]

    samples_full = db.samples.find({"_id": {"$in": list(map(lambda x: x["_id"], samples))}},
                                   {"properties.stamper"  : 1,
                                    "properties.datafiles": 1,
                                    "name"                : 1})
    samples_by_ids = {str(s["_id"]): s for s in samples_full}

    samples_runs_qc = {}
    for sample in samples:
        sample_dict = {}
        if str(sample["_id"]) not in samples_by_ids:
            continue
        name = samples_by_ids[str(sample["_id"])]["name"]
        for run in last_runs:
            for run_sample in run["samples"]:
                if name == samples_by_ids[str(run_sample["_id"])]["name"]:
                    sample_db = samples_by_ids.get(
                        str(run_sample["_id"]), None)
                    if sample_db is not None:
                        qc_val = sample_db.get("properties", {}).get("stamper", {}).get(
                            "summary", {}).get("stamp", {}).get("value", "N/A")
                        reads = sample_db.get("properties", {}).get("datafiles", {}).get(
                            "summary", {}).get("paired_reads", [])

                        if qc_val == "N/A" and not reads:
                            qc_val = "CF(LF)"
                        expert_check = False
                        # if "supplying_lab_check" in stamps and "value" in stamps["supplying_lab_check"]:
                        #     qc_val = stamps["supplying_lab_check"]["value"]
                        #     expert_check = True

                        if qc_val == "supplying lab":
                            qc_val = "SL"
                        elif (qc_val == "core facility" or
                              qc_val == "resequence"):
                            qc_val = "CF"
                        elif qc_val == "OK" or qc_val == "accepted":
                            qc_val = "OK"

                        if expert_check:
                            qc_val += "*"
                        sample_dict[run["name"]] = qc_val
        samples_runs_qc[name] = sample_dict
    return samples_runs_qc


def filter_qc(qc_list):
    if qc_list is None or len(qc_list) == 0:
        return None
    qc_query = []
    for elem in qc_list:
        if elem == "Not checked":
            qc_query.append({"$and": [
                {"properties.datafiles.summary.paired_reads": {"$exists": True}},
                {"properties.stamper.summary.stamp.value": {"$exists": False}}
            ]})
        elif elem == "core facility":
            qc_query.append({"$or": [
                {"properties.datafiles.summary.paired_reads": {"$exists": False}},
                {"properties.stamper.summary.stamp.value": "core facility"}
            ]
            })
        else:
            qc_query.append({"properties.stamper.summary.stamp.value": elem})
    return {"$match": {"$and": qc_query}}


# Need to clean this two functions
def filter(species = None, species_source = None, group = None,
           qc_list = None, date_range = None, run_names = None, sample_ids = None,
           sample_names = None,
           pagination = None,
           projection = None,
           connection_name = "default"):
    if sample_ids is None:
        query_result = _filter(
            run_names = run_names, species = species,
            species_source = species_source, group = group,
            qc_list = qc_list,
            date_range = date_range,
            sample_names = sample_names,
            pagination = pagination,
            projection = projection,
            connection_name = connection_name)
    else:
        # sample ids prevent other filters from working.
        query_result = _filter(
            samples = sample_ids, pagination = pagination,
            projection = projection,
            connection_name = connection_name)
    return query_result

def get_group_list(db, run_name = None, connection_name = "default"):
    if run_name is not None:
        run = db.runs.find_one(
            {"name": run_name},
            {
                "_id"        : 0,
                "samples._id": 1
            }
        )
        if run is None:
            run_samples = []
        else:
            run_samples = run["samples"]
        sample_ids = [s["_id"] for s in run_samples]
        groups = list(db.samples.aggregate([
            {
                "$match": {
                    "_id": {"$in": sample_ids},
                }
            },
            {
                "$group": {
                    "_id"  : "${group}".format(**FLD),
                    "count": {"$sum": 1}
                }
            }
        ]))
    else:
        groups = list(db.samples.aggregate([
            {
                "$group": {
                    "_id"  : "${group}".format(**FLD),
                    "count": {"$sum": 1}
                }
            }
        ]))

    return groups
