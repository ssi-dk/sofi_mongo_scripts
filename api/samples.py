import re
import pymongo
from bson.objectid import ObjectId


def get_all_samples(db):
    pipeline = [{'$project': {'_id'      : '$_id',
                              'name'     : '$name',
                              'read1_md5': '$categories.paired_reads.summary.read1_md5'}}]
    return list(db.samples.aggregate(pipeline))


def get_allele_profiles(db, sample_id_list: list = None, schema_name: str = 'enterobase_senterica_cgmlst'):
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


def get_samples(db, sample_id_list, projection = None):
    if projection is None:
        projection = {}
    return list(db.samples.find({"_id": {"$in": sample_id_list}}, projection))


def get_sample(db, sample_id):
    return db.samples.find_one({"_id": sample_id})


def get_sample_by_id(db, sample_id):
    return db.samples.find_one({"_id": ObjectId(sample_id)})


def delete_sample_by_id(db, sample_id):
    return db.samples.delete_one({"_id": ObjectId(sample_id)})


def get_sample_runs(db, sample_ids):
    """
    Returns runs that contain a given sample
    """
    return list(db.runs.find({"samples": {"$elemMatch": {"_id": {"$in": sample_ids}}}}))


def get_read_paths(db, sample_ids):
    return list(db.samples.find({"_id": {"$in": list(map(lambda x: ObjectId(x), sample_ids))}},
                                {"reads": 1, "name": 1}))


def get_assemblies_paths(db, sample_ids):
    return list(db.sample_components.find({
        "sample._id"    : {"$in": list(map(lambda x: ObjectId(x), sample_ids))},
        "component.name": "assemblatron"
    }, {"path": 1, "sample": 1}))


def get_sample_QC_status(db, last_runs):
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

