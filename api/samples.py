import re
import pymongo
from bson.objectid import ObjectId

from .utils import get_connection, FLD


def get_all_samples(connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    pipeline = [{'$project': {'_id'      : '$_id',
                              'name'     : '$name',
                              'read1_md5': '$categories.paired_reads.summary.read1_md5'}}]
    return list(db.samples.aggregate(pipeline))


def get_allele_profiles(sample_id_list: list = None, schema_name: str = 'enterobase_senterica_cgmlst', connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
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


def get_samples(sample_id_list, projection = None, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    if projection is None:
        projection = {}
    return list(db.samples.find({"_id": {"$in": sample_id_list}}, projection))


def get_sample(sample_id, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    return db.samples.find_one({"_id": sample_id})


def save_sample(data_dict, upsert: bool = False, connection_name = "default"):
    """COPIED FROM BIFROSTLIB Insert sample dict into mongodb.
    Return the dict with an _id element"""
    connection = get_connection(connection_name)
    db = connection.get_database()
    samples_db = db.samples  # Collection name is samples
    if "_id" in data_dict:
        data_dict = samples_db.find_one_and_update(
            filter = {"_id": data_dict["_id"]},
            update = {"$set": data_dict},
            # return new doc if one is upserted
            return_document = pymongo.ReturnDocument.AFTER,
            upsert = upsert  # insert the document if it does not exist, HB: to copy from one to another DB, upsert has to be True
        )
    else:
        data_dict = samples_db.find_one_and_update(
            filter = data_dict,
            update = {"$setOnInsert": data_dict},
            # return new doc if one is upserted
            return_document = pymongo.ReturnDocument.AFTER,
            upsert = True  # insert the document if it does not exist
        )
    return data_dict



def get_sample_by_id(db, sample_id):
    return db.samples.find_one({"_id": ObjectId(sample_id)})


def delete_sample_by_id(sample_id, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    return db.samples.delete_one({"_id": ObjectId(sample_id)})


def get_sample_runs(sample_ids, connection_name = "default"):
    """
    Returns runs that contain a given sample
    """
    connection = get_connection(connection_name)
    db = connection.get_database()
    return list(db.runs.find({"samples": {"$elemMatch": {"_id": {"$in": sample_ids}}}}))


def get_read_paths(sample_ids, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    return list(db.samples.find({"_id": {"$in": list(map(lambda x: ObjectId(x), sample_ids))}},
                                {"reads": 1, "name": 1}))


def get_assemblies_paths(sample_ids, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    return list(db.sample_components.find({
        "sample._id"    : {"$in": list(map(lambda x: ObjectId(x), sample_ids))},
        "component.name": "assemblatron"
    }, {"path": 1, "sample": 1}))


def get_sample_QC_status(last_runs, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
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


def _filter(run_names = None,
            species = None, species_source = "species", group = None,
            qc_list = None, date_range = None, samples = None, pagination = None,
            sample_names = None,
            projection = None,
            connection_name = "default"):
    if species_source == "provided":
        spe_field = FLD["provided_species"]
    elif species_source == "detected":
        spe_field = FLD["detected_species"]
    else:
        spe_field = FLD["species"]
    connection = get_connection(connection_name)
    db = connection.get_database()
    query = []
    sample_set = set()
    if sample_names is not None and len(sample_names) != 0:
        sample_names_query = []
        for s_n in sample_names:
            if s_n.startswith("/") and s_n.endswith("/"):
                sample_names_query.append(re.compile(s_n[1:-1]))
            else:
                sample_names_query.append(s_n)
        query.append({"name": {"$in": sample_names_query}})
    if samples is not None and len(samples) != 0:
        sample_set = {ObjectId(id) for id in samples}
        query.append({"_id": {"$in": list(sample_set)}})
    if run_names is not None and len(run_names) != 0:
        runs = list(db.runs.find(
            {"name": {"$in": run_names}},
            {
                "_id"        : 0,
                "samples._id": 1
            }
        ))
        if runs is None:
            run_sample_set = set()
        else:
            run_sample_set = {s["_id"] for run in runs for s in run['samples']}

        if len(sample_set):
            inter = run_sample_set.intersect(sample_set)
            query.append({"_id": {"$in": list(inter)}})
        else:
            query.append({"_id": {"$in": list(run_sample_set)}})
    if date_range is not None and len(date_range) == 2:
        date_range_query = {}
        if date_range[0] is not None:
            date_range_query["$gte"] = date_range[0]
        if date_range[1] is not None:
            date_range_query["$lt"] = date_range[1]
        query.append({FLD["date_sequenced"]: date_range_query})
    if species is not None and len(species) != 0:

        if "Not classified" in species:
            query.append({"$or":
                [
                    {spe_field: None},
                    {spe_field: {"$in": species}},
                    {spe_field: {"$exists": False}}
                ]
            })
        else:
            query.append({spe_field: {"$in": species}})
    if group is not None and len(group) != 0:
        if "Not defined" in group:
            query.append({"$or":
                [
                    {FLD["group"]: None},
                    {FLD["group"]: {"$in": group}},
                    {FLD["group"]: {
                        "$exists": False}}
                ]
            })
        else:
            query.append(
                {FLD["group"]: {"$in": group}})

    if pagination is not None:
        p_limit = pagination['page_size']
        p_skip = pagination['page_size'] * pagination['current_page']
    else:
        p_limit = 1000
        p_skip = 0

    qc_query = filter_qc(qc_list)

    if len(query) == 0:
        if qc_query is None:
            match_query = {}
        else:
            match_query = qc_query["$match"]
    else:
        if qc_query is None:
            match_query = {"$and": query}
        else:
            match_query = {"$and": query + qc_query["$match"]["$and"]}
    query_result = list(db.samples.find(
        match_query, projection).sort([('name', pymongo.ASCENDING)]).skip(p_skip).limit(p_limit))

    return query_result


def get_group_list(run_name = None, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
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
