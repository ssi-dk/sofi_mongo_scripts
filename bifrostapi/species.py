from .utils import get_connection, FLD


def get_species_list(species_source, run_name = None, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database()
    if species_source == "provided":
        spe_field = FLD["provided_species"]
    else:
        spe_field = FLD["detected_species"]
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
        species = list(db.samples.aggregate([
            {
                "$match": {
                    "_id": {"$in": sample_ids}
                }
            },
            {
                "$group": {
                    "_id"  : "$" + spe_field,
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]))
    else:
        species = list(db.samples.aggregate([
            {
                "$group": {
                    "_id"  : "$" + spe_field,
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]))
    return species


def get_species_QC_values(ncbi_species, connection_name = "default"):
    connection = get_connection(connection_name)
    db = connection.get_database('bifrost_species')
    species = db.species.find_one({"ncbi_species": ncbi_species}, {
        "min_length": 1, "max_length": 1})
    if species is not None:
        return species
    species = db.species.find_one({"organism": ncbi_species}, {
        "min_length": 1, "max_length": 1})
    if species is not None:
        return species
    species = db.species.find_one({"group": ncbi_species}, {
        "min_length": 1, "max_length": 1})
    if species is not None:
        return species
    species = db.species.find_one({"organism": "default"}, {
        "min_length": 1, "max_length": 1})
    return species
