from os import getenv
import api

# Call this once before making any other calls.
api.add_URI(getenv('MONGO_CONNECTION'))

runs = api.runs.get_run_list()
for run in runs:
    print(run['_id'], run['name'])
