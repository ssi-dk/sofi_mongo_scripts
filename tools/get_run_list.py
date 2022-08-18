from os import getenv
import bifrostapi

# Call this once before making any other calls.
bifrostapi.add_URI(getenv('MONGO_CONNECTION'))

runs = bifrostapi.runs.get_run_list()
for run in runs:
    print(run['_id'], run['name'])
