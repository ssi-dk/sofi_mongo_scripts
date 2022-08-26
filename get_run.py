from os import getenv
import api

# Call this once before making any other calls.
api.add_URI(getenv('MONGO_CONNECTION'))

run_name = input("Run name: ")

# Return the first run with the given run name or None.
print(api.runs.get_run(run_name))