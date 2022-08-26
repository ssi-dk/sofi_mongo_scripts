from os import getenv
import api

# Call this once before making any other calls.
api.add_URI(getenv('MONGO_CONNECTION'))

run_name = input("Run name: ")
print(api.runs.check_run_name(run_name))