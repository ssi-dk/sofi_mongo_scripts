from os import getenv
import bifrostapi

# Call this once before making any other calls.
bifrostapi.add_URI(getenv('MONGO_CONNECTION'))

run_name = input("Run name: ")

# Return the first run with the given run name or None.
print(bifrostapi.runs.get_run(run_name))