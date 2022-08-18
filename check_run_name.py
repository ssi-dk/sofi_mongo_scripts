from os import getenv
import bifrostapi

# Call this once before making any other calls.
bifrostapi.add_URI(getenv('MONGO_CONNECTION'))

run_name = input("Run name: ")
print(bifrostapi.runs.check_run_name(run_name))