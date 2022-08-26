from os import getenv

import api

# Call this once before making any other calls.
api.add_URI(getenv('MONGO_CONNECTION'))

run_name = input("Run name: ")
runs = api.runs.get_runs(run_name)
run_count = 0

try:
    while True:
        print()
        run = next(runs)
        print(f"Run id: {run['_id']}")
        print(f"Created at: {run['metadata']['created_at']}")
        print("Samples:")
        for sample in run['samples']:
            print(sample)
        run_count += 1
except StopIteration:
    print(f"Found {run_count} runs.")
