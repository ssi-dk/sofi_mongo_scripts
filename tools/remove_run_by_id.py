from os import getenv
from sys import exit
import argparse

import bifrostapi
from bifrostapi import sample_components

# Call this once before making any other calls.
bifrostapi.add_URI(getenv('MONGO_CONNECTION'))

parser = argparse.ArgumentParser(
    description='Script for removing a run and related objects from MongoDB')
parser.add_argument('run_id', type=str, help='The MongoDB _id field of the run object')
parser.add_argument('--fake', default=False, action='store_true', help='Do not really delete anything.')
args = parser.parse_args()

run = bifrostapi.runs.get_run_by_id(args.run_id)
if run is None:
    print(f"ERROR: no run exists with id {args.run_id}")
    exit(1)
print(f"This will remove a run document with name {run['name']} and related documents in samples and sample_components.")
answer = input("Continue (y/n)? ")
if not answer in ['Y', 'y']:
    exit()

for run_sample in run['samples']:
    sample = bifrostapi.samples.get_sample_by_id(run_sample['_id'])
    if sample is None:
        print(f"Consistency warning: a sample that is referenced in the run does not exist in samples collection:")
        print(run_sample)
    else:
        if run_sample['name'] != sample['name']:
            print(f"Consistency warning: name consistency check failed for sample id {run_sample['_id']}.")
            print(f"Run sample name is {run_sample['name']}")
            print(f"Sample name is {sample['name']}")
    
        component_names = [component['name'] for component in sample['components']]
        print ("Sample [Components]:")
        print(sample['name'], component_names)
        sample_components = list(bifrostapi.sample_components.find_sample_component_ids_by_sample_id(sample['_id']))
        sample_component_object_ids = [sc['_id'] for sc in sample_components]
        for oid in sample_component_object_ids:
            # Delete sample_component document
            if not args.fake:
                bifrostapi.sample_components.delete_sample_component_by_id(oid)
        print(f"Deleted {len(sample_components)} sample_component documents (unless fake)")
        # Delete sample document
        if not args.fake:
            bifrostapi.samples.delete_sample_by_id(run_sample['_id'])
        print(f"Deleted sample document with id {run_sample['_id']} (unless fake)")

# Delete run document
if not args.fake:
    bifrostapi.runs.delete_run_by_id(run['_id'])
print(f"Deleted run document with id {run['_id']} (unless fake)")
