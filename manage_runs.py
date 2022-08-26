from sys import exit
import argparse
import re

import api
from api import db

def delete_run(run):
    # Delete sample and sample_component documents
    for run_sample in run['samples']:
        sample = api.samples.get_sample_by_id(db, run_sample['_id'])
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
            sample_components = list(api.sample_components.find_sample_component_ids_by_sample_id(db, sample['_id']))
            sample_component_object_ids = [sc['_id'] for sc in sample_components]
            for oid in sample_component_object_ids:
                # Delete sample_component document
                if not args.fake:
                    api.sample_components.delete_sample_component_by_id(db, oid)
            print(f"Deleted {len(sample_components)} sample_component documents (unless fake)")
            # Delete sample document
            if not args.fake:
                api.samples.delete_sample_by_id(db, run_sample['_id'])
            print(f"Deleted sample document with id {run_sample['_id']} (unless fake)")

    # Delete run document
    if not args.fake:
        api.runs.delete_run_by_id(db, run['_id'])
    print(f"Deleted run document with id {run['_id']} (unless fake)")

parser = argparse.ArgumentParser(
    description='Find and optionally delete a run and related documents from MongoDB.')
parser.add_argument('inst', type=str, help="Institution, either 'ssi' or 'fvst'")
parser.add_argument('part', type=str, help="Partial run name search for")
parser.add_argument('-d', '--delete', action='store_true', help="Interactively delete documents")
parser.add_argument('-f', '--fake', action='store_true', help="Don't really delete anything (only for testing script)")
args = parser.parse_args()

if args.inst not in ['ssi', 'fvst']:
    print("ERROR: inst must be either 'ssi' or 'fvst'.")
    exit(1)

prefix: str = '.*N_WGS_' if args.inst == 'ssi' else '[Rr][Uu][Nn]'
regex = re.compile(prefix + args.part + '.*')
number_of_runs = db.runs.count_documents({'name':  regex})
print(f"{number_of_runs} matching run(s) found.")
runs = db.runs.find({'name':  regex})
for run in runs:
    print(f"_id: {run['_id']}, name: {run['name']}")
    if args.delete:
        really_delete = input("Delete this run and related documents (y/N)? ")
        if really_delete == 'y':
            delete_run(run)
