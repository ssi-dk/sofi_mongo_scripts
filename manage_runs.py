from sys import exit
import argparse
import re

import api
from api import db

def bifrost_deletion_loop(run):
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
        print(f"Deleted run document with id {run['_id']} and name {run['name']}")
    else:
        print(f"Running script with fake option - did not REALLY delete run document with id {run['_id']} and name {run['name']}")


def sofi_deletion_loop(run_id):
    number_of_sap_analysis_results = db.sap_analysis_results.count_documents({'run_id': run_id})
    print()
    if number_of_sap_analysis_results == 0:
        print("No related SOFI analysis result(s) found.")
    else:
        print(f"{number_of_sap_analysis_results} related SOFI analysis result(s) found:")
        sap_analysis_results = db.sap_analysis_results.find({'run_id': run_id})
        sap_ids = list()
        for a in sap_analysis_results:
            print(f"_id: {a['_id']}, run_id: {a['run_id']}")
            sap_ids.append(a['_id'])
        if args.delete:
            confirm = input("Should these be deleted (y/N)? ")
            if confirm == 'y' and not args.fake:
                for id in sap_ids:
                    db.sap_analysis_results.delete_one({'_id': id})
                print(f"Deleted SOFI analysis results for {args.inst} run {args.part}.")


INST_OPTIONS = ['ssi', 'fvst', 'none']

parser = argparse.ArgumentParser(
    description='Find and optionally delete a run and related documents from MongoDB.')
parser.add_argument('inst', type=str, help=f"Institution, must be in {INST_OPTIONS}")
parser.add_argument('part', type=str, help="Partial run name search for")
parser.add_argument('-d', '--delete', action='store_true', help="Interactively delete documents")
parser.add_argument('-f', '--fake', action='store_true', help="Don't really delete anything (only for testing script)")
args = parser.parse_args()

if args.inst not in INST_OPTIONS:
    print(f"ERROR: inst must be in {INST_OPTIONS}.")
    exit(1)

prefix:str = ''
if args.inst == 'ssi':
    prefix = '.*N_WGS_'
if args.inst == 'fvst':
    prefix = '[Rr][Uu][Nn]'
regex = re.compile(prefix + args.part + '.*')
number_of_runs = db.runs.count_documents({'name':  regex})
print(f"{number_of_runs} matching run(s) found:")
runs = db.runs.find({'name':  regex})
for run in runs:
    print(f"_id: {run['_id']}, name: {run['name']}")
    if args.delete:
        confirm = input("Delete this run and related Bifrost MongoDB documents (y/N)? ")
        if confirm == 'y':
            bifrost_deletion_loop(run)
        sofi_deletion_loop(run['name'])
