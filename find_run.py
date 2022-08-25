from sys import exit
import argparse
import re

from init_mongo_connection import db

def delete_run(run):
    really_delete = input("Delete this run and related documents (y/N)? ")
    if really_delete == 'y':
        print("Use some earlier code here to delete the run and related documents.")

parser = argparse.ArgumentParser(
    description='Find and optionally delete a run and related documents from MongoDB.')
parser.add_argument('inst', type=str, help="Institution, either 'ssi' or 'fvst'")
parser.add_argument('part', type=str, help="Partial run name search for")
parser.add_argument('-d', '--delete', action='store_true', help="Interactively delete documents")
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
        delete_run(run)
