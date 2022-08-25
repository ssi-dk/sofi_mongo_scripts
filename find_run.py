from sys import exit
import argparse
import re

from init_mongo_connection import db

parser = argparse.ArgumentParser(
    description='Find and optionally delete a run and related documents from MongoDB.')
parser.add_argument('inst', type=str, help="Institution, either 'ssi' or 'fvst'")
parser.add_argument('part', type=str, help="Partial run name search for")
parser.add_argument('-d', type=str, help="Interactively delete documents")
args = parser.parse_args()

if args.inst not in ['ssi', 'fvst']:
    print("ERROR: inst must be either 'ssi' or 'fvst'.")
    exit(1)

prefix: str = '.*N_WGS_' if args.inst == 'ssi' else '[Rr][Uu][Nn]'
regex = re.compile(prefix + args.part + '.*')
number_of_runs = db.runs.count_documents({'name':  regex})
if number_of_runs == 0:
    print("No matching runs found.")
elif number_of_runs == 1:
    print("1 matching run found:")
    run = (next(db.runs.find({'name':  regex})))
    print(f"_id: {run['_id']}, name: {run['name']}")
