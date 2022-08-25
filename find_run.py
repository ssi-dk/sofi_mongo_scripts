from os import getenv
from sys import exit
import argparse

from pymongo import MongoClient

connection_string = getenv('BIFROST_DB_KEY')
if not connection_string:
    print("ERROR: envvar BIFROST_DB_KEY not set.")
    exit(1)
connection = MongoClient(connection_string)
db = connection.get_database()

parser = argparse.ArgumentParser(
    description='Find and optionally delete a run and related documents from MongoDB.')
parser.add_argument('inst', type=str, help="Institution, either 'ssi' or 'fvst'")
parser.add_argument('name', type=str, help="Partial run name search for")
parser.add_argument('-d', type=str, help="Interactively delete documents")
args = parser.parse_args()

if args.inst not in ['ssi', 'fvst']:
    print("ERROR: inst must be either 'ssi' or 'fvst'.")
    exit(1)