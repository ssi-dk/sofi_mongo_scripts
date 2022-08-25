from os import getenv
from sys import exit
import argparse

from pymongo import MongoClient

mongo = MongoClient(getenv('MONGO_CONNECTION'))

parser = argparse.ArgumentParser(
    description='Find and optionally delete a run and related documents from MongoDB.')
parser.add_argument('inst', type=str, help="Institution, either 'ssi' or 'fvst'")
parser.add_argument('name', type=str, help="Partial run name search for")
parser.add_argument('-d', type=str, help="Interactively delete documents")
args = parser.parse_args()

if args.inst not in ['ssi', 'fvst']:
    print("ERROR: inst must be either 'ssi' or 'fvst'.")
    exit(1)