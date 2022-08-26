from os import getenv
from sys import exit

from pymongo import MongoClient

connection_string = getenv('BIFROST_DB_KEY')
if not connection_string:
    print("ERROR: envvar BIFROST_DB_KEY not set.")
    exit(1)
connection = MongoClient(connection_string)
db = connection.get_database()
