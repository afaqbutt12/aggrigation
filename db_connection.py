import logging
import sys
import os
from pymongo import MongoClient


_connection = None

def connect_to_database():
    try:
        global _connection
        MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017/")
        MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "ensogov")

        if not _connection:
            _connection = MongoClient(MONGODB_URL, serverSelectionTimeoutMS=5000, directConnection=True)
        return _connection[MONGODB_DB_NAME]

    except Exception as e:
        print("Failed to connect to the database: %s", str(e))
        sys.exit(1)
