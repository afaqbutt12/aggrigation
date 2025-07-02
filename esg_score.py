import sys
from json.decoder import JSONDecodeError
from bson.objectid import ObjectId
import os
from dotenv import load_dotenv
import requests
import logging
import pymongo
import os
from dotenv import load_dotenv
from json.decoder import JSONDecodeError
# from db_connection import connect_to_database
from quaterly_data import process_quarterly_data
from bson.objectid import ObjectId  # Import ObjectId for working with MongoDB Object IDs
from sarima import run_sarima
from datetime import datetime
from calendar import month_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("pymongo").setLevel(logging.ERROR)

def esg_score():
    try:

        # client = connect_to_database()
        db_name = os.getenv("MONGODB_DB_NAME")
        # db = client[db_name]
        cdata_collection = db_name["cdata"]
        # cdata_yearly_collection = db["cdata_yearly"]

        query = {"company_code": str(company_code)}
        if year:
            query["type_year"] = year
        query["type"] = "actual"
        data = list(cdata_collection.find())

        print("Query", data)


    except Exception as e:
        logger.error("An error occurred while processing yearly data: %s", str(e))

esg_score()