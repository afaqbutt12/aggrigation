import requests
import logging
import sys
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


# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# Define MongoDB URL from environment variables
# MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017/")

def fetch_company_data(company_code):
    try:
        base_url = os.getenv("COMPANY_DATA_URL")
        url = f"{base_url}/company/data/{company_code}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            if data:
                start_month = data["data"]["company"]["month"]
                reporting_frequency = ""
                if data["data"]["company"]["reporting_frequency"]:
                    data = data["data"]["company"]["reporting_frequency"]
                    reporting_frequency = data.split(',')
                return start_month, reporting_frequency  # Return tuple
            else:
                return ['January']
        else:
            logger.error("Failed to fetch data for company %s. Error: %s", company_code, response.text)
            return None, None
        
    except JSONDecodeError as e:
        logger.error("Failed to parse JSON response: %s", str(e))
        return None, None
    except Exception as e:
        logger.exception("Exception occurred while fetching company data for %s: %s", company_code, str(e))
        return None, None

def fetch_all_company():
    try:
        base_url = os.getenv("COMPANY_DATA_URL")
        
        url = f"{base_url}/company/data"
        print("Url :: ", url)
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            return data["companies"]  # Return tuple
        else:
            logger.error("Failed to fetch data for company %s. Error: %s", response.text)
            return None, None
        
    except JSONDecodeError as e:
        logger.error("Failed to parse JSON response: %s", str(e))
        return None, None
    except Exception as e:
        logger.exception("Exception occurred while fetching company data for %s: %s", str(e))
        return None, None

# def get_code_data(internal_code_id):
#     client = connect_to_database()
#     db_name = os.getenv("MONGODB_DB_NAME")
#     db = client[db_name]
#     codes_collection = db["codes"]
    
#     try:
#         internal_code_id = ObjectId(internal_code_id)  # Convert internal_code_id to ObjectId
#         code_data = codes_collection.find_one({"_id": internal_code_id})
        
#         if code_data:
#             return code_data.get("name", ""), code_data.get("code", "")
#         else:
#             return None, None
#     except Exception as e:
#         logger.error("Error converting internal_code_id to ObjectId: %s", str(e))
#         return None, None

