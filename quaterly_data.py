import sys
import pymongo
import requests
import logging
from json.decoder import JSONDecodeError
from bson.objectid import ObjectId
import os
from dotenv import load_dotenv

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
MONGODB_URL = "mongodb://localhost:27017/"
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")

def connect_to_database():  
    return pymongo.MongoClient(MONGODB_URL)[MONGODB_DB_NAME]

# Use connect_to_database() function to establish database connection
db_connection = connect_to_database()

def fetch_company_data(company_code):
    try:
        base_url = os.getenv("COMPANY_DATA_URL")
        url = f"{base_url}/company/data/{company_code}"
        response = requests.get(url)
        logger.debug("Response status code: %d", response.status_code)
        
        if response.status_code == 200:
            data = response.json()
            start_month = data["data"]["company"]["month"]
            logger.debug("Start month retrieved: %s", start_month)
            reporting_frequency = data["data"]["company"]["reporting_frequency"]
            logger.info("Start month: %s", start_month)
            logger.info("Reporting frequency: %s", reporting_frequency)
            return start_month, reporting_frequency.split(',')  # Return tuple
        else:
            logger.error("Failed to fetch data for company %s. Error: %s", company_code, response.text)
            return None, None
    except JSONDecodeError as e:
        logger.error("Failed to parse JSON response: %s", str(e))
        return None, None
    except Exception as e:
        logger.exception("Exception occurred while fetching company data for %s: %s", company_code, str(e))
        return None, None

def connect_to_database():
    return pymongo.MongoClient(MONGODB_URL)

# def get_internal_code_ids(company_code):
#     client = connect_to_database()
#     db = client[MONGODB_DB_NAME]
#     cdata_collection = db["cdata"]
#     data = cdata_collection.find({"company_code": str(company_code), "type": "actual"}).distinct("internal_code_id")
#     return data
def get_internal_code_ids(company_code):
    # client = connect_to_database()
    # db = client["ensogov"]
    # cdata_collection = db["cdata"]
    client = connect_to_database()
    db_name = os.getenv("MONGODB_DB_NAME")
    db = client[db_name]
    cdata_collection = db["cdata"]
    data = cdata_collection.find({"company_code": str(company_code), "type": "actual"}).distinct("internal_code_id")
    return data
def get_code_data(internal_code_id):
    client = connect_to_database()
    db = client[MONGODB_DB_NAME]
    codes_collection = db["codes"]
    
    try:
        internal_code_id = ObjectId(internal_code_id)  # Convert internal_code_id to ObjectId
        code_data = codes_collection.find_one({"_id": internal_code_id})
        
        if code_data:
            return code_data.get("name", ""), code_data.get("code", "")
        else:
            return None, None
    except Exception as e:
        logger.error("Error converting internal_code_id to ObjectId: %s", str(e))
        return None, None

def process_quarterly_data(company_code, year=None):
    print("inside process_monthly_data",company_code)
    # Fetch company data
    start_month, reporting_frequency = fetch_company_data(company_code)
    
    if not start_month:
        logger.error("Failed to fetch start month for company %s.", company_code)
        return

    # Connect to the database
    client = connect_to_database()
    db = client[MONGODB_DB_NAME]
    cdata_collection = db["cdata"]
    cdata_quarter_collection = db["cdata_quarter"]

    # Get internal code IDs
    internal_code_ids = get_internal_code_ids(company_code)
    print("internal_code_ids",internal_code_ids);
    if not internal_code_ids:
        logger.error("No internal code IDs found for company %s.", company_code)
        return

    # Get code data for each internal code ID
    for internal_code_id in internal_code_ids:
        # Fetch code name and code for the specific internal_code_id
        code_name, code = get_code_data(internal_code_id)
        if code_name is None or code is None:
            logger.error("No code data found for internal_code_id '%s'.", internal_code_id)
            continue

        print("myYear :", year)
        # Query data from the database
        # data = list(cdata_collection.find({"company_code": str(company_code), "type_year": year, "type": "actual"}))
        # print('llllll :', data)
        # # data = cdata_collection.find({"company_code": str(company_code), "type": "actual"}).distinct("internal_code_id")
        # data = [entry for entry in data if entry["internal_code_id"] == internal_code_id and MONTHS.index(entry["month"]) >= MONTHS.index(start_month)]
        # data.sort(key=lambda x: MONTHS.index(x["month"]))
        # print("data :", data)
        # years = {entry["type_year"] for entry in data}
        #  years = list({entry["type_year"] for entry in data})[:3]
        query = {"company_code": str(company_code)}
        # print("myquery :", query)
        data = list(cdata_collection.find(query))
        data = [entry for entry in data if entry["type"] == "actual" and MONTHS.index(entry["month"]) >= MONTHS.index(start_month)]
        data.sort(key=lambda x: (x["type_year"], MONTHS.index(x["month"])))

        # years_to_process = list(set(entry["type_year"] for entry in data))[:3]
        years_to_process = sorted(set(entry["type_year"] for entry in data))[:3]
    for year in years_to_process:
        for internal_code_data in data:
            
            quarter_month_ranges = {}
            current_quarter = None
            month_index = MONTHS.index(start_month)
            
            while month_index < len(MONTHS):
                if current_quarter is None:
                    current_quarter = f"Q{len(quarter_month_ranges) + 1}"
                    quarter_month_ranges[current_quarter] = {"months": []}
                next_month_str = MONTHS[month_index]
                if next_month_str in [entry["month"] for entry in data]:
                    quarter_month_ranges[current_quarter]["months"].append(next_month_str)
                if len(quarter_month_ranges[current_quarter]["months"]) == 3 or month_index == len(MONTHS):
                    current_quarter = None
                month_index += 1
            
            for quarter, data_range in quarter_month_ranges.items():
                month_range = data_range["months"]
                # Check if data for this quarter and internal code ID already exists
                existing_data = cdata_quarter_collection.find_one({
                    "company_code": company_code,
                    "type_year": year,
                    "internal_code_id": internal_code_id,
                    "quarter": quarter,
                })
                # If data already exists, skip insertion
                if existing_data:
                    logger.info("Data for quarter %s and internal code ID %s already exists. Skipping insertion.", quarter, internal_code_id)
                    continue
                
                # Insert new data if it doesn't already exist
                total_value = sum(int(entry.get("value", 0)) for entry in data if entry["month"] in month_range)
                total_qty = sum(int(entry.get("qty", 0)) for entry in data if entry["month"] in month_range)
                first_entry = next((entry for entry in data if entry["month"] in month_range), None)
                
                if first_entry:
                    currency = first_entry.get("currency", "")
                    unit = first_entry.get("unit", "")
                    month_range_str = " - ".join(month_range)
                    
                    # Save data to cdata_quarter_collection
                    cdata_quarter_collection.insert_one({
                        "company_code": company_code,
                        "type_year": year,
                        "internal_code_id": internal_code_id,
                        "code_name": code_name,
                        "code": code,
                        "quarter": quarter,
                        "month": month_range_str,
                        "total_value": total_value,
                        "total_qty": total_qty,
                        "currency": currency,
                        "unit": unit,
                    })
# print("Data successfully processed and saved.")

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        logger.error("Usage: python data_analysis.py <company_code> <year>")
        sys.exit(1)
    
    company_code = sys.argv[1]
    year = sys.argv[2]  # Get year as string
    process_quarterly_data(company_code, year)
