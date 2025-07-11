import requests
import logging
import sys
import pymongo
import os
from dotenv import load_dotenv
from json.decoder import JSONDecodeError
from sarima import run_sarima
from datetime import datetime
from calendar import month_name
from bson import ObjectId  # Add this import



# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Define MongoDB URL from environment variables
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017/")

# Establish database connection
def connect_to_database():
    return pymongo.MongoClient(MONGODB_URL, connectTimeoutMS=30000, socketTimeoutMS=None, serverSelectionTimeoutMS=30000)

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

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

def connect_to_databaseOne():
    return pymongo.MongoClient("mongodb://localhost:27017/")

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

def get_code_data(internal_code_id):
    client = connect_to_database()
    db_name = os.getenv("MONGODB_DB_NAME")
    db = client[db_name]
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


def process_bi_annual_data(company_code, year=None, reporting_frequency=None):
    try:
        start_month, _ = fetch_company_data(company_code)
        if not start_month:
            logger.warning("Failed to fetch start month for company %s.", company_code)
            return

        client = connect_to_database()
        db_name = os.getenv("MONGODB_DB_NAME")
        db = client[db_name]
        cdata_collection = db["cdata"]
        cdata_bi_annual_collection = db["cdata_bi_annual"]

        internal_code_ids = get_internal_code_ids(company_code)
        if not internal_code_ids:
            logger.warning("No internal code IDs found for company %s.", company_code)
            return

        query = {"company_code": str(company_code)}
        data = list(cdata_collection.find(query))
        data = [entry for entry in data if entry["type"] == "actual" and MONTHS.index(entry["month"]) >= MONTHS.index(start_month)]
        data.sort(key=lambda x: (x["type_year"], MONTHS.index(x["month"])))

        # print("ddata:", data)
        # print("internal_code_idms:", internal_code_ids)
        years_to_process = sorted(set(entry["type_year"] for entry in data))[:3]

        for year_to_process in years_to_process:
            # print("year_to_processll", year_to_process)
            for internal_code_id in internal_code_ids:
                year_data = [entry for entry in data if entry["type_year"] == year_to_process and entry["internal_code_id"] == internal_code_id]

                # print("year_dataa", year_data)
                if not year_data:
                    continue

                start_month_index = MONTHS.index(start_month)
                semester1_months = MONTHS[start_month_index:start_month_index + 6]
                semester2_months = MONTHS[start_month_index + 6:]
                total_value_semester1 = sum(entry.get("value", 0) for entry in year_data if entry["month"] in semester1_months)
                total_qty_semester1 = sum(entry.get("qty", 0) for entry in year_data if entry["month"] in semester1_months)
                total_value_semester2 = sum(entry.get("value", 0) for entry in year_data if entry["month"] in semester2_months)
                total_qty_semester2 = sum(entry.get("qty", 0) for entry in year_data if entry["month"] in semester2_months)
                first_entry_semester1 = next((entry for entry in year_data if entry["month"] in semester1_months), None)
                currency_semester1 = first_entry_semester1.get("currency", "") if first_entry_semester1 else ""
                unit_semester1 = first_entry_semester1.get("unit", "") if first_entry_semester1 else ""
                first_entry_semester2 = next((entry for entry in year_data if entry["month"] in semester2_months), None)
                currency_semester2 = first_entry_semester2.get("currency", "") if first_entry_semester2 else ""
                unit_semester2 = first_entry_semester2.get("unit", "") if first_entry_semester2 else ""
                months_semester1 = [month for month in semester1_months if month in [entry["month"] for entry in year_data]]
                months_semester2 = [month for month in semester2_months if month in [entry["month"] for entry in year_data]]

                # Fetch code and name from cdata
                name, code = get_code_data(internal_code_id)

                # Insert data into cdata_bi_annual_collection
                cdata_bi_annual_collection.delete_many({
                    "company_code": company_code,
                    "type_year": year_to_process,
                    "internal_code_id": internal_code_id,
                    "semester": "semester1",
                    "months": " - ".join(months_semester1),
                    "code": code,
                    "name": name
                })
                cdata_bi_annual_collection.insert_many([
                    {
                        "company_code": company_code,
                        "type_year": year_to_process,
                        "internal_code_id": internal_code_id,
                        "semester": "semester1",
                        "months": " - ".join(months_semester1),
                        "code": code,
                        "name": name,
                        "total_value": total_value_semester1,
                        "total_qty": total_qty_semester1,
                        "currency": currency_semester1,
                        "unit": unit_semester1,
                    },
                    {
                        "company_code": company_code,
                        "type_year": year_to_process,
                        "internal_code_id": internal_code_id,
                        "semester": "semester2",
                        "code": code,
                        "name": name,
                        "months": " - ".join(months_semester2),
                        "total_value": total_value_semester2,
                        "total_qty": total_qty_semester2,
                        "currency": currency_semester2,
                        "unit": unit_semester2,
                    }
                ])
    except Exception as e:
        logger.error("An error occurred while processing bi-annual data: %s", str(e))


def process_yearly_data(company_code, year=None, reporting_frequency=None):
    try:
        if reporting_frequency and "annual" not in reporting_frequency:
            logger.info("Reporting frequency does not include annual data. Skipping...")
            return

        client = connect_to_database()
        db_name = os.getenv("MONGODB_DB_NAME")
        db = client[db_name]
        cdata_collection = db["cdata"]
        cdata_yearly_collection = db["cdata_yearly"]

        query = {"company_code": str(company_code)}
        if year:
            query["type_year"] = year
        query["type"] = "actual"
        data = list(cdata_collection.find(query))

        start_month_tuple = fetch_company_data(company_code)
        if not start_month_tuple:
            logger.warning("Failed to fetch start month or reporting frequency for company %s.", company_code)
            return

        start_month, _ = start_month_tuple

        data.sort(key=lambda x: (x["type_year"], MONTHS.index(x["month"])))

        # years_to_process = set(entry["type_year"] for entry in data)[:3] if not year else [year]
        # years_to_process = list(set(entry["type_year"] for entry in data))[:3]
        years_to_process = sorted(set(entry["type_year"] for entry in data))[:3]



        for year in years_to_process:
            year_data = [entry for entry in data if entry["type_year"] == year]
            start_month_index = MONTHS.index(start_month)
            year_data = [entry for entry in year_data if MONTHS.index(entry["month"]) >= start_month_index]

            if year_data:
                internal_code_ids = set(entry["internal_code_id"] for entry in year_data)

                for internal_code_id in internal_code_ids:
                    internal_code_data = [entry for entry in year_data if entry["internal_code_id"] == internal_code_id]
                    months_present = set(entry["month"] for entry in internal_code_data)
                    months_range = [month for month in MONTHS if month in months_present]
                    total_value_yearly = sum(entry["value"] for entry in internal_code_data)
                    total_qty_yearly = sum(entry["qty"] for entry in internal_code_data)
                    first_entry = internal_code_data[0] if internal_code_data else None
                    currency_yearly = first_entry.get("currency", "") if first_entry else ""
                    unit_yearly = first_entry.get("unit", "") if first_entry else ""

                    name, code = get_code_data(internal_code_id)
                    cdata_yearly_collection.delete_many({
                        "company_code": company_code,
                        "type_year": year,
                        "internal_code_id": internal_code_id,
                        "code": code,
                        "name": name,
                        "months": " - ".join(months_range),
                    })
                    cdata_yearly_collection.insert_one({
                        "company_code": company_code,
                        "type_year": year,
                        "internal_code_id": internal_code_id,
                        "code": code,
                        "name": name,
                        "months": " - ".join(months_range),
                        "total_value": total_value_yearly,
                        "total_qty": total_qty_yearly,
                        "currency": currency_yearly,
                        "unit": unit_yearly,
                    })
            else:
                logger.warning("No data available for the specified year and company code.")
    except Exception as e:
        logger.error("An error occurred while processing yearly data: %s", str(e))


def process_monthly_data(company_code, year=None, start_month=None, reporting_frequency=None):
    MONTHS = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]

    # Convert month names to datetime objects for comparison
    # months_datetime = [datetime.strptime(month, '%B') for month in MONTHS]

    try:
        client = connect_to_database()  # Assuming connect_to_database() is defined elsewhere
        db_name = os.getenv("MONGODB_DB_NAME")
        db = client[db_name]
        cdata_collection = db["cdata"]
        cdata_month_collection = db["cdata_month"]

        query = {"company_code": str(company_code)}
        if year:
            query["type_year"] = year
            query["type"] = "actual"

        data = list(cdata_collection.find(query))
        start_month_tuple = fetch_company_data(company_code)  # Assuming fetch_company_data() is defined elsewhere
        # print("start_month_tuple", start_month_tuple)
        if isinstance(start_month_tuple, tuple):
            start_month = start_month_tuple[0]

        data.sort(key=lambda x: (x["type_year"], MONTHS.index(x["month"])))
        years_to_process = sorted(set(entry["type_year"] for entry in data))[:3]

        sarima_array = []
        # sarima_list = []

        # Define a dictionary to map month names to their numerical values
        month_values = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
        }

        for year in years_to_process:
            year_data = [entry for entry in data if entry["type_year"] == year]
            if year == min(years_to_process):
                if start_month:
                    start_month_index = MONTHS.index(start_month)
                else:
                    start_month_index = 0  # January index
            else:
                start_month_index = 0  # January index

            # Populate sarima_array for the current year
            for month_index in range(start_month_index, 12):
                month = MONTHS[month_index]
                month_found = False
                for entry in year_data:
                    if entry["month"] == month:
                        sarima_array.append(entry.get("qty", 0))
                        month_found = True
                        break
                if not month_found:
                    sarima_array.append(0)
                    

            # Check if the current year is the minimum year
            if year == min(years_to_process) and start_month:
                start_month_index = MONTHS.index(start_month)
                year_data = [entry for entry in year_data if MONTHS.index(entry["month"]) >= start_month_index]
            elif year == min(years_to_process):
                start_month_index = MONTHS.index(start_month) if start_month in MONTHS else 2  # March index if start_month is not provided or invalid
                year_data = [entry for entry in year_data if MONTHS.index(entry["month"]) >= start_month_index]
            else:
                start_month_index = 0  # January index
            
            for entry in year_data:
                try:
                    name, code = get_code_data(entry["internal_code_id"])
                    cdata_month_collection.delete_many({
                        "company_code": company_code,
                        "type_year": year,
                        "internal_code_id": entry["internal_code_id"],
                        "month": entry["month"],
                        "code": code,
                    })
                    cdata_month_collection.insert_one({
                        "company_code": company_code,
                        "type_year": year,
                        "internal_code_id": entry["internal_code_id"],
                        "month": entry["month"],
                        "value": entry.get("value", 0),
                        "qty": entry.get("qty", 0),
                        "code": code,
                        "name": name,
                        "currency": entry.get("currency", ""),
                        "unit": entry.get("unit", ""),
                        "sarima_value": entry.get("qty", 0),
                        "is_predectead": False
                    })

                    # sarima_list.append({entry["month"]: entry.get("qty", 0)})

                    # sarima_array.append(entry.get("qty", 0))
                except Exception as e:
                    logger.error("An error occurred while preparing data for %s: %s", entry["month"], str(e))

        print("Sarima array ooooj: before", sarima_array)

        # month_index_map = {month: index for index, month in enumerate(MONTHS)}
        # # Populate arima_array dynamically
        # arima_array = [0] * len(MONTHS)
        # for month, value in zip(MONTHS, sarima_array):
        #     index = month_index_map.get(month)
        #     if index is not None:
        #         arima_array[index] = value
        # print("ARIMA Array: after", arima_array)
        sarima_predictions = run_sarima(sarima_array, predictedValue=23)

        # sarima_predictions = run_sarima(sarima_array)  # Assuming run_sarima() returns SARIMA predictions

        # Calculate next year and month
        max_year = max(years_to_process)
        last_month_of_max_year = max(((entry["month"], month_values[entry["month"]]) for entry in data if entry["type_year"] == max_year), key=lambda x: x[1])[0]
        # last_month_of_max_year_datetime = datetime.strptime(last_month_of_max_year, '%B')

        # if last_month_of_max_year_datetime.month == 12:  # Check if the month is December
        #     max_year = max_year + 1
        #     next_month = 1  # January
        # else:
        #     max_year = max_year
        #     next_month_index = (MONTHS.index(last_month_of_max_year) + 1) % 12  # Calculate the next month index
        #     next_month = MONTHS[next_month_index]  # Get the next month

        # Prepare data for insertion
        batch_data = []
        for idx, pred_value in enumerate(sarima_predictions):
            if MONTHS[month_index] == "December":  # Check if the month is December
                max_year = max_year + 1
            month_index = (MONTHS.index(last_month_of_max_year) + idx + 1) % 12  # Calculate the correct month index based on the start month and the current iteration index
            batch_data.append({
                "company_code": company_code,
                "type_year": max_year,  # Set next_year for January, otherwise use max_year
                "month": MONTHS[month_index],  # Assign the correct month based on the calculated month index
                "sarima_value": pred_value,
                "is_predectead": True
            })

        # Insert predictions into cdata_month table
        if batch_data:
            cdata_month_collection.delete_many({ "company_code": company_code, "type_year": max_year, "month": MONTHS[month_index]})
            cdata_month_collection.insert_many(batch_data)

    except Exception as e:
        logger.error("An error occurred while processing monthly data: %s", str(e))


def delete_monthly_data(company_code, reporting_month, year, internal_code_id, site_code):
    client = connect_to_database()  # Assuming connect_to_database() is defined elsewhere
    db_name = os.getenv("MONGODB_DB_NAME")
    db = client[db_name]
    
    if db is not None:
        cdata_month_collection = db["cdata_month"]
        logger.info(f"{company_code}, {reporting_month}, {year}, {internal_code_id}, {site_code}")
        
        months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]

        # Debug: Check parameter types
        print(f"Parameter types:")
        print(f"company_code: {type(company_code)} = {company_code}")
        print(f"reporting_month: {type(reporting_month)} = {reporting_month}")
        print(f"year: {type(year)} = {year}")
        print(f"internal_code_id: {type(internal_code_id)} = {internal_code_id}")
        print(f"site_code: {type(site_code)} = '{site_code}' (length: {len(str(site_code))})")
        
        # Check if site_code is empty and warn
        if not site_code or site_code.strip() == '':
            print("WARNING: site_code is empty! This might cause no documents to be found.")
        
        # Validate ObjectId
        try:
            obj_id = ObjectId(internal_code_id)
            print(f"Valid ObjectId: {obj_id}")
        except Exception as e:
            print(f"Invalid ObjectId: {e}")
            return 0

        reporting_index = months.index(reporting_month)
        start_index = months.index(reporting_month)  # Use reporting_month as start month

        # Split into two arrays - ensure they're lists, not sets
        this_year_months = months[start_index:]  # From reporting_month to December
        next_year_months = months[:reporting_index + 1]  # From January to reporting_month
        
        print(f"this_year_months: {this_year_months}")
        print(f"next_year_months: {next_year_months}")
        
        
        # Test query first to see what documents exist
        test_query_current = {
            "company_code": str(company_code),
            "internal_code_id": obj_id,
            "month": {"$in": this_year_months},
            "site_code": str(site_code),
            "type_year": int(year)
        }
        
        test_query_next = {
            "company_code": str(company_code),
            "internal_code_id": obj_id,
            "month": {"$in": next_year_months},
            "site_code": str(site_code),
            "type_year": int(year + 1)
        }
        
        print(f"Test query current year: {test_query_current}")
        print(f"Test query next year: {test_query_next}")
        
        # Count documents before deletion
        current_count = cdata_month_collection.count_documents(test_query_current)
        next_count = cdata_month_collection.count_documents(test_query_next)
        
        print(f"Documents found for current year: {current_count}")
        print(f"Documents found for next year: {next_count}")
        
        # If no documents found, check what's actually in the database
        if current_count == 0 and next_count == 0:
            print("No documents found. Checking database for similar documents...")
            sample_docs = cdata_month_collection.find({
                "company_code": str(company_code),
                "internal_code_id": obj_id
            }).limit(5)
            
            for doc in sample_docs:
                print(f"Found doc: company={doc.get('company_code')}, month={doc.get('month')}, year={doc.get('type_year')}, site={doc.get('site_code')}")
        
        # Delete for current year (August–December)
        current_year_result = cdata_month_collection.delete_many(test_query_current)
        print(f"Current year: {current_year_result.deleted_count} documents deleted!")

        # Delete for next year (January–reporting_month)  
        next_year_result = cdata_month_collection.delete_many(test_query_next)
        print(f"Next year: {next_year_result.deleted_count} documents deleted!")
        
        return current_year_result.deleted_count + next_year_result.deleted_count
    else:
        print("Database connection failed")
        return 0