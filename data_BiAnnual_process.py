from pymongo import MongoClient
from RegionAPI import fetch_company_data
from bson.objectid import ObjectId
from dotenv import load_dotenv
from sarima import run_sarima
from datetime import datetime, timedelta
from helper import get_min_year, get_next_month_name, get_function_type
import os
import json
import re
from collections import defaultdict
import db_connection

load_dotenv()

def process_BiAnnual_data(company_id, internal_code_id, year, start_month, site_code):
    print("Detail :: ", company_id, internal_code_id, year, start_month, site_code)
    # MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017/")
    # MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "ensogov")

    # # Connect to MongoDB
    # client = MongoClient(MONGODB_URL)
    # db = client[MONGODB_DB_NAME]
    

    connection = db_connection.connect_to_database()
    if connection is not None:
        # company_code_collection = connection["company_codes"]
        cdata_month_collection = connection["cdata_month"]
        cdata_quarter_collection = connection["cdata_quarter"]
        cdata_BiAnnual_collection = connection["cdata_bi_annual"]
        cdata_yearly_collection = connection["cdata_yearly"]
        cdata_collection = connection["cdata"]
        code_collection = connection["codes"]
    else:
        print("Database connection is not available.")

    company_id = str(company_id)
 
    month_order = {
        "January": 1,
        "February": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12
    }

    def get_min_year(company_code):
        min_year_query = [
        {"$match": {"company_code": company_code}},
        {"$group": {"_id": None, "min_year": {"$min": "$type_year"}}}
        ]
        min_year_result = list(cdata_collection.aggregate(min_year_query))

        if min_year_result:
            return min_year_result[0]["min_year"]
        else:
            return None

    def get_internal_code_ids(company_code, internal_code_ids):
        query = {
            "_id": {"$in": internal_code_ids}
        }
        projection = {
            "code": 1, 
            "name": 1,  
        }
        matching_documents = list(code_collection.find(query, projection))
        return matching_documents

    def all_values_same_length(lst):
        str_lst = [str(x) for x in lst]
        
        # Get the length of the first element
        length = len(str_lst[0])
        
        # Check if all elements have the same length
        return all(len(x) == length for x in str_lst)

    def get_unique_code_ids(result):
        unique_internal_code_ids = set()  
        for item in result:
            unique_internal_code_ids.add(ObjectId(item['internal_code_id']))

        unique_internal_code_ids_list = list(unique_internal_code_ids)
        return unique_internal_code_ids_list
    
    # Function to get the next month from a given month
    def get_next_month(current_month):
        # List of month names
        months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        # Get the index of the current month
        current_month_index = months.index(current_month)
        # Increment the index to get the index of the next month
        next_month_index = (current_month_index + 1) % 12
        # Return the name of the next month
        return months[next_month_index]
    
    def extract_number_from_string(s):
        # Use regular expression to find all sequences of digits
        match = re.findall(r'\d+', s)
        
        # Join the matched numbers to form the final number
        number = ''.join(match)
        
        return int(number) if number else None

    def safe_int(value, default=0):
        if value is not None and value != "":
            try:
                return int(value)
            except ValueError:
                return 0
        return 0
            
    def details_to_tuple(details):
        if details:
            return tuple(sorted((d['key'], d['value']) for d in details))
        return 
        # print("tuple :")
        # Convert details list to a sorted tuple of tuples

    def merge_objects(objects):
        merged = {}
        # print("Object merge :", objects)
        for obj in objects:
            # print("detailll :", obj.get('qty'))
            if isinstance(obj, dict) and 'details' in obj and isinstance(obj['details'], list):
            # details_key = details_to_tuple(obj['details'])
                details_key = details_to_tuple(obj['details'])
                
                if details_key not in merged:
                    merged[details_key] = {
                        'qty': 0,
                        'unit': obj['unit'],
                        'currency': obj.get('currency', ''),
                        'value': 0,
                        'details': obj['details'],
                        'key': obj.get('key', ''),
                        'value1': obj.get('value1', '')
                    }
                
                merged[details_key]['qty'] += safe_int(obj['qty'])
                merged[details_key]['value'] += safe_int(obj['value'])
                
        return list(merged.values())
    # Yearly Start 
    def process_yearly_data(result, company_code):
        count = 1
        data_type = get_function_type(allCodes)
        for i in range(0, len(result), 2):
            group = result[i:i+2]
            last_record = result[-1]

            if len(group) >= 2:
                if group[0]['is_forecast'] == False and group[1]['is_forecast'] == False:

                    if data_type == 'sum':
                        total_qty = sum(safe_int(obj['qty']) for obj in group)
                        # print("dddd :: ", group)
                        total_value = sum(safe_int(obj['value']) for obj in group)

                    if data_type == 'average':
                        total_qty = sum(safe_int(obj['qty']) for obj in group)
                        total_value = sum(safe_int(obj['value']) for obj in group)

                    if data_type == 'list':
                        total_qty = list(safe_int(obj['qty']) for obj in group)[-1]
                        total_value = list(safe_int(obj['value']) for obj in group)[-1] 

                    merged_objects = []
                    final_dimension = []
                    for dim in group:
                        for dimension in dim['dimension']:
                            merged_objects.append(dimension)
                        if len(merged_objects) > 0:
                            final_dimension = merge_objects(merged_objects)
                    c_name = " "
                    c_code = " "
                    code = next((doc for doc in allCodes if doc['_id'] == ObjectId(result[i]['internal_code_id'])), None)
                    if code is not None:
                        c_code = code['code']
                        c_name = code['name']
                    else:
                        c_code = " "
                        c_name = " "
                    next_data = result[i+2] if i+2 < len(result) else None
                    if next_data is not None and next_data['type_year'] != result[i]['type_year']:
                        count = 0
                        
                    cdata_yearly_collection.delete_many({
                        "company_code": result[i]['company_code'],
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,                                       
                        "code": c_code,
                        "is_forecast": False,
                    })
                    cdata_yearly_collection.insert_one({
                        "company_code": result[i]['company_code'],
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "qty": str(total_qty),
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,                                       
                        "code": c_code,
                        "value": total_value,
                        "currency": result[i]['currency'],
                        "dimension": final_dimension,
                        "unit": result[i]['unit'],
                        "description": result[i]['description'],
                        "is_forecast": False,
                        "created_at": datetime.now()
                                    })
                    
                    sarima_array.append(total_qty)
                    count += 1 
                else:
                    total_qty = sum(safe_int(obj['qty']) for obj in group)
                    code = next((doc for doc in allCodes if doc['_id'] == ObjectId(result[i]['internal_code_id'])), None)
                    c_name = " "
                    c_code = " "
                    if code is not None:
                        c_code = code['code']
                        c_name = code['name']
                    else:
                        c_code = " "
                        c_name = " "
                    
                    next_data = result[i+2] if i+2 < len(result) else None
                    if next_data is not None and next_data['type_year'] != result[i]['type_year']:
                        count = 0
                    cdata_yearly_collection.delete_many({
                        "company_code": result[i]['company_code'],
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,                                       
                        "code": c_code,
                        "is_forecast": True,
                    })
                    cdata_yearly_collection.insert_one({
                        "company_code": result[i]['company_code'],
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "qty": str(total_qty),
                        "site_code": result[i]['site_code'],
                        "internal_code_id": (result[i]['internal_code_id']),
                        "code_name": c_name,
                        "code": c_code,
                        "description": result[i]['description'],
                        "is_forecast": True,
                        "created_at": datetime.now()
                    })
                    
                    sarima_array.append(total_qty)
                    count += 1 
            else:
                    total_qty = sum(safe_int(obj['qty']) for obj in group)
                    code = next((doc for doc in allCodes if doc['_id'] == ObjectId(result[i]['internal_code_id'])), None)
                    c_name = " "
                    c_code = " "
                    if code is not None:
                        c_code = code['code']
                        c_name = code['name']
                    else:
                        c_code = " "
                        c_name = " "

                    next_data = result[i+2] if i+2 < len(result) else None
                    if next_data is not None and next_data['type_year'] != result[i]['type_year']:
                        count = 0
                    cdata_yearly_collection.delete_many({
                        "company_code": result[i]['company_code'],
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,                                       
                        "code": c_code,
                        "is_forecast": True,
                    })
                    cdata_yearly_collection.insert_one({
                        "company_code": result[i]['company_code'],
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "qty": str(total_qty),
                        "site_code": result[i]['site_code'],
                        "internal_code_id": (result[i]['internal_code_id']),
                        "code_name": c_name,
                        "code": c_code,
                        "is_forecast": True,
                        "created_at": datetime.now()
                    })
                    sarima_array.append(total_qty)
                    count += 1 

    get_company_year = get_min_year(company_id)
    if get_company_year is not None:
        get_company_year = int(get_company_year)
        if get_company_year >= int(year):
            min_year = year
        else:
            min_year = get_company_year

        given_month = start_month
        given_month_numeric = month_order[given_month]


        query = {
            "company_code": str(company_id),
            "internal_code_id": ObjectId(internal_code_id),
            "type": "actual",
            "site_code": str(site_code),
            "$or": [
                {
                    "$and": [
                        {"type_year": str(min_year)},
                        {"semi_annual": {"$exists": True, "$nin": [None, ""]}}
                    ]
                },
                {
                    "type_year": {"$gt": str(min_year)},
                    "semi_annual": {"$exists": True, "$nin": [None, ""]}
                }
            ]
        }

        documents = list(cdata_collection.find(query))
        documents_filtered = [doc for doc in documents if int(doc["type_year"]) > min_year]
        cdata = sorted(documents_filtered, key=lambda x: int(x["type_year"]))
        ids = get_unique_code_ids(cdata)
        allCodes = get_internal_code_ids(company_id, ids)

        sarima_array = []
        count =  1
        sarima_group = []
        last_report_year = ''
        for entry in cdata:
            first_record = cdata[0]
            last_record = cdata[-1]
            c_name = " "
            c_code = " "
            code = next((doc for doc in allCodes if doc['_id'] == entry["internal_code_id"]), None)
            if code is not None:
                c_code = code['code']
                c_name = code['name']
            else:
                c_code = " "
                c_name = " "
                
            if start_month != 'January' and first_record['_id'] == entry['_id']:
                reporting_year = int(entry['type_year']) + 1
            
            if start_month == 'January' and first_record['_id'] == entry['_id']:
                reporting_year = int(entry['type_year'])
            
            if count == 3:
                reporting_year += 1
                count =  1

            if last_record['_id'] == entry['_id']:
                last_reporting_count = count
                last_reporting_year = reporting_year
            
            qty_value =  entry.get("qty", "")
            if qty_value is not None and qty_value != "":
                final_qty = qty_value
            else:
                final_qty = 0

            value_value =  entry.get("qty", "")
            if value_value is not None and value_value != "":
                final_value = value_value
            else:
                final_value = 0

            narration = entry.get("narration", "")
            url = entry.get("url", "")
            description = f"{narration} {url}"
            cdata_BiAnnual_collection.delete_many({
                "company_code": company_id,
                "semi_annual": entry.get("semi_annual", ""),
                "month": entry.get("month", ""),
                "type_year": int(entry.get("type_year", "")),
                "reporting_year": reporting_year,
                "site_code" : str(site_code),
                "internal_code_id": entry.get("internal_code_id", ""),
                "code_name": c_name,
                "code": c_code,
                "ref_table": "cdata",
                "is_forecast": False,
            })
            cdata_BiAnnual_collection.insert_one({
                "company_code": company_id,
                "semi_annual": entry.get("semi_annual", ""),
                "month": entry.get("month", ""),
                "type_year": int(entry.get("type_year", "")),
                "reporting_year": reporting_year,
                "qty": str(final_qty),
                "site_code" : str(site_code),
                "internal_code_id": entry.get("internal_code_id", ""),
                "code_name": c_name,
                "code": c_code,
                "value": int(final_value),
                "currency": entry.get("currency", ""),
                "dimension": entry.get("dimension", ""),
                "unit": entry.get("unit", ""),
                "description": entry.get("description", ""),
                "ref_table": "cdata",
                "is_forecast": False,
                "created_at": datetime.now()
                        })
            count += 1
            last_report_year = reporting_year
            qty = entry.get("qty")
            if qty is not None and qty != "":
                number = extract_number_from_string(qty)
                if number is not None and number != "":
                    sarima_array.append(int(number))

        sarima_predictions = []
        if len(sarima_array) != 0:
            if len(sarima_array) > 5:
                sarima_predictions = run_sarima(sarima_array, predictedValue=11, m=2)

        if len(sarima_predictions) > 0:
            next_year = int(last_record['type_year'])
            last_reporting_year = int(last_report_year)
            c_name = last_record['code_name']
            semi_annual = last_record['semi_annual']
            for idx, pred_value in enumerate(sarima_predictions):

                if semi_annual == "Semester2":
                    next_year += 1
                    last_reporting_year += 1
                    semi_annual = f"Semester1"
                    count = 1
                else:
                    semi_annual = f"Semester{count}"
                    next_year = next_year
                    last_reporting_year = last_reporting_year

                if count == 2:
                    count = 1
                cdata_BiAnnual_collection.delete_many({
                    "company_code": company_id,
                    "semi_annual": semi_annual,
                    "type_year": next_year,
                    "reporting_year": last_reporting_year,
                    "site_code" : str(site_code),
                    "internal_code_id": ObjectId(internal_code_id),
                    "code_name": c_name,
                    "code": c_code,
                    "is_forecast": True,
                })
                cdata_BiAnnual_collection.insert_one({
                    "company_code": company_id,
                    "semi_annual": semi_annual,
                    "type_year": next_year,
                    "reporting_year": last_reporting_year,
                    "qty": str(pred_value),
                    "site_code" : str(site_code),
                    "internal_code_id": ObjectId(internal_code_id),
                    "code_name": c_name,
                    "code": c_code,
                    "value": 0,
                    "description": "",
                    "is_forecast": True,
                    "created_at": datetime.now()
                })
                last_reporting_count += 1
                count += 1
        
        query = {
                "company_code": str(company_id),
                "site_code" : str(site_code),
                "internal_code_id": ObjectId(internal_code_id)
            }

        result = list(cdata_BiAnnual_collection.find(query))

        process_yearly_data(result, company_id)
