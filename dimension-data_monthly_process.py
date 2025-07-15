from pymongo import MongoClient
from RegionAPI import fetch_company_data
from bson.objectid import ObjectId
from dotenv import load_dotenv
from sarima import run_sarima
from datetime import datetime, timedelta
from helper import get_min_year, get_next_month_name, get_function_type
from collections import defaultdict
import os
import json
import re

load_dotenv()

def process_monthly_data(company_id, internal_code_id, year, start_month, site_code):
    print("Company Details :: ", company_id, internal_code_id, year, start_month, site_code)
    MONGODB_URL = os.getenv("MONGODB_URL", "")
    MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "")

    # Connect to MongoDB
    client = MongoClient(MONGODB_URL)
    db = client[MONGODB_DB_NAME]
    cdata_month_collection = db["cdata_month"]
    cdata_quarter_collection = db["cdata_quarter"]
    cdata_BiAnnual_collection = db["cdata_bi_annual"]
    cdata_yearly_collection = db["cdata_yearly"]
    cdata_collection = db["cdata"]
    code_collection = db["codes"]

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


    def get_internal_code_ids(company_code, internal_code_ids):
        query = {
            "_id": {"$in": internal_code_ids}
        }
        projection = {
            "code": 1, 
            "name": 1,
            "function": 1,  
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
        if s is None:
            return None

        # Ensure s is a string
        s = str(s)
        
        # Find all digit sequences in the string
        match = re.findall(r'\d+', s)

        # Join the found sequences into one number string
        number = ''.join(match)

        # Convert the number string to an integer if it's not empty, otherwise return None
        return int(number) if number else None
        # match = re.findall(r'\d+', s)

        # number = ''.join(match)
        
        # return int(number) if number else None

    def safe_int(value, default=0):
        if value is not None and value != "":
            try:
                return int(value)
            except ValueError:
                return 0
        return 0
        # try:
        #     return int(value)
        # except (ValueError, TypeError):
        #     return default
            
    def details_to_tuple(details):
        return tuple(sorted((d['key'], d['value']) for d in details))

    def merge_objects(objects):
        merged = {}
        if objects:
            for obj in objects:
                if isinstance(obj, dict) and 'details' in obj and isinstance(obj['details'], list):
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
        return 

    # Quarterly Start 
    def process_quarterly_data(result, company_code):

        data_type = get_function_type(allCodes)

        count = 1
        reporting_year_counter = 1
        for i in range(0, len(result), 3):
            group = result[i:i+3]

            if start_month != 'January':
                reporting_year = int(result[i]['type_year']) + 1
        
            if start_month == 'January':
                reporting_year = int(result[i]['type_year'])
            
            if reporting_year_counter == 4:
                reporting_year += 1
                reporting_year_counter =  1  
            if count == 5:
                count = 1 
       
            if len(group) == 3:
                if group[0]['is_forecast'] == False and group[1]['is_forecast'] == False and group[2]['is_forecast'] == False:
                    if data_type == 'sum':
                        total_qty = sum(safe_int(obj['qty']) for obj in group)
                        total_value = sum(safe_int(obj['value']) for obj in group)

                    if data_type == 'average':
                        sum_value = sum(safe_int(obj['qty']) for obj in group)
                        count = len(group)
                        total_qty = sum_value / count if count > 0 else 0

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
                    quarter = f"Q{count}"
                    months = '-'.join(obj['month'] for obj in group)
                    code = next((doc for doc in allCodes if doc['_id'] == ObjectId(result[i]['internal_code_id'])), None)
                    if code is not None:
                        c_code = code['code']
                        c_name = code['name']
                    else:
                        c_code = " "
                        c_name = " "
                    cdata_quarter_collection.delete_many({
                        "company_code": result[i]['company_code'],
                        "quarter": quarter,
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,
                        "code": c_code,
                        "ref_table": "cdata_month",
                        "is_forecast": False,
                    })
                    cdata_quarter_collection.insert_one({
                        "company_code": result[i]['company_code'],
                        "quarter": quarter,
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "qty": total_qty,
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,
                        "code": c_code,
                        "value": total_value,
                        "currency": result[i]['currency'],
                        "dimension": final_dimension,
                        "unit": result[i]['unit'],
                        "description": result[i]['description'],
                        "ref_table": "cdata_month",
                        "is_forecast": False,
                    })
                    
                    sarima_array.append(total_qty)
                    count += 1 
                    reporting_year_counter += 1
                else:
                    total_qty = sum(safe_int(obj['qty']) for obj in group)
                    total_value = 0
                    total_value = sum(safe_int(obj['value']) for obj in group)
                    code = next((doc for doc in allCodes if doc['_id'] == ObjectId(result[i]['internal_code_id'])), None)
                    if code is not None:
                        c_code = code['code']
                        c_name = code['name']
                    else:
                        c_code = " "
                        c_name = " "
                    quarter = f"Q{count}"
                    months = '-'.join(obj['month'] for obj in group)
                    
                    next_data = result[i+3] if i+3 < len(result) else None
                    if next_data is not None and next_data['type_year'] != result[i]['type_year']:
                        count = 0

                    # Fetch and concatenate narration and url
                    narration = entry.get("narration", "")
                    url = entry.get("url", "")
                    description = f"{narration} {url}"
                    cdata_quarter_collection.delete_many({
                        "company_code": result[i]['company_code'],
                        "quarter": quarter,
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,
                        "code": c_code,
                        "ref_table": "cdata_month",
                        "is_forecast": True,
                    })
                    cdata_quarter_collection.insert_one({
                        "company_code": result[i]['company_code'],
                        "quarter": quarter,
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "qty": total_qty,
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,
                        "code": c_code,
                        "value": total_value,
                        "description": result[i]['description'],
                        "ref_table": "cdata_month",
                        "is_forecast": True,
                    })
                    
                    sarima_array.append(total_qty)
                    count += 1 
                    reporting_year_counter += 1
    # Quarterly End 

    # Bi Annual Start 
    def process_BiAnnual_data(result, company_code):

        data_type = get_function_type(allCodes)

        # data_type = 'sum'
        count = 1
        reporting_year_count = 1
        
        for i in range(0, len(result), 6):
            group = result[i:i+6]

            if start_month != 'January':
                reporting_year = int(result[i]['type_year']) + 1
            
            if start_month == 'January':
                reporting_year = int(result[i]['type_year'])
            
            if count == 3:
                count = 1

            if len(group) == 6:
                if group[0]['is_forecast'] == False and group[1]['is_forecast'] == False and group[2]['is_forecast'] == False and group[3]['is_forecast'] == False and group[4]['is_forecast'] == False and group[5]['is_forecast'] == False:
                    total_qty = sum(safe_int(obj['qty']) for obj in group)
                    total_value = sum(safe_int(obj['value']) for obj in group)

                    if data_type == 'sum':
                        total_qty = sum(safe_int(obj['qty']) for obj in group)
                        total_value = sum(safe_int(obj['value']) for obj in group)

                    if data_type == 'average':
                        # total_qty = sum(safe_int(obj['qty']) for obj in group)
                        sum_value = sum(safe_int(obj['qty']) for obj in group)
                        count = len(group)
                        total_qty = sum_value / count if count > 0 else 0

                        total_value = sum(safe_int(obj['value']) for obj in group)

                    if data_type == 'list':
                        # print("my-list-bi-annual")
                        total_qty = list(safe_int(obj['qty']) for obj in group)[-1]
                        total_value = list(safe_int((obj['value'])) for obj in group)[-1] 

                    merged_objects = []
                    final_dimension = []
                    for dim in group:
                        for dimension in dim['dimension']:
                            merged_objects.append(dimension)
                        if len(merged_objects) > 0:
                            final_dimension = merge_objects(merged_objects)
                            # final_dimension = 0
                    
                    quarter = f"Semester{count}"
                    months = '-'.join(obj['month'] for obj in group)
                    code = next((doc for doc in allCodes if doc['_id'] == ObjectId(result[i]['internal_code_id'])), None)
                    c_name=" "
                    if code is not None:
                        c_code = code['code']
                        c_name = code['name']
                    else:
                        c_code = " "
                        c_name = " "
                    next_data = result[i+6] if i+6 < len(result) else None

                    narration = entry.get("narration", "")
                    url = entry.get("url", "")
                    description = f"{narration} {url}"
                    cdata_BiAnnual_collection.delete_many({
                        "company_code": result[i]['company_code'],
                        "semi_annual": quarter,
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "site_code": result[i]['site_code'],
                        "internal_code_id": ObjectId(result[i]['internal_code_id']),
                        "code_name": c_name,
                        "code": c_code,
                        "is_forecast": False,
                    })
                    cdata_BiAnnual_collection.insert_one({
                        "company_code": result[i]['company_code'],
                        "semi_annual": quarter,
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "qty": total_qty,
                        "site_code": result[i]['site_code'],
                        "internal_code_id": ObjectId(result[i]['internal_code_id']),
                        "code_name": c_name,
                        "code": c_code,
                        "value": total_value,
                        "currency": result[i]['currency'],
                        "dimension": final_dimension,
                        "unit": result[i]['unit'],
                        "description": result[i]['description'],
                        "ref_table": "cdata_month",
                        "is_forecast": False
                    })
                    
                    sarima_array.append(total_qty)
                    count += 1 
                    reporting_year_count += 1
                else:
                    total_qty = sum(safe_int(obj['qty']) for obj in group)
                    total_value = sum(safe_int(obj['value']) for obj in group)
                    quarter = f"Semester{count}"
                    months = '-'.join(obj['month'] for obj in group)
                    code = next((doc for doc in allCodes if doc['_id'] == ObjectId(result[i]['internal_code_id'])), None)
                    if code is not None:
                        c_code = code['code']
                        c_name = code['name']
                    else:
                        c_code = " "
                        c_name = " "
                    next_data = result[i+6] if i+6 < len(result) else None 
                    cdata_BiAnnual_collection.delete_many({
                        "company_code": result[i]['company_code'],
                        "semi_annual": quarter,
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "site_code": result[i]['site_code'],
                        "internal_code_id": ObjectId(result[i]['internal_code_id']),
                        "code_name": c_name,
                        "code": c_code,
                        "is_forecast": True,
                    })
                    cdata_BiAnnual_collection.insert_one({
                        "company_code": result[i]['company_code'],
                        "semi_annual": quarter,
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": result[i]['reporting_year'],
                        "qty": total_qty,
                        "site_code": result[i]['site_code'],
                        "internal_code_id": ObjectId(result[i]['internal_code_id']),
                        "code_name": c_name,
                        "code": c_code,
                        "value": total_value,
                        "description": result[i]['description'],
                        "is_forecast": True
                    })
                    
                    sarima_array.append(total_qty)
                    count += 1 
    # Bi Annual End 

    # Yearly Start 
    def process_yearly_data(result, company_code):

        data_type = get_function_type(allCodes)
        count = 1
            
        # data_type = 'sum'
        for i in range(0, len(result), 12):
            group = result[i:i+12]
            
            reporting_year = result[i]['reporting_year']
            if start_month != 'January' and group[0]['type_year'] == group[0]['reporting_year']:
                reporting_year += 1


            if len(group) >= 12:
                if group[0]['is_forecast'] == False and group[1]['is_forecast'] == False and group[2]['is_forecast'] == False and group[3]['is_forecast'] == False and group[4]['is_forecast'] == False and group[5]['is_forecast'] == False and group[6]['is_forecast'] == False and group[7]['is_forecast'] == False and group[8]['is_forecast'] == False and group[9]['is_forecast'] == False and group[10]['is_forecast'] == False and group[11]['is_forecast'] == False:

                    if data_type == 'sum':
                        total_qty = sum(safe_int(obj['qty']) for obj in group)
                        total_value = sum(safe_int(obj['value']) for obj in group)

                    if data_type == 'average':
                        # total_qty = sum(safe_int(obj['qty']) for obj in group)
                        sum_value = sum(safe_int(obj['qty']) for obj in group)
                        count = len(group)
                        total_qty = sum_value / count if count > 0 else 0
                        total_value = sum(safe_int(obj['value']) for obj in group)

                    if data_type == 'list':
                        # print("my-list")
                        total_qty = list(safe_int(obj['qty']) for obj in group)[-1]
                        total_value = list(safe_int(obj['value']) for obj in group)[-1] 


                    merged_objects = []
                    final_dimension = []
                    for dim in group:
                        for dimension in dim['dimension']:
                            merged_objects.append(dimension)
                        if len(merged_objects) > 0:
                            final_dimension = merge_objects(merged_objects)
                            # final_dimension = 0
                    c_name=" "
                    months = '-'.join(obj['month'] for obj in group)
                    code = next((doc for doc in allCodes if doc['_id'] == ObjectId(result[i]['internal_code_id'])), None)
                    if code is not None:
                        c_code = code['code']
                        c_name = code['name']
                    else:
                        c_code = " "
                        c_name = " "
                    next_data = result[i+12] if i+12 < len(result) else None
                    if next_data is not None and next_data['type_year'] != result[i]['type_year']:
                        count = 0
                    narration = entry.get("narration", "")
                    url = entry.get("url", "")
                    description = f"{narration} {url}"  
                    cdata_yearly_collection.delete_many({
                        "company_code": result[i]['company_code'],
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": reporting_year',
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,                                       
                        "code": c_code,
                        "is_forecast": False,
                    })  
                    cdata_yearly_collection.insert_one({
                        "company_code": result[i]['company_code'],
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": reporting_year,
                        "qty": total_qty,
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
                    })
                    
                    sarima_array.append(total_qty)
                    count += 1 
                else:
                    total_qty = sum(safe_int(obj['qty']) for obj in group)
                    total_value = sum(safe_int(obj['value']) for obj in group)

                    months = '-'.join(obj['month'] for obj in group)
                    code = next((doc for doc in allCodes if doc['_id'] == ObjectId(result[i]['internal_code_id'])), None)
                    if code is not None:
                        c_code = code['code']
                        c_name = code['name']
                    else:
                        c_code = " "
                        c_name = " "
                    next_data = result[i+12] if i+12 < len(result) else None
                    if next_data is not None and next_data['type_year'] != result[i]['type_year']:
                        count = 0
                    cdata_yearly_collection.delete_many({
                        "company_code": result[i]['company_code'],
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": reporting_year,
                        "site_code": result[i]['site_code'],
                        "internal_code_id": result[i]['internal_code_id'],
                        "code_name": c_name,                                       
                        "code": c_code,
                        "is_forecast": True,
                    })
                    cdata_yearly_collection.insert_one({
                        "company_code": result[i]['company_code'],
                        "month": months,
                        "type_year": result[i]['type_year'],
                        "reporting_year": reporting_year,
                        "qty": total_qty,
                        "site_code": result[i]['site_code'],
                        "internal_code_id": (result[i]['internal_code_id']),
                        "code_name": c_name,
                        "code": c_code,
                        "description": result[i]['description'],
                        "value": total_value,
                        "is_forecast": True,
                    })
                    
                    sarima_array.append(total_qty)
                    count += 1 
    # Yearly End 
    
    get_company_year = get_min_year(company_id, cdata_collection)

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
                        {"month": {"$exists": True, "$nin": [None, ""]}}
                    ]
                },
                {
                    "type_year": {"$gt": str(min_year)},
                    "month": {"$exists": True, "$nin": [None, ""]}
                }
            ]
        }

        documents = list(cdata_collection.find(query))
        documents_filtered = [doc for doc in documents if int(doc["type_year"]) > min_year or (int(doc["type_year"]) == min_year and month_order[doc["month"]] >= given_month_numeric)]
        cdata = sorted(documents_filtered, key=lambda x: (x["type_year"]))
        ids = get_unique_code_ids(cdata)
        allCodes = get_internal_code_ids(company_id, ids)

        sarima_array = []
        sarima_dimension_array = []
        count =  1
        sarima_group = []
        dimension_list = dict([])

        for entry in cdata:
            first_record = cdata[0]
            last_record = cdata[-1]
            c_name=" "
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
            
            if count == 13:
                reporting_year += 1
                count =  1

            if last_record['_id'] == entry['_id']:
                last_reporting_count = count
                last_reporting_year = reporting_year
            
            qty_value =  entry.get("qty", "")
            if qty_value is not None and qty_value != "":
                if isinstance(qty_value, str):
                    qty_value = float(qty_value)
                    final_qty = int(qty_value)
                else:
                    final_qty = int(qty_value)
            else:
                final_qty = 0

            # print("Dimension :: ", entry.get("dimension"))
            
            for item in entry.get("dimension"):
                index_value = ''
                for detail in item.get("details"):
                    if detail.get("value") != '':

                        index_value = index_value + detail.get("value")  # Example index value
                        # detail = {"key1": "value1", "key2": "value2"}  # Example dictionary

                        if index_value in dimension_list:
                            # index_value exists in the dictionary
                            dimension_list[index_value].extend(item.get("qty"))  # Add or update a new element with the existing key
                        else:
                            dimension_list[index_value] = [item.get("qty")]
                            # index_value does not exist in the dictionary
                            print("Index value does not exist in the dictionary.")

                        print(detail)

            # Fetch and concatenate narration and url
            narration = entry.get("narration", "")
            url = entry.get("url", "")
            description = f"{narration} {url}" 
            cdata_month_collection.delete_many({
                "company_code": company_id,
                "month": entry.get("month", ""),
                "type_year": int(entry.get("type_year", "")),
                "reporting_year": reporting_year,
                "site_code" : str(site_code),
                "internal_code_id": entry.get("internal_code_id", ""),
                "code_name": c_name,
                "code": c_code,
            })  
            cdata_month_collection.insert_one({
                "company_code": company_id,
                "month": entry.get("month", ""),
                "type_year": int(entry.get("type_year", "")),
                "reporting_year": reporting_year,
                "qty": int(final_qty),
                "site_code" : str(site_code),
                "internal_code_id": entry.get("internal_code_id", ""),
                "code_name": c_name,
                "code": c_code,
                "value": entry.get("value", ""),
                "currency": entry.get("currency", ""),
                "dimension": entry.get("dimension", ""),
                "unit": entry.get("unit", ""),
                "description": description,
                "ref_table": "cdata",
                "is_forecast": False
            })
            count += 1
            
            qty = final_qty
            if qty is not None and qty != "":
                number = extract_number_from_string(qty)
                if number is not None and number != "":
                    sarima_array.append(int(number))

        print(f"dimension_list :: {internal_code_id} ", dimension_list)
        sarima_predictions = []
        query = {
                "company_code": str(company_id),
                "site_code" : str(site_code),
                "internal_code_id": ObjectId(internal_code_id)
            }

        # Retrieve the documents matching the query
        result = list(cdata_month_collection.find(query))

