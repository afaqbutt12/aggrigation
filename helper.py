# Get minimum year of company
def get_min_year(company_code, cdata_collection):
    min_year_query = [
    {"$match": {"company_code": company_code}},
    {"$group": {"_id": None, "min_year": {"$min": "$type_year"}}}
    ]
    min_year_result = list(cdata_collection.aggregate(min_year_query))
    if min_year_result:
        return min_year_result[0]["min_year"]
    else:
        return None

# Get Company internal code
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


# get next month
def get_next_month_name(current_month):
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

    # Reverse dictionary to get month name from month number
    month_order_reverse = {v: k for k, v in month_order.items()}

    # Get the current month number
    current_month_number = month_order.get(current_month)

    if current_month_number is None:
        return None  # Invalid month name

    # Calculate the next month number, wrapping around to January if needed
    next_month_number = (current_month_number % 12) + 1

    # Get the next month name from the reverse dictionary
    next_month = month_order_reverse[next_month_number]

    return next_month

def get_function_type(allCodes):
    if any('function' in code and code['function'] is not None for code in allCodes):
        for code in allCodes:
            if code.get('function') is not None:
                data_type = code.get('function')
    else:
        data_type = 'sum'

    return str(data_type)
