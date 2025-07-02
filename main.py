from script_functions import fetch_company_data, process_monthly_data, process_bi_annual_data, process_yearly_data
from RegionAPI import fetch_company_data, fetch_all_company
from data_quarterly_process import process_quarterly_data
from data_BiAnnual_process import process_BiAnnual_data
from data_monthly_process import process_monthly_data
from data_yearly_process import process_yearly_data
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import date
import argparse
import logging
import sys
import os
import db_connection

load_dotenv()


if __name__ == "__main__":

    connection = db_connection.connect_to_database()
    if connection is not None:
        company_code_collection = connection["company_codes"]
    else:
        print("Database connection is not available.")
    
    # Get the current date
    current_date = date.today()
    current_year = int(current_date.strftime("%Y"))
    year = current_year - 6

    all_company = fetch_all_company()
    print("all company", all_company)

    for company in all_company:
        # if company["id"] == 359:
        
        month = fetch_company_data(company['id'])
        print("startmonth: ", month)
        if month is not None:
            start_month = str(month[0])
        else:
            start_month = 'January'
        company_id = str(company['id'])
        # print("one ::: ", company_code_collection)
        company_codes = list(company_code_collection.find({"company_id": str(company["id"])}))
        # print("company_codes :: ", company_codes)
        reporting_frequency = []
        if company["reporting_frequency"] != None:
            reporting_frequency = company["reporting_frequency"].split(',')

        for company_code in company_codes:
            internal_code_id = company_code['internal_code_id']

            if 'month' in reporting_frequency:
                print("monthly condition")
                if company_id and internal_code_id and year and start_month:
                    process_monthly_data(company_id, internal_code_id, year, start_month,  site_code="")
                for site_code in company['company_sites']:
                    if company_id and internal_code_id and year and start_month and site_code["internal_site_code"]:
                        process_monthly_data(company_id, internal_code_id, year, start_month, site_code["internal_site_code"])
            elif 'quater' in reporting_frequency:
                print('quater condition')
                if company_id and internal_code_id and year and start_month:
                    process_quarterly_data(company_id, internal_code_id, year, start_month,  site_code="")
                for site_code in company['company_sites']:
                    if company_id and internal_code_id and year and start_month and site_code["internal_site_code"]:
                        process_quarterly_data(company_id, internal_code_id, year, start_month, site_code["internal_site_code"])
            elif 'semi_annual' in reporting_frequency:
                print('semi_annual condition')
                if company_id and internal_code_id and year and start_month:
                    process_BiAnnual_data(company_id, internal_code_id, year, start_month,  site_code="")
                for site_code in company['company_sites']:
                    if company_id and internal_code_id and year and start_month and site_code["internal_site_code"]:
                        process_BiAnnual_data(company_id, internal_code_id, year, start_month, site_code["internal_site_code"])
            elif 'annual' in reporting_frequency:
                print('annual condition')
                if company_id and internal_code_id and year and start_month:
                    process_yearly_data(company_id, internal_code_id, year, start_month,  site_code="")
                for site_code in company['company_sites']:
                    if company_id and internal_code_id and year and start_month and site_code["internal_site_code"]:
                        process_yearly_data(company_id, internal_code_id, year, start_month, site_code["internal_site_code"])
            else:
                print("No data found")





















# main.py
# import argparse
# import logging
# import sys
# from script_functions import fetch_company_data, process_monthly_data, process_bi_annual_data, process_yearly_data
# from quaterly_data import process_quarterly_data
# from sarima import run_sarima  # Import the SARIMA function

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description='Process company data.')
#     parser.add_argument('company_code', type=int, help='Company code')
#     parser.add_argument('--year', type=int, help='Year')
#     args = parser.parse_args()

#     company_code = args.company_code
#     year = args.year

#     # Fetch company data
#     start_month_tuple = fetch_company_data(company_code)
#     if not start_month_tuple:
#         logger.error("Failed to fetch start month or reporting frequency for company %s.", company_code)
#         sys.exit(1)
    
#     start_month, reporting_frequency = start_month_tuple

#     # Initialize dictionary to store SARIMA predictions for each reporting frequency
#     sarima_predictions_dict = {}

#     # Process data based on reporting frequency
#     for item in reporting_frequency:
#         if item == "month":
#             logger.info('Processing monthly data...')
#             time_series_data, total_qty = process_monthly_data(company_code, year, start_month)
#         elif item == "quarter":
#             logger.info('Processing quarterly data...')
#             time_series_data, total_qty = process_quarterly_data(company_code, year)
#         elif item == "semi_annual":
#             logger.info('Processing semi-annual data...')
#             time_series_data, total_qty = process_bi_annual_data(company_code, year, reporting_frequency)
#         elif item == "annual":
#             logger.info('Processing annual data...')
#             time_series_data, total_qty = process_yearly_data(company_code, year, reporting_frequency)
        
#         # Run SARIMA model with the fetched time series data and quantity data
#         sarima_predictions = run_sarima(time_series_data, total_qty)
#         sarima_predictions_dict[item] = sarima_predictions
    
#     # Print SARIMA predictions for each reporting frequency
#     for item, predictions in sarima_predictions_dict.items():
#         print(f"SARIMA Predictions for {item}: {predictions}")
