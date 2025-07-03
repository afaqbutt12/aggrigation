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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CompanyDataController:
    def __init__(self):
        self.connection = db_connection.connect_to_database()
        if self.connection is not None:
            self.company_code_collection = self.connection["company_codes"]
        else:
            raise Exception("Database connection is not available.")
    def process_company_data(self, company_id: int = None) -> dict:
        try:
            # Get the current date
            current_date = date.today()
            current_year = int(current_date.strftime("%Y"))
            year = current_year - 6
            all_company = fetch_all_company()
            company = next((c for c in all_company if c['id'] == company_id), None)
            logger.info(all_company)
            # Determine which companies to process
            if company:
                companies = self._get_company_by_id(company_id)
                if not companies:
                    return {
                        "success": False,
                        "message": f"Company with ID {company_id} not found",
                        "data": None
                    }
                logger.info(f"Processing data for company ID: {company_id}")
            else:
                companies = fetch_all_company()
                logger.info(f"Processing data for all companies. Total companies: {len(companies)}")
            
            processed_companies = []
            
            for company in companies:
                try:
                    result = self._process_single_company(company, year)
                    processed_companies.append(result)
                except Exception as e:
                    logger.error(f"Error processing company {company.get('id', 'unknown')}: {str(e)}")
                    processed_companies.append({
                        "company_id": company.get('id', 'unknown'),
                        "status": "error",
                        "error": str(e)
                    })
            
            return {
                "success": True,
                "message": f"Successfully processed {len(processed_companies)} companies",
                "data": processed_companies
            }
            
        except Exception as e:
            logger.error(f"Error processing company data: {str(e)}")
            return {
                "success": False,
                "message": f"Error processing company data: {str(e)}",
                "data": None
            }

    def _get_company_by_id(self, company_id):
        """Get a specific company by ID"""
        try:
            all_companies = fetch_all_company()
            company = next((c for c in all_companies if c["id"] == company_id), None)
            return [company] if company else []
        except Exception as e:
            logger.error(f"Error fetching company by ID {company_id}: {str(e)}")
            return []
    
    def _process_single_company(self, company, year):
        """Process a single company's data"""
        try:
            company_id = str(company['id'])
            logger.info(f"Processing company: {company_id}")
            
            # Get start month
            month = fetch_company_data(company['id'])
            start_month = str(month[0]) if month is not None else 'January'
            logger.info(f"Start month for company {company_id}: {start_month}")
            
            # Get company codes
            company_codes = list(self.company_code_collection.find({"company_id": company_id}))
            
            # Get reporting frequency
            reporting_frequency = []
            if company["reporting_frequency"] is not None:
                reporting_frequency = company["reporting_frequency"].split(',')
            
            processed_codes = []
            
            for company_code in company_codes:
                internal_code_id = company_code['internal_code_id']
                
                # Process based on reporting frequency
                for freq in reporting_frequency:
                    freq = freq.strip()  # Remove any whitespace
                    
                    if freq == 'month':
                        logger.info(f"Processing monthly data for company {company_id}")
                        self._process_monthly(company_id, internal_code_id, year, start_month, company)
                    elif freq == 'quater':
                        logger.info(f"Processing quarterly data for company {company_id}")
                        self._process_quarterly(company_id, internal_code_id, year, start_month, company)
                    elif freq == 'semi_annual':
                        logger.info(f"Processing semi-annual data for company {company_id}")
                        self._process_bi_annual(company_id, internal_code_id, year, start_month, company)
                    elif freq == 'annual':
                        logger.info(f"Processing annual data for company {company_id}")
                        self._process_yearly(company_id, internal_code_id, year, start_month, company)
                    else:
                        logger.warning(f"Unknown reporting frequency: {freq}")
                
                processed_codes.append({
                    "internal_code_id": internal_code_id,
                    "reporting_frequency": reporting_frequency,
                    "status": "processed"
                })
            
            return {
                "company_id": company_id,
                "processed_codes": processed_codes,
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error processing company {company.get('id', 'unknown')}: {str(e)}")
            return {
                "company_id": company.get('id', 'unknown'),
                "status": "error",
                "error": str(e)
            }
    
    def _process_monthly(self, company_id, internal_code_id, year, start_month, company):
        """Process monthly data for a company"""
        if company_id and internal_code_id and year and start_month:
            process_monthly_data(company_id, internal_code_id, year, start_month, site_code="")
        
        for site_code in company.get('company_sites', []):
            if (company_id and internal_code_id and year and start_month and 
                site_code.get("internal_site_code")):
                process_monthly_data(company_id, internal_code_id, year, start_month, 
                                   site_code["internal_site_code"])
    
    def _process_quarterly(self, company_id, internal_code_id, year, start_month, company):
        """Process quarterly data for a company"""
        if company_id and internal_code_id and year and start_month:
            process_quarterly_data(company_id, internal_code_id, year, start_month, site_code="")
        
        for site_code in company.get('company_sites', []):
            if (company_id and internal_code_id and year and start_month and 
                site_code.get("internal_site_code")):
                process_quarterly_data(company_id, internal_code_id, year, start_month, 
                                     site_code["internal_site_code"])
    
    def _process_bi_annual(self, company_id, internal_code_id, year, start_month, company):
        """Process bi-annual data for a company"""
        if company_id and internal_code_id and year and start_month:
            process_BiAnnual_data(company_id, internal_code_id, year, start_month, site_code="")
        
        for site_code in company.get('company_sites', []):
            if (company_id and internal_code_id and year and start_month and 
                site_code.get("internal_site_code")):
                process_BiAnnual_data(company_id, internal_code_id, year, start_month, 
                                    site_code["internal_site_code"])
    
    def _process_yearly(self, company_id, internal_code_id, year, start_month, company):
        """Process yearly data for a company"""
        if company_id and internal_code_id and year and start_month:
            process_yearly_data(company_id, internal_code_id, year, start_month, site_code="")
        
        for site_code in company.get('company_sites', []):
            if (company_id and internal_code_id and year and start_month and 
                site_code.get("internal_site_code")):
                process_yearly_data(company_id, internal_code_id, year, start_month, 
                                  site_code["internal_site_code"])


def main():
    """Main function for command line execution"""
    parser = argparse.ArgumentParser(description='Process company data.')
    parser.add_argument('--company_id', type=int, help='Specific company ID to process')
    args = parser.parse_args()
    
    try:
        controller = CompanyDataController()
        result = controller.process_company_data(company_id=args.company_id)
        
        if result["success"]:
            logger.info(result["message"])
            print("Processing completed successfully!")
        else:
            logger.error(result["message"])
            print(f"Error: {result['message']}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()