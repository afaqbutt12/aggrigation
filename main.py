from script_functions import fetch_company_data, process_monthly_data, process_bi_annual_data, process_yearly_data, delete_monthly_data
from RegionAPI import fetch_company_data_safe as fetch_company_data, fetch_all_company
from data_quarterly_process import process_quarterly_data
from data_BiAnnual_process import process_BiAnnual_data
from data_monthly_process import process_monthly_data

from data_yearly_process import process_yearly_data
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import date, datetime
import argparse
import logging
import sys
import os
import db_connection
from typing import Optional, Dict, List, Any
import traceback

load_dotenv()

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('company_processing.log')
    ]
)
logger = logging.getLogger(__name__)

class CompanyDataController:
    def __init__(self):
        """Initialize the controller with database connection"""
        try:
            self.connection = db_connection.connect_to_database()
            if self.connection is not None:
                self.company_code_collection = self.connection["company_codes"]
                logger.info("Database connection established successfully")
            else:
                raise Exception("Database connection is not available.")
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {str(e)}")
            raise

    def process_company_data(self, company_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Process company data for a specific company or all companies
        
        Args:
            company_id: Optional company ID to process. If None, processes all companies.
            
        Returns:
            Dict containing success status, message, and processed data
        """
        start_time = datetime.now()
        logger.info(f"Starting company data processing at {start_time}")
        
        try:
            # Get the current date and calculate year range
            current_date = date.today()
            current_year = int(current_date.strftime("%Y"))
            year = current_year - 6
            
            logger.info(f"Processing data from year {year} to {current_year}")
            logger.info(f"Target company_id: {company_id}")
            
            # Fetch companies to process
            companies = self._get_companies_to_process(company_id)
            
            if not companies:
                return {
                    "success": False,
                    "message": f"No companies found for processing. Company ID: {company_id}",
                    "data": None
                }
            
            logger.info(f"Processing {len(companies)} companies")
            
            # Process companies
            processed_companies = self._process_companies(companies, year)
            
            # Calculate processing time
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            success_count = len([c for c in processed_companies if c.get('status') == 'success'])
            error_count = len(processed_companies) - success_count
            
            return {
                "success": True,
                "message": f"Processing completed. Success: {success_count}, Errors: {error_count}",
                "data": {
                    "processed_companies": processed_companies,
                    "summary": {
                        "total_companies": len(processed_companies),
                        "successful": success_count,
                        "failed": error_count,
                        "processing_time_seconds": processing_time,
                        "start_time": start_time.isoformat(),
                        "end_time": end_time.isoformat()
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"Error processing company data: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                "success": False,
                "message": f"Error processing company data: {str(e)}",
                "data": None
            }

    def _get_companies_to_process(self, company_id: Optional[int]) -> List[Dict]:
        """Get companies to process based on company_id parameter"""
        try:
            all_companies = fetch_all_company()
            
            if not all_companies:
                logger.error("Failed to fetch companies from API")
                return []
                
            logger.info(f"Fetched {len(all_companies)} total companies from API")
            
            if company_id is not None:
                # Process specific company
                company = next((c for c in all_companies if c['id'] == company_id), None)
                if company:
                    logger.info(f"Found company with ID {company_id}: {company.get('company_name', 'Unknown')}")
                    return [company]
                else:
                    logger.warning(f"Company with ID {company_id} not found")
                    return []
            else:
                # Process all companies
                logger.info("Processing all companies")
                return all_companies
                
        except Exception as e:
            logger.error(f"Error fetching companies: {str(e)}")
            return []

    def _process_companies(self, companies: List[Dict], year: int) -> List[Dict]:
        """Process multiple companies"""
        processed_companies = []
        
        for i, company in enumerate(companies, 1):
            try:
                logger.info(f"Processing company {i}/{len(companies)}: ID {company.get('id')}")
                result = self._process_single_company(company, year)
                processed_companies.append(result)
                
            except Exception as e:
                logger.error(f"Error processing company {company.get('id', 'unknown')}: {str(e)}")
                processed_companies.append({
                    "company_id": company.get('id', 'unknown'),
                    "company_name": company.get('name', 'Unknown'),
                    "status": "error",
                    "error": str(e),
                    "traceback": traceback.format_exc()
                })
        
        return processed_companies

    def _process_single_company(self, company: Dict, year: int) -> Dict:
        """Process a single company's data"""
        company_start_time = datetime.now()
        
        try:
            company_id = str(company['id'])
            company_name = company.get('company_name', 'Unknown')
            
            logger.info(f"Processing company: {company_id} - {company_name}")
            
            # Get start month
            month_data = fetch_company_data(company_id)
            start_month = str(month_data[0]) if month_data else 'January'
            logger.info(f"Start month for company {month_data}")
            
            # Get company codes from database
            company_codes = list(self.company_code_collection.find({"company_id": str(company_id)}))
            logger.info(f"Company Codes: {company_codes}")
            
            if not company_codes:
                logger.warning(f"No company codes found for company {company_id}")
                return {
                    "company_id": company_id,
                    "company_name": company_name,
                    "status": "warning",
                    "message": "No company codes found",
                    "processed_codes": []
                }
            
            # Get and validate reporting frequency
            reporting_frequencies = self._get_reporting_frequencies(company)
            
            if not reporting_frequencies:
                logger.warning(f"No valid reporting frequencies for company {company_id}")
                return {
                    "company_id": company_id,
                    "company_name": company_name,
                    "status": "warning",
                    "message": "No valid reporting frequencies found",
                    "processed_codes": []
                }
            
            # Process each company code
            processed_codes = self._process_company_codes(
                company, company_codes, reporting_frequencies, year, start_month
            )
            
            # Calculate processing time
            processing_time = (datetime.now() - company_start_time).total_seconds()
            
            return {
                "company_id": company_id,
                "company_name": company_name,
                "processed_codes": processed_codes,
                "reporting_frequencies": reporting_frequencies,
                "processing_time_seconds": processing_time,
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error processing company {company.get('id', 'unknown')}: {str(e)}")
            return {
                "company_id": company.get('id', 'unknown'),
                "company_name": company.get('name', 'Unknown'),
                "status": "error",
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def _get_reporting_frequencies(self, company: Dict) -> List[str]:
        """Extract and validate reporting frequencies"""
        reporting_frequency = company.get("reporting_frequency")
        
        if not reporting_frequency:
            return []
        
        # Split and clean frequencies
        frequencies = [freq.strip() for freq in reporting_frequency.split(',')]
        
        # Validate frequencies
        valid_frequencies = ['month', 'quater', 'semi_annual', 'annual']
        validated_frequencies = []
        
        for freq in frequencies:
            if freq in valid_frequencies:
                validated_frequencies.append(freq)
            else:
                logger.warning(f"Invalid reporting frequency: {freq}")
        
        return validated_frequencies

    def _process_company_codes(self, company: Dict, company_codes: List[Dict], reporting_frequencies: List[str], year: int, start_month: str) -> List[Dict]:
        """Process all company codes for a company"""
        processed_codes = []
        company_id = str(company['id'])
        
        for company_code in company_codes:
            try:
                internal_code_id = company_code['internal_code_id']
                logger.info(f"Processing code {internal_code_id} for company {company_id}")
                
                code_results = []
                
                # Process each reporting frequency
                for freq in reporting_frequencies:
                    try:
                        is_reporting_next = False if len(reporting_frequencies) == 1 else True
                        result = self._process_frequency(
                            company, company_id, internal_code_id, freq, year, start_month, is_reporting_next
                        )
                        code_results.append(result)
                        
                    except Exception as e:
                        logger.error(f"Error processing frequency {freq} for code {internal_code_id}: {str(e)}")
                        code_results.append({
                            "frequency": freq,
                            "status": "error",
                            "error": str(e)
                        })
                
                processed_codes.append({
                    "internal_code_id": internal_code_id,
                    "frequency_results": code_results,
                    "status": "processed"
                })
                
            except Exception as e:
                logger.error(f"Error processing company code {company_code}: {str(e)}")
                processed_codes.append({
                    "internal_code_id": company_code.get('internal_code_id', 'unknown'),
                    "status": "error",
                    "error": str(e)
                })
        
        return processed_codes

    def _process_frequency(self, company: Dict, company_id: str, internal_code_id: str, frequency: str, year: int, start_month: str, is_reporting_next: bool) -> Dict:
        """Process a specific frequency for a company code"""
        try:
            logger.info(f"Processing {frequency} data for company {company_id}, code {internal_code_id}")
            # Process main company data
            if frequency == 'month':
                logger.info(f"Processing code {start_month}, code {year}")
                self._process_monthly(company_id, internal_code_id, year, start_month, company)
            elif frequency == 'quater':
                self._process_quarterly(company_id, internal_code_id, year, start_month, company)
            elif frequency == 'semi_annual':
                self._process_bi_annual(company_id, internal_code_id, year, start_month, company)
            elif frequency == 'annual':
                self._process_yearly(company_id, internal_code_id, year, start_month, company, is_reporting_next)
            
            return {
                "frequency": frequency,
                "status": "success",
                "sites_processed": len(company.get('company_sites', []))
            }
            
        except Exception as e:
            logger.error(f"Error processing {frequency} for company {company_id}: {str(e)}")
            return {
                "frequency": frequency,
                "status": "error",
                "error": str(e)
            }
    
    def _process_monthly(self, company_id: str, internal_code_id: str, year: int, start_month: str, company: Dict):
        """Process monthly data for a company"""
        # Process main company data
        if all([company_id, internal_code_id, year, start_month]):
            delete_monthly_data(company_id, start_month, year, internal_code_id, "")
            process_monthly_data(company_id, internal_code_id, year, start_month, site_code="")
        
        # Process site data
        for site_code in company.get('company_sites', []):
            if (all([company_id, internal_code_id, year, start_month]) and 
                site_code.get("internal_site_code")):
                delete_monthly_data(company_id, start_month, year, internal_code_id, site_code["internal_site_code"])
                process_monthly_data(company_id, internal_code_id, year, start_month, site_code["internal_site_code"])
    
    def _process_quarterly(self, company_id: str, internal_code_id: str, year: int, start_month: str, company: Dict):
        """Process quarterly data for a company"""
        # Process main company data
        if all([company_id, internal_code_id, year, start_month]):
            process_quarterly_data(company_id, internal_code_id, year, start_month, site_code="")
        
        # Process site data
        for site_code in company.get('company_sites', []):
            if (all([company_id, internal_code_id, year, start_month]) and 
                site_code.get("internal_site_code")):
                process_quarterly_data(company_id, internal_code_id, year, start_month, site_code["internal_site_code"])
    
    def _process_bi_annual(self, company_id: str, internal_code_id: str, year: int, 
                          start_month: str, company: Dict):
        """Process bi-annual data for a company"""
        # Process main company data
        if all([company_id, internal_code_id, year, start_month]):
            process_BiAnnual_data(company_id, internal_code_id, year, start_month, site_code="")
        
        # Process site data
        for site_code in company.get('company_sites', []):
            if (all([company_id, internal_code_id, year, start_month]) and 
                site_code.get("internal_site_code")):
                process_BiAnnual_data(company_id, internal_code_id, year, start_month, 
                                    site_code["internal_site_code"])
    
    def _process_yearly(self, company_id: str, internal_code_id: str, year: int, start_month: str, company: Dict, is_reporting_next: bool):
        """Process yearly data for a company"""
        # Process main company data
        if all([company_id, internal_code_id, year, start_month]):
            reporting_month = start_month if is_reporting_next else "January"
            process_yearly_data(company_id, internal_code_id, year, reporting_month, site_code="")
        
        # Process site data
        for site_code in company.get('company_sites', []):
            if (all([company_id, internal_code_id, year, start_month]) and 
                site_code.get("internal_site_code")):
                process_yearly_data(company_id, internal_code_id, year, reporting_month, site_code["internal_site_code"])

def main(company_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Main function for command line execution and programmatic use
    
    Args:
        company_id: Optional company ID to process
        
    Returns:
        Dict containing processing results
    """
    logger.info("Starting main processing function")
    
    # Only parse command line arguments if company_id is not provided directly
    if company_id is None:
        parser = argparse.ArgumentParser(description='Process company data.')
        parser.add_argument('--company_id', type=int, help='Specific company ID to process')
        args = parser.parse_args()
        company_id = args.company_id
    
    try:
        controller = CompanyDataController()
        result = controller.process_company_data(company_id=company_id)
        
        if result["success"]:
            logger.info(result["message"])
            print("Processing completed successfully!")
            
            # Print summary if available
            if result.get("data") and result["data"].get("summary"):
                summary = result["data"]["summary"]
                print(f"Summary: {summary['successful']}/{summary['total_companies']} companies processed successfully")
                print(f"Processing time: {summary['processing_time_seconds']:.2f} seconds")
            
            return result
        else:
            logger.error(result["message"])
            print(f"Error: {result['message']}")
            if __name__ == "__main__":
                sys.exit(1)
            else:
                return result
                
    except Exception as e:
        error_msg = f"Fatal error: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        print(error_msg)
        
        if __name__ == "__main__":
            sys.exit(1)
        else:
            raise

if __name__ == "__main__":
    main()
