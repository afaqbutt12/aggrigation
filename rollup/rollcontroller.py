import json
import copy
from datetime import datetime
from typing import Dict, List, Any, Optional

class SiteDataRollup:
    def __init__(self):
        """Initialize the controller with database connection"""
        try:
            self.connection = db_connection.connect_to_database()
            if self.connection is not None:
                self.company_code_collection = self.connection["company_codes"]
                self.new_rollup_table = self.connection["rollup_yearly"]
                self.processed_combinations = set()  # Track processed (site_code, year, internal_code_id)
                logger.info("Database connection established successfully")
            else:
# sourcery skip: raise-specific-error
                raise Exception("Database connection is not available.")
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {str(e)}")
            raise
    
    def find_latest_cdata_for_site(self, cdata_list: List[Dict], site_code: str, year: int, internal_code_id: str) -> Optional[Dict]:
        """
        Find the latest updated cdata record for a specific site, year, and internal_code_id combination
        Returns only ONE record - the most recently updated one
        """
        matching_records = []
        
        for cdata in cdata_list:
            # Match site_code, year, and internal_code_id
            cdata_year = cdata.get('type_year') or cdata.get('reporting_year')
            cdata_internal_id = str(cdata.get('internal_code_id', {}).get('$oid', ''))
            
            if (cdata.get('site_code') == site_code and 
                cdata_year == year and 
                cdata_internal_id == internal_code_id):
                matching_records.append(cdata)
        
        if not matching_records:
            return None
        
        # If only one record, return it
        if len(matching_records) == 1:
            return matching_records[0]
        
        # Multiple records found - pick the latest updated one
        latest_record = matching_records[0]
        latest_date = self.parse_date(latest_record.get('created_at') or latest_record.get('updated_at'))
        
        for record in matching_records[1:]:
            record_date = self.parse_date(record.get('created_at') or record.get('updated_at'))
            if record_date and (not latest_date or record_date > latest_date):
                latest_record = record
                latest_date = record_date
        
        print(f"    Found {len(matching_records)} records for {site_code}, using latest updated record")
        return latest_record
    
    def parse_date(self, date_field) -> Optional[datetime]:
        """
        Parse date from various formats
        """
        if not date_field:
            return None
        
        try:
            if isinstance(date_field, dict) and '$date' in date_field:
                date_str = date_field['$date']
            elif isinstance(date_field, str):
                date_str = date_field
            else:
                return None
            
            # Try to parse ISO format
            if 'T' in date_str:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                return datetime.fromisoformat(date_str)
        except:
            return None
    
    def create_rollup_record(self, cdata: Dict, site: Dict, rollup_qty: float = 0, rollup_value: float = 0) -> Dict:
        """
        Create a new record for the rollup table
        """
        new_record = copy.deepcopy(cdata)
        
        # Add rollup fields
        new_record['rollup_qty'] = rollup_qty
        new_record['rollup_value'] = rollup_value
        new_record['site_ownership'] = site.get('ownership', 100)
        new_record['site_id'] = site['id']
        new_record['rollup_processed_at'] = {'$date': datetime.now().isoformat()}
        new_record['rollup_processed'] = True
        
        return new_record
    
    def rollup_recursive(self, site: Dict, cdata_list: List[Dict], year: int, internal_code_id: str, level: int = 0) -> Dict:
        """
        Efficient recursive function that processes sites and cdata in one pass
        Returns: {
            'own_contribution': {'qty': x, 'value': y},  # This site's contribution to parent
            'total_rollup': {'qty': x, 'value': y}       # Total rollup from all children
        }
        """
        site_cdata = self.find_latest_cdata_for_site(cdata_list, site_code, year, internal_code_id)
        indent = "  " * level
        site_code = site['internal_site_code']
        parent_qty= 0
        parent_value= 0
        if(site_cdata):   
            parent_qty= site_cdata['qty']
            parent_value= site_cdata['value']
        
        print(f"{indent}Processing site: {site_code} (Level {level})")
        
        # Initialize return values
        result = {
            'own_contribution': {'qty': 0, 'value': 0},
            'total_rollup': {'qty': parent_qty, 'value': parent_value}
        }
        
        # Step 1: Process all children first (post-order traversal)
        total_child_rollup_qty = 0
        total_child_rollup_value = 0
        
        if 'sites' in site and site['sites']:
            print(f"{indent}  Processing {len(site['sites'])} child sites...")
            for child_site in site['sites']:
                child_result = self.rollup_recursive(child_site, cdata_list, year, internal_code_id, level + 1)
                
                # Accumulate child contributions
                total_child_rollup_qty += child_result['own_contribution']['qty']
                total_child_rollup_value += child_result['own_contribution']['value']
                
                print(f"{indent}    Child {child_site['internal_site_code']} contributed: qty={child_result['own_contribution']['qty']:.2f}, value={child_result['own_contribution']['value']:.2f}")
        
        # Step 2: Check if this site has its own cdata
        combination_key = (site_code, year, internal_code_id)
        
        if combination_key not in self.processed_combinations:
            if site_cdata:
                print(f"{indent}  Found cdata for site {site_code}")
                
                # Get original values
                original_qty = float(site_cdata.get('qty', 0) or 0)
                original_value = float(site_cdata.get('value', 0) or 0)
                
                # Create rollup record with original data + rollup from children
                rollup_record = self.create_rollup_record(
                    site_cdata, 
                    site, 
                    total_child_rollup_qty, 
                    total_child_rollup_value
                )
                
                self.new_rollup_table.append(rollup_record)
                self.processed_combinations.add(combination_key)
                
                print(f"{indent}    Added record: original qty={original_qty}, value={original_value}")
                print(f"{indent}    Rollup from children: qty={total_child_rollup_qty:.2f}, value={total_child_rollup_value:.2f}")
                
                # Calculate this site's contribution to its parent
                # Contribution = (original + rollup from children) * ownership
                ownership_factor = site.get('ownership', 100) / 100.0
                
                total_qty_contribution = (original_qty + total_child_rollup_qty) * ownership_factor
                total_value_contribution = (original_value + total_child_rollup_value) * ownership_factor
                
                result['own_contribution'] = {
                    'qty': total_qty_contribution,
                    'value': total_value_contribution
                }
                
                print(f"{indent}    Contribution to parent: qty={total_qty_contribution:.2f}, value={total_value_contribution:.2f} (ownership: {site.get('ownership', 100)}%)")
                
            else:
                print(f"{indent}  No cdata found for site {site_code}")
                
                # No own cdata, just pass through children's contributions with ownership applied
                if total_child_rollup_qty > 0 or total_child_rollup_value > 0:
                    ownership_factor = site.get('ownership', 100) / 100.0
                    
                    result['own_contribution'] = {
                        'qty': total_child_rollup_qty * ownership_factor,
                        'value': total_child_rollup_value * ownership_factor
                    }
                    
                    print(f"{indent}    Passing through children's contributions with ownership: qty={result['own_contribution']['qty']:.2f}, value={result['own_contribution']['value']:.2f}")
        else:
            print(f"{indent}  Site {site_code} already processed for this combination")
        
        # Store total rollup for reference
        result['total_rollup'] = {
            'qty': total_child_rollup_qty,
            'value': total_child_rollup_value
        }
        
        return result
    
    def process_rollup(self, site_data: Dict, cdata_list: List[Dict], year: int, internal_code_id: str):
        """
        Main entry point for rollup processing
        """
        print(f"Starting efficient recursive rollup for year {year}, internal_code_id {internal_code_id}")
        print("="*80)
        
        # Reset state
        self.new_rollup_table = []
        self.processed_combinations = set()
        
        # Start recursive processing from root
        root_result = self.rollup_recursive(site_data, cdata_list, year, internal_code_id)
        
        print("\n" + "="*80)
        print(f"Rollup completed! Created {len(self.new_rollup_table)} records")
        print(f"Root site total contribution: qty={root_result['own_contribution']['qty']:.2f}, value={root_result['own_contribution']['value']:.2f}")
    
    def get_rollup_table(self) -> List[Dict]:
        """
        Get the new rollup table
        """
        return self.new_rollup_table
    
    def print_rollup_table_summary(self):
        """
        Print detailed summary of the rollup table
        """
        print("\n" + "="*80)
        print("NEW ROLLUP TABLE SUMMARY")
        print("="*80)
        
        for i, record in enumerate(self.new_rollup_table, 1):
            print(f"\nRecord {i}:")
            print(f"  Site Code: {record.get('site_code', 'N/A')}")
            print(f"  Code: {record.get('code', 'N/A')} - {record.get('code_name', 'N/A')}")
            print(f"  Year: {record.get('type_year', record.get('reporting_year', 'N/A'))}")
            print(f"  Original Qty: {record.get('qty', 0)}")
            print(f"  Original Value: {record.get('value', 0)}")
            print(f"  Rollup Qty (from children): {record.get('rollup_qty', 0):.2f}")
            print(f"  Rollup Value (from children): {record.get('rollup_value', 0):.2f}")
            print(f"  Site Ownership: {record.get('site_ownership', 100)}%")
            print(f"  Total Effective Qty: {float(record.get('qty', 0) or 0) + record.get('rollup_qty', 0):.2f}")
            print(f"  Total Effective Value: {float(record.get('value', 0) or 0) + record.get('rollup_value', 0):.2f}")
    
    def export_to_database_format(self) -> List[Dict]:
        """
        Export rollup table in format ready for database insertion
        """
        return [
            {
                'site_code': record.get('site_code'),
                'site_id': record.get('site_id'),
                'company_code': record.get('company_code'),
                'year': record.get('type_year', record.get('reporting_year')),
                'internal_code_id': record.get('internal_code_id'),
                'code': record.get('code'),
                'code_name': record.get('code_name'),
                'original_qty': record.get('qty'),
                'original_value': record.get('value'),
                'rollup_qty': record.get('rollup_qty'),
                'rollup_value': record.get('rollup_value'),
                'total_qty': float(record.get('qty', 0) or 0) + record.get('rollup_qty', 0),
                'total_value': float(record.get('value', 0) or 0) + record.get('rollup_value', 0),
                'ownership_percentage': record.get('site_ownership'),
                'currency': record.get('currency'),
                'unit': record.get('unit'),
                'processed_at': record.get('rollup_processed_at'),
                'is_rollup_record': True
            }
            for record in self.new_rollup_table
        ]
    
    def _process_frequency(self, company: Dict, company_id: str, internal_code_id: str, frequency: str, year: int, start_month: str, is_reporting_next: bool) -> Dict:
        """Process a specific frequency for a company code"""
        try:
            logger.info(f"Processing {frequency} data for company {company_id}, code {internal_code_id}")
            # Process main company data
            if frequency == 'month':
                logger.info(f"Processing code {start_month}, code {year}")
                # Process the rollup
                self.process_rollup(
                    site_data, 
                    sample_cdata, 
                    year, 
                    internal_code_id,
                    company_id
                )
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
                        is_reporting_next = False if len(reporting_frequencies) == 4 else True
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