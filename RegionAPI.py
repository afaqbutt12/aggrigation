import requests
import logging
import sys
import pymongo
import os
import time
from dotenv import load_dotenv
from json.decoder import JSONDecodeError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Tuple, List, Dict, Any
import urllib3

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# Check urllib3 version to use correct parameter names
URLLIB3_VERSION = urllib3.__version__.split('.')
USE_ALLOWED_METHODS = int(URLLIB3_VERSION[0]) >= 2

class APIClient:
    """Enhanced API client with retry logic and better error handling"""
    
    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
        
        # Configure retry strategy with version-specific parameters
        retry_kwargs = {
            'total': max_retries,
            'status_forcelist': [429, 500, 502, 503, 504],
            'backoff_factor': 1
        }
        
        # Use the correct parameter name based on urllib3 version
        if USE_ALLOWED_METHODS:
            retry_kwargs['allowed_methods'] = ["HEAD", "GET", "OPTIONS"]
        else:
            retry_kwargs['method_whitelist'] = ["HEAD", "GET", "OPTIONS"]
        
        retry_strategy = Retry(**retry_kwargs)
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'User-Agent': 'CompanyDataClient/1.0',
            'Accept': 'application/json',
            'Connection': 'keep-alive'
        })
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make GET request with proper error handling"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.info(f"Making request to: {url}")
            
            response = self.session.get(
                url, 
                params=params, 
                timeout=self.timeout
            )
            
            logger.info(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    return data
                except JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response from {url}: {str(e)}")
                    logger.error(f"Response content: {response.text[:500]}...")
                    return None
            else:
                logger.error(f"API request failed. Status: {response.status_code}, URL: {url}")
                logger.error(f"Response: {response.text[:500]}...")
                return None
                
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error for {url}: {str(e)}")
            return None
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout error for {url}: {str(e)}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {url}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {url}: {str(e)}")
            return None

# Initialize API client
def get_api_client() -> Optional[APIClient]:
    """Get configured API client"""
    base_url = os.getenv("COMPANY_DATA_URL")
    if not base_url:
        logger.error("COMPANY_DATA_URL environment variable not set")
        return None
    
    return APIClient(base_url)

def fetch_company_data(company_code: int) -> List:
    """
    Fetch company data for a specific company code
    
    Args:
        company_code: The company code to fetch data for
        
    Returns:
        List containing start month or ['January'] if error
    """
    try:
        api_client = get_api_client()
        if not api_client:
            logger.error("Failed to create API client")
            return ['January']
        
        logger.info(f"Fetching company data for company_code: {company_code}")
        
        data = api_client.get(f"/company/data/{company_code}")
        
        if not data:
            logger.warning(f"No data received for company {company_code}")
            return ['January']
        
        # Validate response structure
        if not isinstance(data, dict) or "data" not in data:
            logger.error(f"Invalid response structure for company {company_code}: {data}")
            return ['January']
        
        company_data = data.get("data", {}).get("company", {})
        
        if not company_data:
            logger.warning(f"No company data found for company {company_code}")
            return ['January']
        
        # Extract start month
        start_month = company_data.get("month")
        if not start_month:
            logger.warning(f"No start month found for company {company_code}, using January")
            return ['January']
        elif isinstance(start_month, str):
            return [start_month]
        elif isinstance(start_month, list):
            return start_month
        else:
            logger.warning(f"Unexpected start_month type: {type(start_month)}")
            return ['January']
        
    except Exception as e:
        logger.error(f"Exception occurred while fetching company data for {company_code}: {str(e)}")
        return ['January']

def fetch_all_company() -> List[Dict]:
    """
    Fetch all company data
    
    Returns:
        List of company dictionaries (empty list if error)
    """
    try:
        api_client = get_api_client()
        if not api_client:
            logger.error("Failed to create API client")
            return []
        
        logger.info("Fetching all company data")
        
        data = api_client.get("/company/data")
        
        if not data:
            logger.warning("No data received when fetching all companies")
            return []
        
        # Validate response structure
        if not isinstance(data, dict):
            logger.error(f"Invalid response structure when fetching all companies: {type(data)}")
            return []
        
        companies = data.get("companies")
        
        if not companies:
            logger.warning("No companies found in response")
            return []
        
        if not isinstance(companies, list):
            logger.error(f"Companies data is not a list: {type(companies)}")
            return []
        
        logger.info(f"Successfully fetched {len(companies)} companies")
        
        # Validate each company has required fields
        valid_companies = []
        for i, company in enumerate(companies):
            if not isinstance(company, dict):
                logger.warning(f"Company at index {i} is not a dictionary: {type(company)}")
                continue
            
            if 'id' not in company:
                logger.warning(f"Company at index {i} missing 'id' field: {company}")
                continue
            
            valid_companies.append(company)
        
        logger.info(f"Validated {len(valid_companies)} companies out of {len(companies)}")
        return valid_companies
        
    except Exception as e:
        logger.error(f"Exception occurred while fetching all companies: {str(e)}")
        # Always return an empty list instead of None
        return []

# Fallback data for testing/development
def get_fallback_companies() -> List[Dict[str, Any]]:
    """
    Provide fallback company data when API is unavailable
    
    Returns:
        List of sample company data
    """
    logger.info("Using fallback company data")
    return [
        {
            "id": 707,
            "name": "Sample Company 1",
            "reporting_frequency": "month,quater",
            "company_sites": [
                {"internal_site_code": "SITE001"},
                {"internal_site_code": "SITE002"}
            ]
        },
        {
            "id": 708,
            "name": "Sample Company 2", 
            "reporting_frequency": "annual",
            "company_sites": []
        }
    ]

def fetch_all_company_with_fallback() -> List[Dict]:
    """
    Fetch all companies with fallback to sample data
    
    Returns:
        List of company data (real or fallback)
    """
    companies = fetch_all_company()
    
    if not companies:
        logger.warning("API unavailable, using fallback data")
        return get_fallback_companies()
    
    return companies

def fetch_company_with_retry(company_id: int) -> List:
    """
    Fetch company data with retry logic
    
    Args:
        company_id: The company ID to fetch
        
    Returns:
        List containing start month or ['January'] if all attempts fail
    """
    max_attempts = 3
    delay = 1.0
    
    for attempt in range(max_attempts):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_attempts} to fetch company {company_id}")
            
            result = fetch_company_data(company_id)
            
            if result:
                return result
            
            if attempt < max_attempts - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
                
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed for company {company_id}: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(delay)
                delay *= 2
    
    logger.error(f"All {max_attempts} attempts failed for company {company_id}")
    return ['January']  # Default fallback

# For backward compatibility
def fetch_company_data_safe(company_id: int) -> List:
    """Safe wrapper for fetch_company_data that always returns a list"""
    result = fetch_company_with_retry(company_id)
    return result if result else ['January']

def fetch_all_company_safe() -> List[Dict]:
    """Safe wrapper for fetch_all_company that always returns a list"""
    return fetch_all_company_with_fallback()

if __name__ == "__main__":
    # Test the API functions
    print("Testing API connection...")
    
    # Test fetching all companies
    companies = fetch_all_company_safe()
    print(f"✓ Fetched {len(companies)} companies")
    
    if companies:
        # Test fetching specific company
        first_company_id = companies[0].get('id')
        if first_company_id:
            start_month = fetch_company_data_safe(first_company_id)
            print(f"✓ Fetched data for company {first_company_id}: {start_month}")
