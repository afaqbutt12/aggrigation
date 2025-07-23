import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import json
from bson import ObjectId
from dataclasses import dataclass, asdict
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import traceback
import aiohttp
import ssl

load_dotenv()
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class rollup_yearly:
    company_code: str
    internal_code_id: str
    category_id: str
    site_code: str
    scope: str
    rollup_qty: str
    qty: str
    unit: str
    currency: str
    rollup_currency: str
    value: str
    description: str
    specific_period: str
    type: str
    type_year: str
    jurisdiction: List[Any]
    dimension: List[Any]
    company_name: str
    month: str
    semi_annual: str
    quarter: str
    createdAt: datetime
    updatedAt: datetime
    __v: int

class RollupGenerator:
    def __init__(self, mongodb_uri: str, database_name: str, region_url: str, auth_token: str):
        self.client = AsyncIOMotorClient(mongodb_uri)
        self.db = self.client[database_name]
        self.region_url = region_url
        self.auth_token = auth_token
        
        # Collections'
        self.cdata_yearly = self.db.cdata_yearly
        self.company_frameworks = self.db.company_frameworks
        self.company_codes = self.db.company_codes
        self.cdata = self.db.cdata
        self.codes = self.db.codes
        self.standards = self.db.standards
        self.standard_codes = self.db.standard_codes
        self.projects = self.db.projects
        self.categories = self.db.categories
        
        # Report collections
        self.health_check_reports = self.db.health_check_reports
        self.compliance_reports = self.db.compliance_reports
        self.portfolio_reports = self.db.portfolio_reports
        self.health_check_detail_reports = self.db.health_check_detail_reports

    async def fetch_company_sites(self, company_id: str) -> List[Dict]:
        """Fetch company sites from API"""
        try:
            url = f"{self.region_url}/api/companysites/company/{company_id}"
            headers = {
                'Authorization': self.auth_token,
                'Content-Type': 'application/json'
            }

            # Create SSL context that doesn't verify certificates (use with caution)
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            connector = aiohttp.TCPConnector(ssl=ssl_context)

            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    logger.error(f"Failed to fetch company sites for {company_id}. Status: {response.status}")
                    return []

        except Exception as e:
            logger.error(f"Error fetching company sites for {company_id}: {str(e)}")
            return []

    async def get_company_sites(self, company_id: str) -> List[Dict]:
        """Get company sites - now uses the API call"""
        return await self.fetch_company_sites(company_id)

    async def get_total_metrics(self, company_id: str, site_code: str) -> List[Dict]:
        """Get total metrics for company"""
        return await self.company_codes.find(
            {"company_id": company_id, "site_code": site_code}
        ).to_list(None)



    async def generate_rollup_table(self, company_id: str, year: int, period: str , sites_data=[],) -> PortfolioReport:
        """Generate roll up bottom to top in hirarical structure"""
        try:
            # Get total metrics
            total_ma = await self.get_total_metrics(company_id, "")
            
            # Set period values
            period_mapping = {
                "month": 12,
                "quarter": 4,
                "semi_annual": 2,
                "annual": 1
            }
            
            period_value = period_mapping.get(period, 1)
            
            achievement = []
            cur_categ_esg = []
            
            # Process each metric
            for data in total_ma:
                internal_code_id = str(data["internal_code_id"])
                # Get actual, baseline, target data
                actual = await self.cdata.find_one({
                    "internal_code_id": ObjectId(internal_code_id),
                    "type": "actual",
                    "qty": {"$nin": ["Y", "N"]},
                    "company_code": str(company_id),
                    "type_year": str(year),
                    "site_code": ""
                })
                
                baseline = await self.cdata.find_one({
                    "internal_code_id": ObjectId(internal_code_id),
                    "type": "baselinedata",
                    "qty": {"$nin": ["Y", "N"]},
                    "company_code": str(company_id),
                    "site_code": ""
                }, sort=[("createdAt", -1)])
                
                target = await self.cdata.find_one({
                    "internal_code_id": ObjectId(internal_code_id),
                    "type": "target",
                    "qty": {"$nin": ["Y", "N"]}, 
                    "company_code": str(company_id),
                    "site_code": ""
                }, sort=[("createdAt", -1)])
                
                # Get code information
                code = await self.codes.find_one({"_id": ObjectId(internal_code_id)})
                if baseline and target and code and baseline.get("qty") and target.get("qty"):
                    # Calculate achievement (simplified calculation)
                    baseline_qty = float(baseline.get("qty", 0))
                    target_qty = float(target.get("qty", 0))
                    actual_qty = float(actual.get("qty", 0)) if actual else 0
            # Get last entry
            last_entry = await self.get_last_entry(company_id)
            portfolio_sites = []
            
            for site_data in sites_data:
                logging.info("sites_data response:")
                total_sites_ma = await self.get_total_metrics(company_id, site_data.get("internal_site_code"))
                site_portfolio_data = {
                    "site_name": site_data.get("site_name", ""),
                    "site_type": site_data.get("site_type", ""),
                    "site_code": site_data.get("internal_site_code", ""),
                    "ownership": site_data.get("ownership", ""),
                    "esg_score": "0.0%",  # Calculate actual site ESG score
                    "categories": []  # Calculate site-specific category data
                }
                portfolio_sites = await self.build_hierarchy(sites_data, total_sites_ma, company_id, year, period_value)
                
            # portfolio_sites.append(get_company_sites_esg_score)
            
            return PortfolioReport(
                company_id=company_id,
                year=year,
                period=period,
                esg_score=f"{esg_score:.1f}%",
                site_type="Company",
                last_entry=last_entry,
                categories=company_data,
                sites=portfolio_sites,
                generated_at=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"Error generating portfolio report: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def save_reports_to_db(self, reports: Dict[str, Any]):
        """Save all reports to database"""
        logger.info("Saving reports to database")
        
        try:
            bulk_operations = []
            
            # Health Check Reports
            if "health_check" in reports:
                for report in reports["health_check"]:
                    bulk_operations.append(
                        UpdateOne(
                            {
                                "company_id": report.company_id,
                                "year": report.year
                            },
                            {
                                "$set": {
                                    **asdict(report),
                                    "sites": [asdict(site) for site in report.sites]
                                }
                            },
                            upsert=True
                        )
                    )
                
                if bulk_operations:
                    await self.health_check_reports.bulk_write(bulk_operations)
                    logger.info(f"Saved {len(bulk_operations)} health check reports")
                    bulk_operations = []
            
            # Compliance Reports
            if "compliance" in reports:
                for report in reports["compliance"]:
                    bulk_operations.append(
                        UpdateOne(
                            {
                                "company_id": report.company_id,
                                "year": report.year
                            },
                            {
                                "$set": {
                                    **asdict(report),
                                    "standards": {k: asdict(v) for k, v in report.standards.items()}
                                }
                            },
                            upsert=True
                        )
                    )
                
                if bulk_operations:
                    await self.compliance_reports.bulk_write(bulk_operations)
                    logger.info(f"Saved {len(bulk_operations)} compliance reports")
                    bulk_operations = []
            
            # Portfolio Reports
            if "portfolio" in reports:
                for report in reports["portfolio"]:
                    bulk_operations.append(
                        UpdateOne(
                            {
                                "company_id": report.company_id,
                                "year": report.year,
                                "period": report.period
                            },
                            {
                                "$set": {
                                    **asdict(report),
                                    "categories": [asdict(item) for item in report.categories]
                                }
                            },
                            upsert=True
                        )
                    )
                
                if bulk_operations:
                    await self.portfolio_reports.bulk_write(bulk_operations)
                    logger.info(f"Saved {len(bulk_operations)} portfolio reports")
            
        except Exception as e:
            logger.error(f"Error saving reports to database: {str(e)}")
            raise

    async def generate_all_reports(self, company_id: str, years: List[str] = None, 
                                 periods: List[str] = None) -> Dict[str, Any]:
        """Generate all reports for a company"""
        logger.info(f"Generating all reports for company {company_id}")
        if not periods:
            periods = ["annual", 
            # "semi_annual", "quarter", "month"
            ]
        
        if not years:
            current_year = await self.cdata_yearly.find_one(
                {
                    "company_code": str(company_id),
                    "is_forecast": False,
                },
                sort=[("reporting_year", -1)]
            )
            distinct_years = await self.cdata_yearly.aggregate([
                {"$match": {"company_code": str(company_id)}},
                {"$group": {"_id": "$reporting_year"}},
                {"$project": {"year": {"$toInt": "$_id"}, "_id": 0}}
            ]).to_list(length=None)
            # Extract years
            unique_years = [doc["year"] for doc in distinct_years]
            # Sort years in descending order
            sorted_years = sorted(unique_years, reverse=True)
            years = sorted_years
        
        reports = {
            "health_check": [],
            "compliance": [],
            "portfolio": []
        }
        
        try:
            # Get sites data for portfolio
            sites_data = await self.get_company_sites(company_id)
            sites = sites_data["data"]
            # Generate rollup
            for period in periods:
                try:
                    portfolio_report = await self.generate_rollup_table(company_id, 2023, period, sites)
                    reports["portfolio"].append(portfolio_report)
                    logger.info(f"Generated Rollup for {company_id}, year {2023}, period {period}")
                except Exception as e:
                    logger.error(f"Failed to generate portfolio report for {company_id}, year {2023}, period {period}: {str(e)}")
        
            # Save all reports to database
            await self.save_reports_to_db(reports)
            
            return reports
            
        except Exception as e:
            logger.error(f"Error generating reports for company {company_id}: {str(e)}")
            raise

async def main():
    """Main function to run the report generator"""
    # Configuration
    MONGODB_URI = os.getenv("MONGO_URI")
    DATABASE_NAME = os.getenv("MONGO_DB_NAME")
    REGION_URL = os.getenv("REGION_API_URL")
    AUTH_TOKEN = os.getenv("TOKEN")
    
    # Company IDs to process
    COMPANY_IDS = [
        "690",
        # Add more company IDs here
    ]
    
    # Initialize generator
    generator = ReportGenerator(MONGODB_URI, DATABASE_NAME, REGION_URL, AUTH_TOKEN)
    
    try:
        # Generate reports for all companies
        results = await generator.generate_reports_for_companies(COMPANY_IDS)
        
        logger.info("Report generation summary:")
        logger.info(f"Total companies: {results['total_companies']}")
        logger.info(f"Successful: {results['successful']}")
        logger.info(f"Failed: {results['failed']}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        logger.error(traceback.format_exc())
    
    finally:
        await generator.close()

if __name__ == "__main__":
    # Run the script
    asyncio.run(main())
