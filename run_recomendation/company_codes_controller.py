from pymongo import MongoClient
from bson import ObjectId
from config import Config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

    
class CompanyCodesController:
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
        self._connect_to_db()
    # MongoDB Configuration
    MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    DATABASE_NAME = os.getenv('DATABASE_NAME', 'ensogov')
    COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'company_codes')
    
    # Redis Configuration (for Celery)
    REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    
    # Flask Configuration
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key')
    DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Celery Configuration
    CELERY_BROKER_URL = REDIS_URL
    CELERY_RESULT_BACKEND = REDIS_URL
    
    def _connect_to_db(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(MONGODB_URI)
            self.db = self.client[DATABASE_NAME]
            self.collection = self.db['company_codes']
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise
    
    def merge_company_codes(self):
        """
        Merge company codes by grouping documents by company_id and internal_code_id,
        combining their site_code values into arrays.
        """
        try:
            return self._extracted_from_merge_company_codes_7()
        except Exception as e:
            error_msg = f"Error during merge process: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }

    # TODO Rename this here and in `merge_company_codes`
    def _extracted_from_merge_company_codes_7(self):
        logger.info("Starting company codes merge process...")

        # Step 1: Aggregation pipeline to group documents
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "company_id": "$company_id",
                        "internal_code_id": "$internal_code_id"
                    },
                    "category_id": {"$first": "$category_id"},
                    "isChecked": {"$first": "$isChecked"},
                    "createdAt": {"$first": "$createdAt"},
                    "updatedAt": {"$first": "$updatedAt"},
                    "__v": {"$first": "$__v"},
                    "site_code": {"$push": "$site_code"},  # Collect all site_codes into an array
                    "doc_count": {"$sum": 1}  # Count documents for each group
                }
            }
        ]

        # Perform the aggregation
        grouped_documents = list(self.collection.aggregate(pipeline))
        logger.info(f"Found {len(grouped_documents)} grouped documents to process")

        # Step 2: Process each grouped document
        processed_count = 0
        for doc in grouped_documents:
            company_id = doc["_id"]["company_id"]
            internal_code_id = doc["_id"]["internal_code_id"]

                # Only process if there are multiple documents to merge
            if doc["doc_count"] > 1:
                    # Combine all site_code values into one array
                combined_site_codes = list(doc["site_code"])

                # Prepare the updated document
                updated_document = {
                    "company_id": company_id,
                    "internal_code_id": internal_code_id,
                    "category_id": doc["category_id"],
                    "isChecked": doc["isChecked"],
                    "createdAt": doc["createdAt"],
                    "updatedAt": doc["updatedAt"],
                    "__v": doc["__v"],
                    "site_code": combined_site_codes
                }

                # Step 3: Delete existing documents with same company_id and internal_code_id
                delete_result = self.collection.delete_many({
                    "company_id": company_id,
                    "internal_code_id": internal_code_id
                })

                # Step 4: Insert the merged document
                insert_result = self.collection.insert_one(updated_document)

                processed_count += 1
                logger.info(f"Processed group {processed_count}: company_id={company_id}, "
                          f"internal_code_id={internal_code_id}, "
                          f"deleted={delete_result.deleted_count}, "
                          f"inserted=1")

        result = {
            "status": "success",
            "message": "Company codes merge completed successfully",
            "groups_processed": processed_count,
            "total_groups_found": len(grouped_documents)
        }

        logger.info(f"Merge process completed. Processed {processed_count} groups out of {len(grouped_documents)} total groups")
        return result
    
    def get_collection_stats(self):
        """Get basic statistics about the collection"""
        try:
            total_docs = self.collection.count_documents({})
            
            # Count unique combinations of company_id and internal_code_id
            unique_combinations = len(list(self.collection.aggregate([
                {
                    "$group": {
                        "_id": {
                            "company_id": "$company_id",
                            "internal_code_id": "$internal_code_id"
                        }
                    }
                }
            ])))
            
            return {
                "status": "success",
                "total_documents": total_docs,
                "unique_combinations": unique_combinations,
                "potential_duplicates": total_docs - unique_combinations
            }
        except Exception as e:
            logger.error(f"Error getting collection stats: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def close_connection(self):
        """Close the MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")