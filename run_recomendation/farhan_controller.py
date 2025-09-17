from pymongo import MongoClient
from bson import ObjectId
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()  
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
DATABASE_NAME = os.getenv('MONGODB_DB_NAME', 'ensogove')
# Connect to MongoDB
client = MongoClient(MONGODB_URI)  # Replace with your MongoDB URI
db = client[DATABASE_NAME]  # Replace with your database name
collection = db['company_codes']  # Replace with your collection name
print("Connected to Mongo DB")


# Step 1: Find all documents grouped by company_id and internal_code_id
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
            "site_code": {"$push": "$site_code"}  # Collect all site_codes into an array
        }
    }
]

# Perform the aggregation to group documents by company_id and internal_code_id
grouped_documents = collection.aggregate(pipeline)
print("Collection Found")

# Step 2: Update the collection with the grouped data
for doc in grouped_documents:
    # Correctly access the company_id and internal_code_id from the _id field
    company_id = doc["_id"]["company_id"]
    internal_code_id = doc["_id"]["internal_code_id"]
    print(f"Calculating for {company_id} {internal_code_id}")
    # Combine all site_code values (including those with empty strings) into one array
    combined_site_codes = [code for code in doc["site_code"]]  # Exclude empty site_codes

    # Prepare the updated document
    updated_document = {
        "company_id": company_id,
        "internal_code_id": internal_code_id,
        "category_id": doc["category_id"],
        "isChecked": doc["isChecked"],
        "createdAt": doc["createdAt"],
        "updatedAt": doc["updatedAt"],
        "__v": doc["__v"],
        "site_code": combined_site_codes  # Store the combined site_code values
    }

    # Step 3: Update or insert the merged document (if not already existing)
    collection.update_one(
        {"company_id": company_id, "internal_code_id": internal_code_id},
        {"$set": updated_document},  # Avoid modifying the _id field
        upsert=True  # If no document is found, create a new one
    )

# Step 4: Remove the old individual documents that are no longer needed
# After the update, delete the records that have the same company_id and internal_code_id, but are not the merged record
collection.delete_many({
    "company_id": {"$exists": True},
    "internal_code_id": {"$exists": True},
    "site_code": {"$ne": ""}  # Ensure only original records with a non-empty site_code are deleted
})