from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Step 1: Initialize Classes
db_connector = DatabaseConnector()   # Handles database connections
extractor = DataExtractor()          # Extracts data from the RDS
cleaner = DataCleaning()             # Cleans the data

# Step 2: Extract Data from the legacy_users Table
user_table = 'legacy_users'
print(f"Extracting data from table: {user_table}...")
try:
    raw_data = extractor.read_rds_table(db_connector, user_table)
    print(f"Extracted {len(raw_data)} rows from the table '{user_table}'.")
except Exception as e:
    print(f"Error extracting data: {e}")
    exit()

# Step 3: Clean the Extracted Data
print("Cleaning the extracted data...")
try:
    cleaned_data = cleaner.clean_user_data(raw_data)
    print(f"Cleaned data contains {len(cleaned_data)} rows.")
    if len(cleaned_data) != 15284:
        print(f"Warning: Expected 15,284 rows after cleaning but got {len(cleaned_data)}.")
except Exception as e:
    print(f"Error cleaning data: {e}")
    exit()

# Step 4: Upload Cleaned Data to Local Database
print("Uploading cleaned data to the sales_data database...")
try:
    db_connector.upload_to_db(cleaned_data, 'dim_users')
    print("Data uploaded successfully to the dim_users table.")
except Exception as e:
    print(f"Error uploading data: {e}")
    exit()