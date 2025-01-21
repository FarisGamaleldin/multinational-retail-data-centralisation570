import pandas as pd
from database_utils import DatabaseConnector

# Initialize DatabaseConnector
db_connector = DatabaseConnector()

# Create a sample DataFrame
sample_data = pd.DataFrame({
    'user_id': [1, 2, 3],
    'join_date': ['2022-01-01', '2023-03-15', '2021-12-15'],
    'age': [25, 30, 35]
})

# Upload the sample data to the database
db_connector.upload_to_db(sample_data, 'dim_users')
