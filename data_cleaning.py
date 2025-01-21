import pandas as pd

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, data):

        # 1. Change "NULL" strings to actual NULL (NaN) values
        data.replace("NULL", pd.NA, inplace=True)

        # 2. Remove rows with NULL (NaN) values
        data.dropna(inplace=True)

        # 3. Convert "join_date" column to a datetime data type
        if 'join_date' in data.columns:
            data['join_date'] = pd.to_datetime(data['join_date'], errors='coerce')

        # Remove rows with invalid dates (datetime conversion failed)
        data = data[data['join_date'].notnull()]

        # Reset index after cleaning
        data.reset_index(drop=True, inplace=True)

        return data

