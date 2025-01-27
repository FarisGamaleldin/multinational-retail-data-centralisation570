import pandas as pd

class DataCleaning:
    """
    A class to clean user data extracted from the database.
    """

    def __init__(self):
        pass

    def clean_user_data(self, data):
        """
        Cleans the raw user data by handling null values, fixing data types,
        and ensuring consistency.

        :param data: Pandas DataFrame containing raw user data.
        :return: Cleaned Pandas DataFrame.
        """
        print(f"Initial data rows: {len(data)}")
        
        # 1. Replace "NULL" strings with NaN
        data.replace("NULL", pd.NA, inplace=True)
        print(f"Rows after replacing 'NULL': {len(data)}")

        # 2. Drop rows with missing critical columns
        critical_columns = ['email', 'date_uuid']
        data.dropna(subset=critical_columns, inplace=True)
        print(f"Rows after dropping rows with missing critical columns: {len(data)}")

        # 3. Convert 'date_uuid' to datetime and rename it to 'join_date'
        if 'date_uuid' in data.columns:
            data.rename(columns={'date_uuid': 'join_date'}, inplace=True)
            data['join_date'] = pd.to_datetime(data['join_date'], errors='coerce')
        print(f"Rows after datetime conversion: {len(data)}")

        # 4. Remove rows with invalid 'join_date'
        data = data[data['join_date'].notnull()]
        print(f"Rows after removing invalid 'join_date': {len(data)}")

        # 5. Reset the DataFrame index
        data.reset_index(drop=True, inplace=True)

        print("Data cleaning complete.")
        return data
