import pandas as pd

class DataCleaning:
    def __init__(self):
        pass
    def clean_user_data(self, data):

        # Remove rows with NULL values
        data = data.dropna()

        # Ensure date columns are in datetime format
        if 'registration_date' in data.columns:
            data['registration_date'] = pd.to_datetime(data['registration_date'], errors='coerce')

        # Drop rows with invalid or corrupted dates
        data = data[data['registration_date'].notnull()]

        # Filter rows with incorrect data types (example: age column should be an integer)
        if 'age' in data.columns:
            data = data[data['age'].apply(lambda x: isinstance(x, int) and x > 0)]

        # Reset index after cleaning
        data = data.reset_index(drop=True)

        return data
