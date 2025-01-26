import pandas as pd

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, data):

        data.replace("NULL", pd.NA, inplace=True)

        critical_columns = ['email', 'date_uuid']
        data.dropna(subset=critical_columns, inplace=True)

        if 'date_uuid' in data.columns:
            data.rename(columns={'date_uuid': 'join_date'}, inplace=True)
            data['join_date'] = pd.to_datetime(data['join_date'], errors='coerce')

        data = data[data['join_date'].notnull()]

        data.reset_index(drop=True, inplace=True)

        return data
