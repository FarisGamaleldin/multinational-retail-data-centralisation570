def clean_user_data(self, data):
    # 1. Change "NULL" strings to actual NULL (NaN) values
    data.replace("NULL", pd.NA, inplace=True)

    # 2. Remove rows with NULL (NaN) values in critical columns
    critical_columns = ['email', 'date_uuid']  # Adjust based on the data
    data.dropna(subset=critical_columns, inplace=True)

    # 3. Rename 'date_uuid' to 'join_date' and convert to datetime
    if 'date_uuid' in data.columns:
        data.rename(columns={'date_uuid': 'join_date'}, inplace=True)
        data['join_date'] = pd.to_datetime(data['join_date'], errors='coerce')

    # Remove rows with invalid join_date
    data = data[data['join_date'].notnull()]

    # Reset index after cleaning
    data.reset_index(drop=True, inplace=True)

    return data