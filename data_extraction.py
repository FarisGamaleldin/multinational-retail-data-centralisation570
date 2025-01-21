import pandas as pd

class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, db_connector, table_name):
        # Initialize the database engine using the connector
        engine = db_connector.init_db_engine()

        # Query the table data
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, engine)

        return df
