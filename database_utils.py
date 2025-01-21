import yaml
from sqlalchemy import create_engine
import pandas as pd

class DatabaseConnector:
    def __init__(self, db_config_path='db_creds.yaml'):
        """
        Initialize the DatabaseConnector with a path to the credentials file.
        :param db_config_path: Path to the YAML file containing database credentials.
        """
        self.db_config_path = db_config_path

    def read_db_creds(self):
        """
        Read database credentials from a YAML file.
        :return: Dictionary containing database credentials.
        """
        with open(self.db_config_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        """
        Initialize and return a SQLAlchemy database engine.
        :return: SQLAlchemy engine.
        """
        creds = self.read_db_creds()
        engine = create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        return engine

    def list_db_tables(self):
        """
        List all tables in the database.
        :return: List of table names.
        """
        engine = self.init_db_engine()
        with engine.connect() as connection:
            result = connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            )
            tables = [row[0] for row in result]
        return tables

    def upload_to_db(self, df, table_name):
        """
        Upload a Pandas DataFrame to a specified table in the database.
        :param df: DataFrame to upload.
        :param table_name: Name of the table to upload data to.
        """
        try:
            # Initialize the database engine
            engine = self.init_db_engine()
            
            # Upload the DataFrame to the specified table
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Data uploaded successfully to table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")
