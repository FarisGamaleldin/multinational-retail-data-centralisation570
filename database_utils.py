import yaml
from sqlalchemy import create_engine

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
        try:
            with open(self.db_config_path, 'r') as file:
                creds = yaml.safe_load(file)
            print("Database credentials loaded successfully.")
            return creds
        except FileNotFoundError:
            print("Database credentials file not found.")
            raise
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
            raise

    def init_db_engine(self):
        """
        Initialize and return a SQLAlchemy database engine.
        :return: SQLAlchemy engine.
        """
        creds = self.read_db_creds()
        try:
            engine = create_engine(
                f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            )
            print("Database engine initialized successfully.")
            return engine
        except Exception as e:
            print(f"Failed to initialize database engine: {e}")
            raise


    def list_db_tables(self):
        """
        List all tables in the public schema of the database.
        :return: List of table names.
        """
        engine = self.init_db_engine()
        try:
            with engine.connect() as connection:
                result = connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
                 )
                tables = [row[0] for row in result.fetchall()]  # Fetch all rows and access table names
            print(f"Debug - Tables fetched: {tables}")
            return tables
        except Exception as e:
            print(f"Failed to list tables: {e}")
            return []


    def upload_to_db(self, df, table_name):
        """
        Upload a Pandas DataFrame to a specified table in the database.
        :param df: DataFrame to upload.
        :param table_name: Name of the table to upload data to.
        """
        engine = self.init_db_engine()
        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Data uploaded successfully to table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")
            raise
