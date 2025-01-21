import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, db_config_path='db_creds.yaml'):
             self.db_config_path = db_config_path

    def read_db_creds(self):
        with open(self.db_config_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        with engine.connect() as connection:
            result = connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            )
            tables = [row[0] for row in result]
        return tables

    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)


