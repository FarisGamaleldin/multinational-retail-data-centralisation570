from database_utils import DatabaseConnector

# Initialize DatabaseConnector
db_connector = DatabaseConnector()

# Test database engine initialization
try:
    engine = db_connector.init_db_engine()
    print("Database engine initialized successfully!")
    print(engine)
except Exception as e:
    print(f"Failed to initialize database engine: {e}")
