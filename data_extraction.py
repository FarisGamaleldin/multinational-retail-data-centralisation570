import pandas as pd
from tabula import read_pdf

class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, db_connector, table_name):
        """
        Reads data from an RDS table and returns it as a Pandas DataFrame.
        :param db_connector: Instance of the DatabaseConnector class.
        :param table_name: Name of the RDS table to query.
        :return: Pandas DataFrame containing the table data.
        """
        try:
            # Initialize the database engine using the connector
            engine = db_connector.init_db_engine()
            print(f"Querying table: {table_name}")
            
            # Query the table data
            query = f"SELECT * FROM {table_name};"
            df = pd.read_sql(query, engine)
            
            print(f"Extracted {len(df)} rows from table '{table_name}'.")
            return df
        except Exception as e:
            print(f"Error reading RDS table '{table_name}': {e}")
            raise

    def retrieve_pdf_data(self, pdf_link):
        """
        Extracts data from a PDF file and returns it as a Pandas DataFrame.
        :param pdf_link: Link or path to the PDF document.
        :return: Pandas DataFrame containing the extracted data.
        """
        try:
            print(f"Retrieving data from PDF: {pdf_link}")
            
            # Extract tables from the PDF
            data_frames = read_pdf(pdf_link, pages="all", multiple_tables=True, pandas_options={"header": None})
            
            # Combine all extracted tables into a single DataFrame
            combined_data = pd.concat(data_frames, ignore_index=True)
            
            print("Data extraction from PDF complete.")
            return combined_data
        except Exception as e:
            print(f"Error extracting data from PDF: {e}")
            raise
