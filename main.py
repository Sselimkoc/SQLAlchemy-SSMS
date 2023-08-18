# Import required libraries
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager

# Database configuration
server = ''
database = ''
username = ''
password = ''
conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

# Define a context manager for handling transactions
@contextmanager
def transaction_context():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

# Function to fetch data from the database using a query
def fetch_data_from_database(query, session):
    try:
        combined_data = pd.read_sql(query, session.bind)
        return combined_data
    except Exception as e:
        print("Hata:", e)
        return None

# Your SQL query
combined_query = """
    -- Write your SQL query here
"""

try:
    with transaction_context() as session:
        combined_data = fetch_data_from_database(combined_query, session)
except SQLAlchemyError as e:
    print("Veritabanı Hatası:", e)
except Exception as e:
    print("Genel Hata:", e)

# Establish a connection using SQLAlchemy
with create_engine(conn_str).connect() as connection:
    schema_name = 'your_schema_name'
    table_name = 'your_table_name'
    
    # Join the columns into a comma-separated string
    columns = ', '.join(data_to_insert.columns)
    
    # Create placeholders for the values in the query
    placeholders = ', '.join([':' + col for col in data_to_insert.columns])
    
    # Construct the INSERT query
    query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
    
    # Begin a transaction using the context manager
    with transaction_context() as session:
        # Iterate through the rows of the DataFrame and execute the query for each row
        for row in data_to_insert.itertuples(index=False):
            # Create a dictionary of column names and values
            values = {col: None if pd.isna(value) else value for col, value in zip(data_to_insert.columns, row)}
            # Execute the query with the parameterized values
            connection.execute(text(query).bindparams(**values))
        # Commit the transaction
        session.commit()
