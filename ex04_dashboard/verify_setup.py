import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import psycopg2

print("Libraries imported successfully.")

# Database Connection Details
DB_USER = 'postgres'
DB_PASS = 'password'
DB_HOST = 'localhost'
DB_PORT = '5433' # External port mapped in docker-compose
DB_NAME = 'warehouse'

connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

print("Connecting to database...")
try:
    connection = engine.connect()
    print("Connection successful!")
    
    # Simple query to verify data access
    result = connection.execute("SELECT COUNT(*) FROM fact_trips")
    count = result.fetchone()[0]
    print(f"Row count in fact_trips: {count}")
    
    connection.close()
except Exception as e:
    print(f"Connection failed: {e}")
