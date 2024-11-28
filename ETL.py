import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime

# Database connection parameters
AIMS_DB = {
    "dbname": "AIMS_DB",
    "user": "username",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}
AMOS_DB = {
    "dbname": "AMOS_DB",
    "user": "username",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}
DW_DB = {
    "dbname": "DW_DB",
    "user": "username",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

CSV_FILE = "aircraft-manufacturerinfo-lookup.csv"

# Step 1: Connect to databases
def connect_to_db(db_params):
    return psycopg2.connect(**db_params)

# Extract from PostgreSQL and CSV
def extract_data():
    # Connect to AIMS and AMOS
    aims_conn = connect_to_db(AIMS_DB)
    amos_conn = connect_to_db(AMOS_DB)
    
    # Extract AIMS Data
    flights_query = "SELECT * FROM Flights;"
    slots_query = "SELECT * FROM Slots;"
    flights = pd.read_sql_query(flights_query, aims_conn)
    slots = pd.read_sql_query(slots_query, aims_conn)
    
    # Extract AMOS Data
    maintenance_query = "SELECT * FROM MaintenanceEvents;"
    maintenance_events = pd.read_sql_query(maintenance_query, amos_conn)
    
    # Extract CSV Data
    aircraft_csv = pd.read_csv(CSV_FILE)
    
    # Close connections
    aims_conn.close()
    amos_conn.close()
    
    return flights, slots, maintenance_events, aircraft_csv

# Step 2: Transform Data
def transform_data(flights, slots, maintenance_events, aircraft_csv):
    # Clean flights data
    flights = flights.drop_duplicates()
    flights = flights.dropna(subset=["actualDeparture", "actualArrival"])
    
    # Calculate Flight Hours (FH) and Flight Cycles (FC)
    flights["flightHours"] = (pd.to_datetime(flights["actualArrival"]) - pd.to_datetime(flights["actualDeparture"])).dt.total_seconds() / 3600
    flights["flightCycles"] = 1  # Each flight counts as one cycle
    
    # Clean and integrate aircraft data
    aircraft = flights[["aircraftRegistration"]].drop_duplicates()
    aircraft = aircraft.merge(aircraft_csv, on="aircraftRegistration", how="left")
    
    # Handle missing models or manufacturers
    aircraft.fillna({"model": "Unknown", "manufacturer": "Unknown"}, inplace=True)
    
    # Create Time Dimension
    slots["day"] = pd.to_datetime(slots["scheduledDeparture"]).dt.date
    slots["month"] = pd.to_datetime(slots["scheduledDeparture"]).dt.month
    slots["year"] = pd.to_datetime(slots["scheduledDeparture"]).dt.year
    time_dim = slots[["day", "month", "year"]].drop_duplicates()
    
    # Calculate DYR, CNR, and ADOS
    flights["delay"] = flights["delayCode"].notnull()
    flights["cancelled"] = flights["cancelled"].fillna(False)
    delay_rate = flights.groupby("aircraftRegistration")["delay"].mean() * 100
    cancellation_rate = flights.groupby("aircraftRegistration")["cancelled"].mean() * 100
    maintenance_events["daysOutOfService"] = maintenance_events["duration"].dt.days
    
    # Integrate all metrics into a fact table
    fact_table = flights.groupby(["aircraftRegistration", "scheduledDeparture"]).agg({
        "flightHours": "sum",
        "flightCycles": "sum"
    }).reset_index()
    
    fact_table = fact_table.merge(delay_rate, on="aircraftRegistration", how="left")
    fact_table = fact_table.merge(cancellation_rate, on="aircraftRegistration", how="left")
    fact_table = fact_table.merge(maintenance_events[["aircraftRegistration", "daysOutOfService"]], on="aircraftRegistration", how="left")
    
    return aircraft, time_dim, fact_table

# Step 3: Load Data into DW
def load_data(aircraft, time_dim, fact_table):
    dw_conn = create_engine(f"postgresql://{DW_DB['user']}:{DW_DB['password']}@{DW_DB['host']}:{DW_DB['port']}/{DW_DB['dbname']}")
    
    # Load data into dimension tables
    aircraft.to_sql("Dim_Aircraft", dw_conn, if_exists="append", index=False)
    time_dim.to_sql("Dim_Time", dw_conn, if_exists="append", index=False)
    
    # Load data into fact table
    fact_table.to_sql("Fact_FlightMetrics", dw_conn, if_exists="append", index=False)

# Main ETL Process
if __name__ == "__main__":
    print("Starting ETL Process...")
    
    # Extract
    flights, slots, maintenance_events, aircraft_csv = extract_data()
    print("Data extracted successfully.")
    
    # Transform
    aircraft, time_dim, fact_table = transform_data(flights, slots, maintenance_events, aircraft_csv)
    print("Data transformed successfully.")
    
    # Load
    load_data(aircraft, time_dim, fact_table)
    print("Data loaded successfully into the Data Warehouse.")
