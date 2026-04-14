import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"),
        sslmode="require"
    )

def init_db():
    conn = get_connection()
    cur = conn.cursor()
    
    # Create raw_sensor_data table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_sensor_data (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(50),
            temperature FLOAT,
            aqi FLOAT,
            timestamp TIMESTAMP
        );
    """)
    
    # Create dim_time table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_time (
            time_id SERIAL PRIMARY KEY,
            hour INT,
            day INT,
            month INT,
            year INT
        );
    """)
    
    # Create fact_sensor table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_sensor (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(50),
            time_id INT REFERENCES dim_time(time_id),
            temperature FLOAT,
            aqi FLOAT,
            anomaly BOOLEAN
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()

def save_raw(weather, air):
    conn = get_connection()
    cur = conn.cursor()
    
    air_value = air.get("current", {}).get("us_aqi", 0)
    
    cur.execute("""
        INSERT INTO raw_sensor_data (device_id, temperature, aqi, timestamp)
        VALUES (%s, %s, %s, NOW())
    """, (
        "api_sensor",
        weather["current_weather"]["temperature"],
        air_value
    ))
    conn.commit()
    cur.close()
    conn.close()