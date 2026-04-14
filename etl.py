from db import get_connection
import pandas as pd

def load_to_star(df):

    conn = get_connection()
    cur = conn.cursor() 

    for _, row in df.iterrows():

        now = pd.Timestamp.now()

        # insert into dim_table
        cur.execute("""
            INSERT INTO dim_time (hour, day, month, year)
            VALUES (%s, %s, %s, %s)
            RETURNING time_id
        """, (
            now.hour,
            now.day,
            now.month,
            now.year
        ))

        time_id = cur.fetchone()[0]

        # insert into fact_table
        cur.execute("""
            INSERT INTO fact_sensor (device_id, time_id, temperature, humidity, anomaly)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            "api_sensor",
            time_id,
            float(row["temperature"]),
            float(row["air_values"]),
            bool(row.get("is_anomaly", False))
        ))

    conn.commit()
    cur.close()
    conn.close()