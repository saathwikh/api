from db import get_connection

def get_analytics():

    conn = get_connection()
    cur = conn.cursor()

    # 1. average temperature
    cur.execute("SELECT AVG(temperature) FROM fact_sensor")
    avg_temp = cur.fetchone()[0]

    # 2. total records
    cur.execute("SELECT COUNT(*) FROM fact_sensor")
    total = cur.fetchone()[0]

    # 3. anomaly count
    cur.execute("SELECT COUNT(*) FROM fact_sensor WHERE anomaly = TRUE")
    anomaly_count = cur.fetchone()[0]

    # 4. avg temp per day
    cur.execute("""
        SELECT d.day, AVG(f.temperature)
        FROM fact_sensor f
        JOIN dim_time d ON f.time_id = d.time_id
        GROUP BY d.day
        ORDER BY d.day
    """)
    avg_per_day_raw = cur.fetchall()

    cur.close()
    conn.close()

    # CLEAN FORMAT
    avg_per_day = [
        {"day": int(row[0]), "avg_temp": float(row[1])}
        for row in avg_per_day_raw
    ]

    return {
        "summary": {
            "average_temperature": float(avg_temp) if avg_temp else 0,
            "total_records": total,
            "anomaly_count": anomaly_count
        },
        "trends": {
            "avg_temp_per_day": avg_per_day
        }
    }