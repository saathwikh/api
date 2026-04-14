import pandas as pd
import numpy as np

def process_data(weather, air):

    temp = weather["current_weather"]["temperature"]

    air_value = air.get("current", {}).get("us_aqi", 0)

    df = pd.DataFrame({
        "temperature": [temp],
        "air_values": [air_value]
    })

    df["score"] = np.array(df["temperature"]) * 0.5 + np.array(df["air_values"]) * 0.5

    df["is_anomaly"] = (df["temperature"] > 50.0) | (df["air_values"] > 100.0)

    return df