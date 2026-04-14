import pandas as pd
import numpy as np

def process_data(weather, air):

    # extract weather
    temp = weather["current_weather"]["temperature"]

    # extract air quality from Open-Meteo
    air_value = air.get("current", {}).get("us_aqi", 0)

    # convert to dataframe
    df = pd.DataFrame({
        "temperature": [temp],
        "air_values": [air_value]
    })

    # numpy calculations
    df["score"] = np.array(df["temperature"]) * 0.5 + np.array(df["air_values"]) * 0.5

    return df