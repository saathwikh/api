from fastapi import FastAPI
from fetch_data import fetch_weather, fetch_air_quality
from processing import process_data
from etl import load_to_star
from db import save_raw, init_db

init_db()

app = FastAPI()

@app.get("/run-pipeline")
def run_pipeline():

    weather = fetch_weather()
    air = fetch_air_quality()

    save_raw(weather, air)

    df = process_data(weather, air)

    load_to_star(df)

    return {"status": "pipeline executed"}