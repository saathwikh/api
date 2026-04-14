from fastapi import FastAPI, HTTPException
from fetch_data import fetch_weather, fetch_air_quality
from processing import process_data
from etl import load_to_star
from db import save_raw, init_db
from analytics import get_analytics

init_db()

app = FastAPI()

@app.get("/run-pipeline")
def run_pipeline():
    try:
        weather = fetch_weather()
        air = fetch_air_quality()

        save_raw(weather, air)

        df = process_data(weather, air)

        load_to_star(df)

        return {"status": "pipeline executed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics")
def analytics():
    return get_analytics()