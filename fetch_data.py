import requests

def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=12.97&longitude=77.59&current_weather=true"
    res = requests.get(url)
    return res.json()


def fetch_air_quality():
    url = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=12.97&longitude=77.59&current=us_aqi"
    res = requests.get(url)
    return res.json()