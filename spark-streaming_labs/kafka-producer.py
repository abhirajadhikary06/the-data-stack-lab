import os 
import time 
import json 
import logging 
import requests 
from kafka import KafkaProducer 
from dotenv import load_dotenv
from spark.sql import SparkSession

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("weather-producer")

API_KEY = os.environ["WEATHERAPI_KEY"]
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather-data")
CITIES = [c.strip() for c in os.getenv("CITIES", "London").split(",") if c.strip()]
INTERVAL = int(os.getenv("FETCH_INTERVAL_SECONDS", "10"))
WEATHER_URL = "https://api.weatherapi.com/v1/current.json"


def kafka_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                acks='all',
                retries=5,
                linger_ms=10,
                key_serializer=lambda k: k.encode('utf-8'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            log.info("Connected to Kafka")
            return p 
        except Exception as e:
            log.warning("Kafka is not ready: %s", e)
            time.sleep(5)

def fetch_weather(city):
    data = requests.get(
        WEATHER_URL,
        params={"key": API_KEY, "q": city, "aqi": "no"},
        timeout=10
    ).json()

    loc, cur = data["location"], data["current"]
    return{
        "city": loc["name"],
        "region": loc["region"],
        "country": loc["country"],
        "lat": loc["lat"],
        "lon": loc["lon"],
        "localtime": loc["localtime"],
        "temp_c": cur["temp_c"],
        "feelslike_c": cur["feelslike_c"],
        "humidity": cur["humidity"],
        "wind_kph": cur["wind_kph"],
        "pressure_mb": cur["pressure_mb"],
        "condition": cur["condition"]["text"],
        "cloud": cur["cloud"],
        "uv": cur["uv"],
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

def main():
    producer = kafka_producer()
    while True:
        for city in CITIES:
            try: 
                record = fetch_weather(city)
                producer.send(TOPIC, key=city, value=record)
                log.info("Sent %s: %.1f C, %s", city, record["temp_c"], record["condition"])
            except Exception as e:
                log.error("Error fetching weather for %s: %s", city, e)
                continue
        producer.flush()
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()