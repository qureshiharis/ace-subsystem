# fetcher.py
import requests
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

FIXED_OFFSET = "+02:00"
BASE_URL = 'https://webport.it.pitea.se/api'

def fetch_sensor_data(tag_name, api_key, window_minutes=60):
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=window_minutes)

    formatted_start = urllib.parse.quote(start_time.strftime("%Y-%m-%dT%H:%M:%S") + FIXED_OFFSET)
    formatted_end = urllib.parse.quote(end_time.strftime("%Y-%m-%dT%H:%M:%S") + FIXED_OFFSET)

    logger.info(f"Fetching data for tag '{tag_name}' from {formatted_start} to {formatted_end}")

    url = f"{BASE_URL}/v1/trend/history?tag={tag_name}&start={formatted_start}&end={formatted_end}"

    headers = {
        "accept": "application/json",
        "token": api_key
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get(tag_name, {})
            df = pd.DataFrame(data.items(), columns=["Timestamp", "Value"])
            df["Timestamp"] = pd.to_datetime(df["Timestamp"])
            logger.debug(f"Retrieved {len(df)} records for tag '{tag_name}'")
            return df
        else:
            logger.warning(f"Failed to fetch data for tag '{tag_name}' - Status {response.status_code}: {response.text}")
    except Exception as e:
        logger.exception(f"Exception occurred while fetching data for tag '{tag_name}': {e}")
    return None