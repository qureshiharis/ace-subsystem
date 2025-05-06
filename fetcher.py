import requests
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta
import pytz  # for timezone awareness

from logger_config import setup_logger
logger = setup_logger(__name__)

BASE_URL = 'https://webport.it.pitea.se/api'

def fetch_sensor_data(tag_name, api_key, window_minutes=60):
    # Use Stockholm local time
    local_tz = pytz.timezone("Europe/Stockholm")
    end_time = datetime.now(local_tz)
    start_time = end_time - timedelta(minutes=window_minutes)

    # Format with proper timezone-aware ISO format
    formatted_start = urllib.parse.quote(start_time.replace(microsecond=0).isoformat())
    formatted_end = urllib.parse.quote(end_time.replace(microsecond=0).isoformat())


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