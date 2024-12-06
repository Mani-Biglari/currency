import requests
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv("CURRENCY_BEACON_API_KEY")
BASE_URL = "https://api.currencybeacon.com/v1"

def fetch_all_currencies():
    url = f"{BASE_URL}/currencies"
    params = {"api_key": API_KEY, "type": "fiat"}
    try:
        logging.info(f"Fetching currencies from {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        # logging.info(f"Exchange rates response: {data}")
        return data.get("response", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching currencies: {e}")

def fetch_exchange_rates(base_currency, target_currencies):
    url = f"{BASE_URL}/latest"
    params = {
        "api_key": API_KEY,
        "base": base_currency,
        "symbols": ",".join(target_currencies)
    }
    try:
        logging.info(f"Fetching exchange rates from {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("response", {})
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching exchange rates: {e}")
