import sqlite3
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def initialize_staging_tables(db_name):
    conn = sqlite3.connect(db_name)
    try:
        cursor = conn.cursor()
        
        # Drop existing staging tables to ensure a clean start every run
        cursor.execute("DROP TABLE IF EXISTS stg_currencies;")
        cursor.execute("DROP TABLE IF EXISTS stg_exchange_rates;")

        # Recreate the staging tables
        cursor.execute("""
            CREATE TABLE stg_currencies (
                id INTEGER,
                short_code TEXT,
                name TEXT,
                code TEXT,
                precision INTEGER,
                subunit INTEGER,
                symbol TEXT,
                symbol_first BOOLEAN,
                decimal_mark TEXT,
                thousands_separator TEXT,
                load_timestamp DATETIME
            )
        """)

        cursor.execute("""
            CREATE TABLE stg_exchange_rates (
                load_timestamp DATETIME,
                base TEXT,
                currency TEXT,
                rate REAL
            )
        """)

        conn.commit()
        logging.info("Staging tables dropped and recreated.")
    except Exception as e:
        logging.error(f"Error initializing staging tables: {e}")
    finally:
        conn.close()

def load_staging_currencies(db_name, currencies_data):
    conn = sqlite3.connect(db_name)
    try:
        # Add the current load timestamp to the DataFrame
        df = pd.DataFrame(currencies_data)
        load_timestamp = datetime.utcnow().isoformat()
        df["load_timestamp"] = load_timestamp

        # Load data into the stg_currencies table
        df.to_sql("stg_currencies", conn, if_exists="append", index=False)
        logging.info("Staging currencies data loaded.")
    except Exception as e:
        logging.error(f"Error loading staging currencies: {e}")
    finally:
        conn.close()

def load_staging_exchange_rates(db_name, exchange_rates_response):
    # Use current UTC time as the load timestamp
    load_timestamp = datetime.utcnow().isoformat()

    base = exchange_rates_response.get("base")
    rates = exchange_rates_response.get("rates", {})

    data = []
    for currency, rate in rates.items():
        data.append({
            "load_timestamp": load_timestamp,
            "base": base,
            "currency": currency,
            "rate": rate
        })

    conn = sqlite3.connect(db_name)
    try:
        df = pd.DataFrame(data)
        # logging.info(f"Exchange rates DataFrame:\n{df.head()}")  # Debugging: Print DataFrame preview
        df.to_sql("stg_exchange_rates", conn, if_exists="append", index=False)
        logging.info("Staging exchange rates data loaded.")
    except Exception as e:
        logging.error(f"Error loading staging exchange rates: {e}")
    finally:
        conn.close()
