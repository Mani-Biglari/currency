import logging
from fetch_data import fetch_all_currencies, fetch_exchange_rates
from load_data import initialize_staging_tables, load_staging_currencies, load_staging_exchange_rates

logging.basicConfig(level=logging.INFO)
DB_NAME = "currency_data.db"
BASE_CURRENCY = "USD"

def main():
    # Initialize staging tables (drops and recreates them)
    initialize_staging_tables(DB_NAME)

    # Fetch raw currencies data from the source
    currencies_data = fetch_all_currencies()
    if currencies_data:
        load_staging_currencies(DB_NAME, currencies_data)
    else:
        logging.warning("No currencies data returned from source.")

    # Fetch raw exchange rates data
    target_currencies = [record['short_code'] for record in currencies_data] if currencies_data else []
    exchange_rates_data = fetch_exchange_rates(BASE_CURRENCY, target_currencies)
    if exchange_rates_data:
        load_staging_exchange_rates(DB_NAME, exchange_rates_data)
    else:
        logging.warning("No exchange rates data returned from source.")

    logging.info("Staging load completed successfully.")

if __name__ == "__main__":
    main()


""" import logging
from fetch_data import fetch_all_currencies, fetch_exchange_rates
from transform_data import transform_currencies_data, transform_exchange_rates_data
from load_data import initialize_database, load_currencies_data, load_exchange_rates_data

logging.basicConfig(level=logging.INFO)

DB_NAME = "currency_data.db"
BASE_CURRENCY = "USD"

def main():
    # Initialize database
    initialize_database(DB_NAME)

    # Fetch and transform currencies
    currencies_data = fetch_all_currencies()
    if not currencies_data:
        logging.error("No currencies data fetched.")
        return

    currencies_df = transform_currencies_data(currencies_data)

    # Print the columns of the DataFrame to inspect them
    print(currencies_df.columns) \

    load_currencies_data(DB_NAME, currencies_df)

    # Fetch and transform exchange rates
    target_currencies = currencies_df["currency_code"].tolist()
    exchange_rates_data = fetch_exchange_rates(BASE_CURRENCY, target_currencies)
    if not exchange_rates_data:
        logging.error("No exchange rates data fetched.")
        return

    exchange_rates_df = transform_exchange_rates_data(exchange_rates_data, BASE_CURRENCY)
    if exchange_rates_df.empty:
        logging.error("No exchange rates data to load.")
        return

    load_exchange_rates_data(DB_NAME, exchange_rates_df)
    logging.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
 """