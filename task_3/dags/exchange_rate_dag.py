from datetime import datetime
import os

from airflow.decorators import dag, task
import pandas as pd

from config import DATADIR
from utils.currency_api import ApiClient
from utils.gcp import GCSClient


@dag(
    dag_id="Exchange_Rate_DAG",
    description="DAG for fetching, processing and storing exchange rate data based on USD",
    start_date=datetime(2024, 7, 1),
    schedule="@daily",
    catchup=True,
    default_args={"owner": "Dee", "retries": 1},
    tags=["exchange_rate"]
)
def currency_exchanger_dag():
    @task
    def fetch_exchange_rates(execution_date) -> dict:
        """
        Fetches historical exchange rates for the given execution date.

        :param execution_date: Execution date provided by Airflow.

        :return: Dictionary containing the date, exchange rates and base currency.
        :rtype: dict
        """
        date_to_fetch = execution_date.strftime("%Y-%m-%d")
        data = ApiClient.get_historical_exchange_rate(date=date_to_fetch)

        timestamp = data.get("timestamp")
        date = datetime.fromtimestamp(timestamp).date()

        base = data.get("base")
        if not base:
            raise Exception(f"Error fetching data: No base for {date}")

        rates = data.get("rates")
        if not rates:
            raise Exception(f"Error fetching data: No rates for {date}")

        return {"date": date, "rates": rates, "base": base}

    @task
    def process_data(data: dict) -> str:
        """
        Processes the exchange rate data and saves it as a Parquet file.
        Stores the exchange rate for each date in the directory with the base currency.
        Returns the relative path along with the base currency directory.
        For example: USD/2024-07-01.parquet

        :param data: Dictionary containing the date, exchange rates, and base currency.

        :return: String containing the local file path of the saved Parquet file.
        :rtype: str
        """
        currency_df = pd.DataFrame(data['rates'], index=[data['date']])

        if not os.path.exists(os.path.join(DATADIR, data['base'])):
            raise Exception(f"Error processing data: No directory for base:{data['base']}")

        temp_parquet_path = os.path.join(DATADIR, data['base'], f"{data['date']}.parquet")
        currency_df.to_parquet(temp_parquet_path, engine='pyarrow', compression='snappy')

        return temp_parquet_path

    @task
    def store_data_to_gcp(temp_parquet_path: str) -> str:
        """
        Uploads the processed Parquet file to Google Cloud Storage.
        Saves the exchange rate in the corresponding folder with the base currency.
        Example: <gcp-storage>/USD/2024-07-01.parquet

        :param temp_parquet_path: Abspath to the temporary Parquet file.

        :return: Abspath to the temporary Parquet file.
        :rtype: str
        """
        relative_file_path = os.path.relpath(temp_parquet_path, DATADIR)
        GCSClient.upload_file(temp_parquet_path, relative_file_path)
        return temp_parquet_path

    @task
    def cleanup_temporary_storage(temp_parquet_path: str):
        """
        Cleans up the temporary Parquet file in local storage.

        :param temp_parquet_path: Path to the temporary Parquet file.
        :return: None
        """
        if os.path.exists(temp_parquet_path):
            os.remove(temp_parquet_path)

    exchange_rates_data = fetch_exchange_rates()
    processed_data_filepath = process_data(exchange_rates_data)
    temp_file_filepath = store_data_to_gcp(processed_data_filepath)
    cleanup_temporary_storage(temp_file_filepath)


currency_exchanger_dag()
