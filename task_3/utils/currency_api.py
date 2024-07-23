from datetime import datetime
from typing import Optional
import requests

from config import CurrencyApiConfig


class ApiClient:
    config = CurrencyApiConfig

    @staticmethod
    def get_latest_exchange_rate(base: str = 'USD') -> dict:
        """
        Fetches the latest exchange rates.

        :param base: The base currency to use for the exchange rates.
            (Works only with USD because of the free tier)

        :return: A dictionary containing the exchange rates.
        :rtype: dict
        """
        url = ApiClient.config.ENDPOINT_LATEST
        payload = {
            "app_id": ApiClient.config.APP_ID,
            "base": base,
        }
        response = requests.get(url, params=payload)
        if response.status_code != 200:
            raise Exception("Error fetching data from API")
        return response.json()

    @staticmethod
    def get_historical_exchange_rate(date: Optional[str] = None, base: str = 'USD') -> dict:
        """
        Fetches historical exchange rates for a given date.

        :param date: Format: "YYYY-MM-DD".
            The date for which to fetch the exchange rates.
        :param base: The base currency to use for the exchange rates.
            (Works only with USD because of the free tier)

        :return: A dictionary containing the exchange rates.
        :rtype: dict
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        url = ApiClient.config.ENDPOINT_HISTORICAL.format(date=date)
        payload = {
            "app_id": ApiClient.config.APP_ID,
            "base": base,
        }
        response = requests.get(url, params=payload)
        if response.status_code != 200:
            raise Exception("Error fetching data from API")
        return response.json()

    @staticmethod
    def get_currencies() -> dict:
        """
        Fetches a list of available currencies.

        :return: A dictionary containing the available currencies.
        :rtype: dict
        """
        url = ApiClient.config.ENDPOINT_CURRENCIES
        payload = {
            "app_id": ApiClient.config.APP_ID,
        }
        response = requests.get(url, params=payload)
        if response.status_code != 200:
            raise Exception("Error fetching data from API")
        return response.json()
