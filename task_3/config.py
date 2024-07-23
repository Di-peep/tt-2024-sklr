import os


BASEDIR = os.path.dirname(__file__)

DAGSDIR = os.path.join(BASEDIR, "dags")
DATADIR = os.path.join(BASEDIR, "data")


class CurrencyApiConfig:
    APP_ID = "--YOUR-APP-ID--"
    BASE_URL = "https://openexchangerates.org/api/"

    # API Endpoints
    ENDPOINT_LATEST = BASE_URL + "latest.json"
    ENDPOINT_HISTORICAL = BASE_URL + "historical/{date}.json"
    ENDPOINT_CURRENCIES = BASE_URL + "currencies.json"


class GCSConfig:
    PROJECT_ID = "--YOUR-PROJECT-ID--"
    CREDENTIALS = os.path.join(BASEDIR, "credentials.json")

    BUCKET_NAME = "--YOUR-BUCKET--"
    BUCKET_URL = f"gs://{BUCKET_NAME}"
