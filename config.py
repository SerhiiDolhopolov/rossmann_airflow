import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
load_dotenv(dotenv_path=".env.secret", override=True)

SQL_PATH = Path(__file__).parent.absolute() / "src" / "sql"
TMP_PATH = Path(__file__).parent.absolute() / "tmp"
if not TMP_PATH.exists():
    TMP_PATH.mkdir(parents=True)

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")

S3_HOST = os.getenv("S3_HOST")
S3_PORT = os.getenv("S3_PORT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DELIVERY_DB = os.getenv("CLICKHOUSE_DELIVERY_DB")