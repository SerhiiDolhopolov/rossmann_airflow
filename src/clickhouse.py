import logging

import clickhouse_connect

from config import CLICKHOUSE_HOST, CLICKHOUSE_HTTP_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

logger = logging.getLogger(__name__)


def get_clickhouse_client(attempts=3):
    while attempts:
        try:

            client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, 
                                                port=CLICKHOUSE_HTTP_PORT, 
                                                username=CLICKHOUSE_USER,
                                                password=CLICKHOUSE_PASSWORD)
            return client
        except Exception as e:
            attempts -= 1
            if attempts == 0:
                logger.error(
                    "Failed to connect to ClickHouse after multiple attempts: %s", 
                    e, 
                    exc_info=True
                )
                raise
            else:
                logger.warning(
                    "Attempt to connect to ClickHouse failed: %s. "
                    "Retrying... (%s attempts left)", 
                    e, 
                    attempts,
                    exc_info=True
                )
