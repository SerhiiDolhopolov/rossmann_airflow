import clickhouse_connect

from config import CLICKHOUSE_HOST, CLICKHOUSE_HTTP_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD


def get_clickhouse_client():
    try:
        # Attempt to create a connection to ClickHouse
        client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, 
                                               port=CLICKHOUSE_HTTP_PORT, 
                                               username=CLICKHOUSE_USER,
                                               password=CLICKHOUSE_PASSWORD)
        return client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        raise