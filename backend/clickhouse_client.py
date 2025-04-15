from clickhouse_connect import get_client

def connect_clickhouse(host, port, user, jwt_token, database, secure=True):
    client = get_client(
        host=host,
        port=port,
        username=user,
        password=jwt_token,
        database=database,
        secure=secure
    )
    return client

def list_tables(client):
    result = client.query("SHOW TABLES")
    return [row[0] for row in result.result_rows]
