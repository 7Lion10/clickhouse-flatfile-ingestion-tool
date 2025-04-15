import pandas as pd

def upload_csv_to_clickhouse(file_path, client, table_name):
    df = pd.read_csv(file_path)

    client.command(f"DROP TABLE IF EXISTS {table_name}")

    create_stmt = f"CREATE TABLE {table_name} ("
    for col in df.columns:
        create_stmt += f"{col} String, "
    create_stmt = create_stmt.rstrip(", ") + ") ENGINE = MergeTree() ORDER BY tuple()"
    client.command(create_stmt)

    client.insert_dataframe(table_name, df)
    return len(df)
