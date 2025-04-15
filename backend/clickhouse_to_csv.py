import pandas as pd

def export_table_to_csv(client, table_name, file_path):
    result = client.query(f"SELECT * FROM {table_name}")
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    df.to_csv(file_path, index=False)
    return len(df)
