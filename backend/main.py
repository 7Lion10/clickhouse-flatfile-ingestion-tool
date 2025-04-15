from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"message": "ClickHouse Ingestion Tool is running!"}

from fastapi import HTTPException
from pydantic import BaseModel
import clickhouse_client

class ClickHouseConnectRequest(BaseModel):
    host: str
    port: int
    user: str
    jwt_token: str
    database: str

@app.post("/connect-clickhouse/")
def connect_clickhouse(data: ClickHouseConnectRequest):
    try:
        client = clickhouse_client.connect_clickhouse(
            host=data.host,
            port=data.port,
            user=data.user,
            jwt_token=data.jwt_token,
            database=data.database
        )
        tables = clickhouse_client.list_tables(client)
        return {"status": "success", "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

from fastapi import UploadFile, File
import csv_to_clickhouse

@app.post("/upload-csv/")
def upload_csv(
    file: UploadFile = File(...),
    host: str = "", port: int = 0,
    user: str = "", jwt_token: str = "",
    database: str = "", table_name: str = ""
):
    try:
        contents = file.file.read()
        temp_path = f"temp_upload.csv"
        with open(temp_path, "wb") as f:
            f.write(contents)

        client = clickhouse_client.connect_clickhouse(host, port, user, jwt_token, database)
        rows = csv_to_clickhouse.upload_csv_to_clickhouse(temp_path, client, table_name)
        return {"status": "success", "rows_inserted": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

import clickhouse_client
import clickhouse_to_csv

@app.get("/export-table/")
def export_table(
    host: str, port: int, user: str,
    jwt_token: str, database: str,
    table_name: str
):
    try:
        client = clickhouse_client.connect_clickhouse(host, port, user, jwt_token, database)
        rows = clickhouse_to_csv.export_table_to_csv(client, table_name, "export.csv")
        return {"status": "success", "rows_exported": rows, "file_path": "export.csv"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

