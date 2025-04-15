from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"message": "ClickHouse Ingestion Tool is running!"}
