from fastapi import FastAPI
from contextlib import asynccontextmanager
from sqlmodel import SQLModel
from sqlalchemy import text
from server.config.db import engine

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/ping")
def ping():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        version = result.fetchone()
    return {"postgres_version": version[0]}