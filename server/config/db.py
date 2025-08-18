import os
from sqlmodel import create_engine, Session
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("POSTGRES_URI")

engine = create_engine(DATABASE_URL, echo=True, future=True)

def get_session():
    with Session(engine) as session:
        yield session
