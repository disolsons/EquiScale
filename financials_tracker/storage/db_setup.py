from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "financials_tracker.db"

Base = declarative_base()


def get_engine(db_path: str | None = None):
    db_file = Path(db_path) if db_path else DEFAULT_DB_PATH
    db_file.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{db_file}", future=True)


def get_session_factory(db_path: str | None = None):
    engine = get_engine(db_path)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def create_tables(db_path: str | None = None):
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)