from pathlib import Path

from sqlalchemy import MetaData, create_engine, delete
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

def reset_all_data(db_path: str | None = None):
    engine = get_engine(db_path)

    metadata = MetaData()
    metadata.reflect(bind=engine)

    conn = engine.connect()
    with conn.begin() as trans:
        for table in reversed(metadata.sorted_tables):
            print(f"Deleting all rows from table: {table.name}")
            # Use delete without a where condition to remove all rows efficiently
            delete_statement = delete(table)
            conn.execute(delete_statement)
        trans.commit()
    conn.close()
