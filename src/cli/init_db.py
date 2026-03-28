from src.storage import models 
from src.storage.db_setup import create_tables


def main():
    create_tables()
    print("SQLite tables created.")


if __name__ == "__main__":
    main()