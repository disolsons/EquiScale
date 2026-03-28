from src.storage import models 
from src.storage.db_setup import reset_all_data


def main():
    reset_all_data()
    print("All data deleted and tables reset.")


if __name__ == "__main__":
    main()