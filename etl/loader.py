# loader.py
import pandas as pd
from tqdm import tqdm
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import os, sys
import logging


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from database.models import Database


# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    filename="err.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class Loader:
    def __init__(self, database_url):
        self.database = Database(database_url)

    def load_dataframes_to_database(self, dataframes):
        self.database.create_table()

        with tqdm(
            total=len(dataframes),
            desc="Loading dataframes into database",
            colour="green",
        ) as pbar:
            for df in dataframes:
                try:
                    self.database.insert_data(df)
                except IntegrityError as e:
                    logging.error(e)
                finally:
                    pbar.update(1)

        return True
