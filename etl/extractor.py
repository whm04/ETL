import pandas as pd
import os
from tqdm import tqdm
import time
from typing import List


class Extractor:
    def __init__(self, directory: str):
        """
        Initialize Extractor object with the directory containing CSV files.

        Args:
            directory (str): Directory path containing CSV files.
        """
        self.directory = directory

    def extract(self):
        """
        Extract data from CSV files in the specified directory.

        Returns:
            List[pd.DataFrame]: List of pandas DataFrames extracted from CSV files.
        """
        dataframes = []
        csv_files = self.list_csv_files()
        with tqdm(
            total=len(csv_files), desc="Extracting CSV files", colour="green"
        ) as pbar:
            for filename in csv_files:
                try:
                    time.sleep(1)  # Simulating data extraction delay
                    df = pd.read_csv(os.path.join(self.directory, filename))
                    dataframes.append(df)
                except FileNotFoundError:
                    print(f"Error: File {filename} not found in {self.directory}")
                pbar.update(1)

        return dataframes

    def list_csv_files(self):
        """
        List CSV files in the specified directory.

        Returns:
            List[str]: List of CSV file names.
        """
        # Filter files with .csv extension (modify if needed)
        return [file for file in os.listdir(self.directory) if file.endswith(".csv")]
