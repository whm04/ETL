# tests/test_etl.py
import unittest
import pandas as pd
import sys, os
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)


import unittest
from etl.extractor import Extractor
from etl.transformer import Transformer
from etl.loader import Loader

from database.models import Database


class TestExtractor(unittest.TestCase):
    def setUp(self):
        # Initialize the Extractor with a test directory containing CSV files
        self.extractor = Extractor(directory="tests/test_data")
        self.transformer = Transformer()
        
        self.test_database_url = "sqlite:///:memory:"  # In-memory SQLite database for testing
        self.loader = Loader(database_url=self.test_database_url)
        

    def test_extract(self):
        # Test that the extract method returns a list of pandas DataFrames
        dataframes = self.extractor.extract()
        self.assertIsInstance(dataframes, list)
        for df in dataframes:
            self.assertIsInstance(df, pd.DataFrame)

    def test_list_csv_files(self):
        # Test that the list_csv_files method returns a list of CSV file names
        csv_files = self.extractor.list_csv_files()
        self.assertIsInstance(csv_files, list)
        for filename in csv_files:
            self.assertTrue(filename.endswith(".csv"))
            
    def test_transform_dataframes(self):
        # Create a sample DataFrame to be transformed
        dataframes = [
            pd.DataFrame({
                "numeroOffre": [1, 2],
                "adresseOffre": ["Address 1", "Address 2"],
                "intitule": ["Title 1", "Title 2"],
                "datePublication": ["2022-01-01", "2022-01-02"],
                "LANGUE": ["English", "French"],
                "SAVOIR_ETRE": ["Soft Skills 1", "Soft Skills 2"],
                "SAVOIR_FAIRE": ["Hard Skills 1", "Hard Skills 2"],
                "salaireTexte": ["Salary 1", "Salary 2"],
                "entreprise": ["Company 1", "Company 2"],
                "profil": ["Profile 1", "Profile 2"]
            })
        ]

        # Transform the sample DataFrame
        transformed_dataframes = self.transformer.transform_dataframes(dataframes)

        # Check if the transformation is correct
        self.assertEqual(len(transformed_dataframes), 1)
        transformed_df = transformed_dataframes[0]
        self.assertTrue("offer_number" in transformed_df.columns)
        self.assertTrue("offer_address" in transformed_df.columns)
        self.assertTrue("title" in transformed_df.columns)
        self.assertTrue("publication_date" in transformed_df.columns)
        self.assertTrue("language" in transformed_df.columns)
        self.assertTrue("soft_skills" in transformed_df.columns)
        self.assertTrue("hard_skills" in transformed_df.columns)
        self.assertTrue("salary" in transformed_df.columns)
        self.assertTrue("company" in transformed_df.columns)
        self.assertTrue("profile" in transformed_df.columns)
        # Add more assertions as needed to check the transformation logic

    def test_load_dataframes_to_database(self):
        # Create a sample DataFrame to be loaded into the database
        sample_df = pd.DataFrame({
                "offer_number": [1, 2],
                "offer_address": ["Address 1", "Address 2"],
                "title": ["Title 1", "Title 2"],
                "publication_date": ["2022-01-01", "2022-01-02"],
                "language": ["English", "French"],
                "soft_skills": ["Soft Skills 1", "Soft Skills 2"],
                "hard_skills": ["Hard Skills 1", "Hard Skills 2"],
                "salary": ["Salary 1", "Salary 2"],
                "company": ["Company 1", "Company 2"],
                "profile": ["Profile 1", "Profile 2"]
            })
        dataframes = [sample_df]

        # Load the sample DataFrame into the test database
        transformed_dataframes = self.transformer.transform_dataframes(dataframes)
        
        
        self.assertEqual(self.loader.load_dataframes_to_database(transformed_dataframes), True)

if __name__ == '__main__':
    unittest.main()
