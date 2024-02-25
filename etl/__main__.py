from extractor import Extractor
from loader import Loader
from transformer import Transformer  # Optional
from config.config import INPUT_CSV_DIRECTORY, DATABASE_URL


def main():
    extractor = Extractor(INPUT_CSV_DIRECTORY)
    dataframes = extractor.extract()
    transformer = Transformer()
    transformed_dataframes = transformer.transform_dataframes(dataframes)

    # Optional: Transform data
    loader = Loader(DATABASE_URL)
    loader.load_dataframes_to_database(transformed_dataframes)

    print("ETL pipeline completed.")


if __name__ == "__main__":
    main()
