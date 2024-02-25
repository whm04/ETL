import pandas as pd
from config.config import TABLE_NAME
from tqdm import tqdm


class Transformer:
    def __init__(self):
        pass

    def transform_dataframes(self, dataframes):
        transformed_dataframes = []
        with tqdm(
            total=len(dataframes), desc="Transform CSV files", colour="green"
        ) as pbar:
            for df in dataframes:
                # Perform transformations to match database columns
                df.rename(
                    columns={
                        "numeroOffre": "offer_number",
                        "adresseOffre": "offer_address",
                        "intitule": "title",
                        "datePublication": "publication_date",
                        "LANGUE": "language",
                        "SAVOIR_ETRE": "soft_skills",
                        "SAVOIR_FAIRE": "hard_skills",
                        "salaireTexte": "salary",
                        "entreprise": "company",
                        "profil": "profile",
                    },
                    inplace=True,
                )

                # Convert 'publication_date' column to datetime format
                df["publication_date"] = pd.to_datetime(df["publication_date"])

                transformed_dataframes.append(df)
                pbar.update(1)

        return transformed_dataframes
