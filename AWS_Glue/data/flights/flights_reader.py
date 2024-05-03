from pyspark.sql import DataFrame
from ...common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from ...context.context import logger
from .flights_schema import (
    FLIGHTS_SCHEMA,

    PREFIX_PATH_FLIGHTS,
)

N_APPLIC_INFQ_VALUE = 38

# Définition de la classe FlightsReader
class FlightsReader:
    # Constructeur de la classe
    def __init__(self, path: str) -> None:
        # Initialisation du chemin du fichier
        self.path = path

    # Méthode pour lire les données
    def read(self) -> DataFrame:
        # Log de début de lecture
        logger.info("start reading table")
        # Lecture du fichier Parquet et sélection des colonnes définies dans FLIGHTS_SCHEMA
        flights_df: DataFrame = read_from_parquet(
            self.path
        ).select(*FLIGHTS_SCHEMA.fieldNames())
        
        
        # Création d'un DataFrame avec le schéma défini dans FLIGHTS_SCHEMA
        return create_df_with_schema(
            flights_df,
            FLIGHTS_SCHEMA,
        )
