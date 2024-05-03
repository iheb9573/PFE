# Importation des modules nécessaires
from enum import Enum
from typing import List, Set
from ..config.config import ACCESS_KEY_ID, SECRET_ACCESS_KEY

import boto3
from botocore.response import StreamingBody
from pyspark.sql import DataFrame

# Définition d'une énumération pour les modes d'écriture
class WriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"

# Fonction pour lister les noms des objets dans un dossier spécifique sur S3
def list_objects_names(folder_key: str, prefix: str) -> List[str]:
    # connection to Amazon S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = SECRET_ACCESS_KEY
    )
    bucket_name = folder_key.split("/")[2]

    buckets: Set = set()

    element: int = prefix.count('/')
    # Parcours des objets dans le bucket spécifié avec le préfixe donné
    for s3_object in s3.list_objects(Bucket=bucket_name, Prefix=prefix)["Contents"]:

        array_split = s3_object["Key"].split("/")
        if len(array_split) >= element:
            buckets.add(array_split[element])
    # Retourne une liste des noms des objets
    return list(buckets)

# Fonction pour écrire un DataFrame en format parquet sur S3
def write_to_parquet(df: DataFrame, output_file_path: str) -> str:
    df.write.mode(WriteMode.OVERWRITE.value).parquet(output_file_path)
    return output_file_path
