import sys
from ..config.config import ACCESS_KEY_ID, SECRET_ACCESS_KEY

# Essayer d'importer les modules nécessaires pour AWS Glue et PySpark
try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import SparkConf
    from pyspark.context import SparkContext

    # Récupération des arguments passés au script
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTRYPOINT", "ENV"])

    ## Access Datalake objects using PySpark
    # conf spark
    conf = (
        SparkConf()
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY_ID)
        .set("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY)
        .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    )
    # Création d'un contexte Spark avec la configuration définie
    sc = SparkContext(conf=conf)
    # Définition du niveau de journalisation à "TRACE"
    sc.setLogLevel("TRACE")
    # Création d'un contexte Glue à partir du contexte Spark
    glueContext = GlueContext(sc)
    # Récupération du journal de Glue
    logger = glueContext.get_logger()
    # Récupération de la session Spark à partir du contexte Glue
    spark = glueContext.spark_session
    # Création d'un job Glue
    job = Job(glueContext)

# Si les modules nécessaires pour AWS Glue et PySpark ne sont pas disponibles
except ImportError:
    import logging
 # Initialisation de Spark à une chaîne vide
    spark = ""
    logging.basicConfig(
        format="%(asctime)s\t%(module)s\t%(levelname)s\t%(message)s", level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    logger.warning("Package awsglue not found! Excepted if you run the code locally")
