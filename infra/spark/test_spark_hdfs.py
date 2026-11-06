from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum


spark = (
    SparkSession.builder
    .appName("CloudPredictAI-Test-Spark-HDFS")
    .config("spark.hadoop.dfs.replication", "2")
    .getOrCreate()
)

try:
    if not spark.sparkContext.master.startswith("spark://"):
        raise RuntimeError("Soumettre ce test au cluster Spark Standalone.")

    # Un chemin unique conserve les resultats des executions precedentes.
    path = "hdfs://namenode:9000/tmp/cloudpredict-tests/" + uuid4().hex
    spark.range(0, 1000, 1, 2).write.mode("errorifexists").parquet(path)

    result = spark.read.parquet(path).agg(
        {"id": "count"}
    ).first()[0]
    total = spark.read.parquet(path).select(spark_sum("id")).first()[0]

    print("Chemin HDFS :", path)
    print("Nombre de lignes :", result)
    print("Somme calculee :", total)
    if result != 1000 or total != 499500:
        raise RuntimeError("Les donnees relues depuis HDFS sont incorrectes.")
    print("TEST SPARK HDFS REUSSI")
finally:
    spark.stop()
