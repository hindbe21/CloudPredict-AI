from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


spark = SparkSession.builder.appName("CloudPredictAI-Test-Cluster").getOrCreate()

try:
    sc = spark.sparkContext
    print("Master utilise :", sc.master)
    print("Application :", sc.applicationId)
    if not sc.master.startswith("spark://"):
        raise RuntimeError("Soumettre ce test au cluster Spark Standalone.")

    df = spark.range(0, 100000, 1, 8)
    count = df.count()
    total = df.select(spark_sum(col("id")).alias("total")).first()["total"]

    print("Nombre de lignes :", count)
    print("Somme calculee :", total)
    if count != 100000 or total != 4999950000:
        raise RuntimeError("Le resultat du calcul est incorrect.")
    print("TEST SPARK REUSSI")
finally:
    spark.stop()
