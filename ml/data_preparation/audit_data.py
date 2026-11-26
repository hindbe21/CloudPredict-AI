
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument(
    "--input",
    default="hdfs://namenode:9000/cloudpredict/tests/backblaze_multiday_parquet"
)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("CloudPredict-L3-DataAudit")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(args.input)

    print("\n=== 1. SCHEMA DES DONNEES ===")
    df.printSchema()

    print("\n=== 2. STATISTIQUES GENERALES ===")
    df.agg(
        F.count("*").alias("observations"),
        F.countDistinct("serial_number").alias("disques"),
        F.countDistinct("model").alias("modeles"),
        F.countDistinct("date").alias("jours"),
        F.min("date").alias("date_debut"),
        F.max("date").alias("date_fin"),
        F.sum(F.when(F.col("failure") == 1, 1)
              .otherwise(0)).alias("pannes")
    ).show(truncate=False)

    print("\n=== 3. OBSERVATIONS PAR JOUR ===")
    df.groupBy("date").agg(
        F.count("*").alias("observations"),
        F.sum(F.when(F.col("failure") == 1, 1)
              .otherwise(0)).alias("pannes")
    ).orderBy("date").show(100, truncate=False)

    print("\n=== 4. DISTRIBUTION DES CLASSES ===")
    df.groupBy("failure").count().show()

    print("\n=== 5. VARIABLES SMART DISPONIBLES ===")
    smart_columns = [
        c for c in df.columns if c.startswith("smart_")
    ]

    print("Nombre de variables SMART :", len(smart_columns))
    print("Variables SMART :", smart_columns)

    print("\n=== 6. COUVERTURE PAR DISQUE ===")
    df.groupBy("serial_number").agg(
        F.countDistinct("date").alias("jours_observes")
    ).groupBy("jours_observes").count().orderBy(
        "jours_observes"
    ).show()

    print("\n=== 7. DOUBLONS METIER ===")
    duplicates = df.groupBy("serial_number", "date").count()
    print("Cles dupliquees :", duplicates.filter(
        F.col("count") > 1
    ).count())

    print("\nSUCCES : AUDIT TERMINE")

finally:
    spark.stop()
