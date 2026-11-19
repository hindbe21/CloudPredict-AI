import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument(
    "--input",
    default="hdfs://namenode:9000/cloudpredict/tests/backblaze_csv/backblaze_sample_2000.csv"
)

parser.add_argument(
    "--output",
    default="hdfs://namenode:9000/cloudpredict/tests/backblaze_parquet"
)

args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("CloudPredict-Prepare-Backblaze")
    .config("spark.hadoop.dfs.replication", "2")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(args.input)
    )

    required = [
        "date",
        "serial_number",
        "model",
        "capacity_bytes",
        "failure"
    ]

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError("Colonnes manquantes : " + ", ".join(missing))

    cleaned = df.select(
        F.to_date("date").alias("date"),
        F.col("serial_number"),
        F.col("model"),
        F.col("capacity_bytes").cast("long").alias("capacity_bytes"),
        F.col("failure").cast("int").alias("failure")
    )

    print("\n=== NOMBRE TOTAL D'OBSERVATIONS ===")
    print(cleaned.count())

    print("\n=== OBSERVATIONS ET PANNES PAR JOUR ===")

    cleaned.groupBy("date").agg(
        F.count("*").alias("observations"),
        F.sum(
            F.when(F.col("failure") == 1, 1).otherwise(0)
        ).alias("pannes")
    ).orderBy("date").show(truncate=False)

    print("\n=== CONVERSION PARQUET ===")

    cleaned.repartition(4).write.mode("overwrite").parquet(
        args.output
    )

    print("PARQUET CREE AVEC SUCCES")
    print("Destination :", args.output)

finally:
    spark.stop()
