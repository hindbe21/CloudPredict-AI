"""Controle batch des Parquet produits par le streaming."""
import argparse
from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default="hdfs://namenode:9000/cloudpredict/bronze/drives_raw")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("CloudPredictAI-Verify-Bronze").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        df = spark.read.parquet(args.path).cache()
        count = df.count()
        invalid = df.filter("is_invalid").count()
        unique = df.select("kafka_topic", "kafka_partition", "kafka_offset").distinct().count()
        print("Nombre de lignes :", count)
        print("Lignes invalides :", invalid)
        print("Doublons topic/partition/offset :", count - unique)
        df.select("date", "serial_number", "model", "failure", "kafka_key", "kafka_partition", "kafka_offset", "is_invalid").show(10, truncate=False)
        df.groupBy("kafka_topic", "kafka_partition").agg(
            F.count("*").alias("rows"), F.min("kafka_offset").alias("min_offset"),
            F.max("kafka_offset").alias("max_offset")
        ).show(truncate=False)
        if not count or count != unique:
            raise RuntimeError("Bronze vide ou offsets dupliques : verifier le test.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
