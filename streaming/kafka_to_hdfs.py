"""Archivage Bronze : JSON brut, colonnes Backblaze et metadonnees Kafka."""
import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType


def bronze_frame(source, schema):
    parsed = source.select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("payload", F.from_json("raw_json", schema, {
        "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"
    }))
    return parsed.select("*", "payload.*").drop("payload").withColumn(
        "event_date", F.to_date("date", "yyyy-MM-dd")
    ).withColumn("ingested_at", F.current_timestamp()).withColumn(
        "is_invalid",
        F.col("raw_json").isNull() | F.col("_corrupt_record").isNotNull()
        | F.col("event_date").isNull()
        | F.col("serial_number").isNull()
        | (F.length(F.trim(F.col("serial_number"))) == 0)
        | F.col("failure").isNull() | ~F.col("failure").isin(0, 1)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="cloudpredict.metrics.raw")
    parser.add_argument("--output", default="hdfs://namenode:9000/cloudpredict/bronze/drives_raw")
    parser.add_argument("--checkpoint", default="hdfs://namenode:9000/cloudpredict/checkpoints/kafka_to_hdfs")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--available-now", action="store_true", help="Traiter le stock disponible puis terminer")
    args = parser.parse_args()
    if args.output.rstrip("/") == args.checkpoint.rstrip("/"):
        parser.error("Les chemins de donnees et de checkpoint doivent etre distincts.")
    schema = StructType.fromJson(json.loads(
        Path(__file__).with_name("backblaze_schema.json").read_text(encoding="utf-8")
    ))
    spark = (SparkSession.builder.appName("CloudPredictAI-Kafka-To-HDFS")
             .config("spark.hadoop.dfs.replication", "2")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    query = None
    try:
        spark.sparkContext.setLogLevel("WARN")
        source = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", args.bootstrap)
                  .option("subscribe", args.topic)
                  .option("startingOffsets", args.starting_offsets)
                  .option("failOnDataLoss", "true")
                  .option("maxOffsetsPerTrigger", 1000).load())
        writer = (bronze_frame(source, schema).writeStream
                  .queryName("cloudpredict_bronze").format("parquet")
                  .outputMode("append").option("path", args.output)
                  .option("checkpointLocation", args.checkpoint))
        writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")
        query = writer.start()
        print("Archivage :", args.output, flush=True)
        print("Checkpoint :", args.checkpoint, flush=True)
        query.awaitTermination()
        print("Traitement termine sans erreur.", flush=True)
        if query.lastProgress:
            print(json.dumps(query.lastProgress, indent=2), flush=True)
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
