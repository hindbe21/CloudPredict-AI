
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark import StorageLevel

parser = argparse.ArgumentParser()

parser.add_argument(
    "--input",
    default="hdfs://namenode:9000/cloudpredict/silver/livrable3/drives_clean"
)

parser.add_argument(
    "--output",
    default="hdfs://namenode:9000/cloudpredict/silver/livrable3/drives_labeled"
)

parser.add_argument("--horizon", type=int, default=7)

args = parser.parse_args()

if args.horizon < 1:
    parser.error("L'horizon doit etre strictement positif")

spark = (
    SparkSession.builder
    .appName("CloudPredict-L3-LabelConstruction")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:

    print("\n=== 1. LECTURE DU DATASET SILVER ===")

    df = spark.read.parquet(args.input)

    stats = df.agg(
        F.count("*").alias("observations"),
        F.min("date").alias("date_debut"),
        F.max("date").alias("date_fin"),
        F.countDistinct(
            F.when(F.col("failure") == 1, F.col("serial_number"))
        ).alias("disques_en_panne")
    ).first()

    print("Observations :", stats["observations"])
    print("Date debut :", stats["date_debut"])
    print("Date fin :", stats["date_fin"])
    print("Disques distincts en panne :", stats["disques_en_panne"])
    print("Horizon de prediction :", args.horizon, "jours")

    if not stats["date_fin"]:
        raise ValueError("Dataset vide")

    print("\n=== 2. CONSTRUCTION DES FENETRES TEMPORELLES ===")

    df = df.withColumn(
        "_day",
        F.datediff(F.col("date"), F.lit("1970-01-01"))
    )

    disk_window = (
        Window.partitionBy("serial_number")
        .orderBy("_day")
    )

    future_window = disk_window.rangeBetween(
        1, args.horizon
    )

    past_window = disk_window.rangeBetween(
        Window.unboundedPreceding, 0
    )

    df = (
        df.withColumn(
            "_future_failure",
            F.max("failure").over(future_window)
        )
        .withColumn(
            "_future_days",
            F.count("*").over(future_window)
        )
        .withColumn(
            "_past_failure",
            F.max("failure").over(past_window)
        )
    )

    print("\n=== 3. CONTROLE DE L'ELIGIBILITE ===")

    cutoff = F.date_sub(
        F.lit(stats["date_fin"]),
        args.horizon
    )

    df = df.withColumn(
        "_status",
        F.when(
            F.col("_past_failure") == 1,
            "already_failed"
        )
        .when(
            F.col("date") > cutoff,
            "end_of_period"
        )
        .when(
            F.col("_future_failure") == 1,
            "positive"
        )
        .when(
            F.col("_future_days") == args.horizon,
            "negative"
        )
        .otherwise("incomplete_history")
    ).persist(StorageLevel.DISK_ONLY)

    df.groupBy("_status").count().show(
        truncate=False
    )

    print("\n=== 4. CONSTRUCTION DU LABEL ===")

    labeled = (
        df.filter(
            F.col("_status").isin("positive", "negative")
        )
        .withColumn(
            "failure_next_7_days",
            F.when(
                F.col("_status") == "positive", 1
            ).otherwise(0)
        )
        .drop(
            "failure",
            "_day",
            "_future_failure",
            "_future_days",
            "_past_failure",
            "_status"
        )
    )

    print("\n=== 5. DISTRIBUTION DES CLASSES ===")

    labeled.groupBy(
        "failure_next_7_days"
    ).count().orderBy(
        "failure_next_7_days"
    ).show()

    results = labeled.agg(
        F.count("*").alias("observations"),
        F.sum("failure_next_7_days").alias("positives"),
        F.countDistinct("serial_number").alias("disques"),
        F.countDistinct(
            F.when(
                F.col("failure_next_7_days") == 1,
                F.col("serial_number")
            )
        ).alias("disques_positifs"),
        F.min("date").alias("date_debut"),
        F.max("date").alias("date_fin")
    ).first()

    total = results["observations"]
    positives = results["positives"] or 0

    print("Observations finales :", total)
    print("Labels positifs :", positives)
    print("Labels negatifs :", total - positives)
    print("Disques distincts :", results["disques"])
    print("Disques positifs :", results["disques_positifs"])
    print("Date debut :", results["date_debut"])
    print("Date fin :", results["date_fin"])

    if total:
        print(
            "Taux de labels positifs :",
            round(100 * positives / total, 4),
            "%"
        )

    if positives == 0 or positives == total:
        raise ValueError("Les deux classes sont necessaires")

    print("\n=== 6. DISTRIBUTION MENSUELLE ===")

    (
        labeled.withColumn(
            "mois", F.date_format("date", "yyyy-MM")
        )
        .groupBy("mois")
        .agg(
            F.count("*").alias("observations"),
            F.sum("failure_next_7_days").alias("positives"),
            F.countDistinct(
                F.when(
                    F.col("failure_next_7_days") == 1,
                    F.col("serial_number")
                )
            ).alias("disques_positifs")
        )
        .orderBy("mois")
        .show(truncate=False)
    )

    print("\n=== 7. EXEMPLES DE LABELS POSITIFS ===")

    labeled.filter(
        F.col("failure_next_7_days") == 1
    ).select(
        "date",
        "serial_number",
        "model",
        "smart_5_raw",
        "smart_197_raw",
        "failure_next_7_days"
    ).show(15, truncate=False)

    print("\n=== 8. SAUVEGARDE DU DATASET LABELLISE ===")

    (
        labeled.write
        .mode("errorifexists")
        .option("compression", "snappy")
        .partitionBy("date")
        .parquet(args.output)
    )

    df.unpersist()

    print("Chemin de sortie :", args.output)
    print("SUCCES : CONSTRUCTION DES LABELS TERMINEE")

finally:
    spark.stop()
