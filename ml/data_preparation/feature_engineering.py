
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import SQLTransformer
from pyspark import StorageLevel


SMART_IDS = [5, 9, 194, 197, 198]

SMART_COLUMNS = [
    f"smart_{i}_{kind}"
    for i in SMART_IDS
    for kind in ["raw", "normalized"]
]

RAW_FEATURES = [
    "model",
    "capacity_bytes"
] + SMART_COLUMNS

DERIVED_FEATURES = [
    "capacity_gb",
    "disk_age_days",
    "log_reallocated_sectors",
    "log_pending_sectors",
    "log_uncorrectable_sectors"
]

MISSING_FEATURES = [
    f"smart_{i}_missing"
    for i in SMART_IDS
]

LABEL = "failure_next_7_days"


def feature_transformer():

    expressions = [
        "capacity_bytes / 1000000000.0 AS capacity_gb",

        """CASE WHEN smart_9_raw >= 0
           THEN smart_9_raw / 24.0
           ELSE NULL END AS disk_age_days""",

        """CASE WHEN smart_5_raw >= 0
           THEN LOG1P(smart_5_raw)
           ELSE NULL END AS log_reallocated_sectors""",

        """CASE WHEN smart_197_raw >= 0
           THEN LOG1P(smart_197_raw)
           ELSE NULL END AS log_pending_sectors""",

        """CASE WHEN smart_198_raw >= 0
           THEN LOG1P(smart_198_raw)
           ELSE NULL END AS log_uncorrectable_sectors"""
    ]

    for i in SMART_IDS:
        expressions.append(
            f"""CASE WHEN smart_{i}_raw IS NULL
                THEN 1.0 ELSE 0.0 END
                AS smart_{i}_missing"""
        )

    statement = (
        "SELECT *, "
        + ", ".join(expressions)
        + " FROM __THIS__"
    )

    return SQLTransformer(statement=statement)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        default="hdfs://namenode:9000/cloudpredict/silver/livrable3/drives_labeled"
    )

    parser.add_argument(
        "--output",
        default="hdfs://namenode:9000/cloudpredict/gold/livrable3/drives_features"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("CloudPredict-L3-FeatureEngineering")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        print("\n=== 1. LECTURE DES DONNEES LABELLISEES ===")

        df = spark.read.parquet(args.input)

        selected = df.select(
            "date",
            "serial_number",
            *RAW_FEATURES,
            LABEL
        )

        print("\n=== 2. FEATURE ENGINEERING ===")

        features = feature_transformer().transform(
            selected
        ).persist(StorageLevel.DISK_ONLY)

        features.printSchema()

        print("\n=== 3. VARIABLES CREEES ===")

        for col in DERIVED_FEATURES + MISSING_FEATURES:
            print(col)

        print("\n=== 4. STATISTIQUES GENERALES ===")

        stats = features.agg(
            F.count("*").alias("observations"),
            F.countDistinct(
                "serial_number"
            ).alias("disques"),
            F.sum(LABEL).alias("positives"),
            F.min("date").alias("date_debut"),
            F.max("date").alias("date_fin")
        ).first()

        for key, value in stats.asDict().items():
            print(f"{key}: {value}")

        if stats["observations"] != 5698922:
            raise ValueError(
                "Nombre d'observations inattendu"
            )

        if stats["positives"] != 957:
            raise ValueError(
                "Nombre de labels positifs inattendu"
            )

        print("\n=== 5. EXEMPLES DE FEATURES ===")

        features.select(
            "date",
            "model",
            "capacity_gb",
            "disk_age_days",
            "log_reallocated_sectors",
            "log_pending_sectors",
            LABEL
        ).show(10, truncate=False)

        print("\n=== 6. STATISTIQUES DES VARIABLES ===")

        features.select(
            *DERIVED_FEATURES
        ).summary(
            "count", "mean", "stddev",
            "min", "50%", "max"
        ).show(truncate=False)

        print("\n=== 7. DISTRIBUTION DES CLASSES ===")

        features.groupBy(LABEL).count().show()

        print("\n=== 8. SAUVEGARDE GOLD ===")

        (
            features.coalesce(16)
            .write
            .mode("errorifexists")
            .option("compression", "snappy")
            .parquet(args.output)
        )

        features.unpersist()

        print("Chemin de sortie :", args.output)
        print("SUCCES : FEATURE ENGINEERING TERMINE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
