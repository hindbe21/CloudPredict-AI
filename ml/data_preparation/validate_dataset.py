
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument(
    "--input",
    default="hdfs://namenode:9000/cloudpredict/gold/livrable3/ml/v1"
)

args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("CloudPredict-L3-DatasetValidation")
    .config("spark.sql.shuffle.partitions", "32")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

LABEL = "failure_next_7_days"

SMART_COLUMNS = [
    f"smart_{i}_{kind}"
    for i in [5, 9, 194, 197, 198]
    for kind in ["raw", "normalized"]
]

EXPECTED_SCHEMA = {
    "date": "date",
    "serial_number": "string",
    "model": "string",
    "capacity_bytes": "bigint",
    LABEL: "int"
}

EXPECTED_SCHEMA.update({
    c: "double" for c in SMART_COLUMNS
})

EXPECTED = {
    "train": (
        2066288, 432, 85,
        "2025-10-01", "2025-10-31"
    ),
    "validation": (
        1075014, 206, 39,
        "2025-11-08", "2025-11-23"
    ),
    "test": (
        1618193, 133, 27,
        "2025-12-01", "2025-12-24"
    )
}

try:

    total = 0
    positives = 0

    for name, expected in EXPECTED.items():

        print(f"\n=== VALIDATION : {name.upper()} ===")

        df = spark.read.parquet(
            f"{args.input}/{name}"
        )

        if dict(df.dtypes) != EXPECTED_SCHEMA:
            raise ValueError(
                f"Schema incorrect pour {name}"
            )

        print("Schema : OK")
        print("Nombre de colonnes :", len(df.columns))

        stats = df.agg(
            F.count("*").alias("observations"),
            F.sum(LABEL).alias("positives"),
            F.countDistinct(
                F.when(
                    F.col(LABEL) == 1,
                    F.col("serial_number")
                )
            ).alias("disques_positifs"),
            F.min("date").alias("date_debut"),
            F.max("date").alias("date_fin"),
            F.count(
                F.when(
                    F.col(LABEL).isNull() |
                    (~F.col(LABEL).isin(0, 1)),
                    1
                )
            ).alias("labels_invalides")
        ).first()

        observed = (
            stats["observations"],
            stats["positives"],
            stats["disques_positifs"],
            str(stats["date_debut"]),
            str(stats["date_fin"])
        )

        if observed != expected:
            raise ValueError(
                f"Statistiques incorrectes pour {name}"
            )

        if stats["labels_invalides"] != 0:
            raise ValueError("Labels invalides detectes")

        for key, value in stats.asDict().items():
            print(f"{key}: {value}")

        if name == "train":

            available = df.agg(*[
                F.count(c).alias(c)
                for c in SMART_COLUMNS
            ]).first()

            for c in SMART_COLUMNS:
                if available[c] == 0:
                    raise ValueError(
                        f"Variable vide dans Train : {c}"
                    )

            print("Disponibilite des variables SMART : OK")

        total += stats["observations"]
        positives += stats["positives"]

        print(f"Dataset {name} : VALIDE")

    print("\n=== BILAN FINAL ===")
    print("Datasets valides : 3/3")
    print("Total observations :", total)
    print("Total positifs :", positives)
    print("SUCCES : DATASETS ML VALIDES")

finally:
    spark.stop()
