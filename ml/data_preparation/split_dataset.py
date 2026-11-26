
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

parser = argparse.ArgumentParser()

parser.add_argument(
    "--input",
    default="hdfs://namenode:9000/cloudpredict/gold/livrable3/drives_features"
)

parser.add_argument(
    "--output",
    default="hdfs://namenode:9000/cloudpredict/gold/livrable3/ml/v1"
)

args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("CloudPredict-L3-TemporalSplit")
    .config("spark.sql.shuffle.partitions", "32")
    .config("spark.hadoop.dfs.replication", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

LABEL = "failure_next_7_days"

SMART_COLUMNS = [
    f"smart_{i}_{kind}"
    for i in [5, 9, 194, 197, 198]
    for kind in ["raw", "normalized"]
]

INPUT_COLUMNS = [
    "date",
    "serial_number",
    "model",
    "capacity_bytes"
] + SMART_COLUMNS + [LABEL]

PERIODS = {
    "train": ("2025-10-01", "2025-10-31"),
    "validation": ("2025-11-08", "2025-11-23"),
    "test": ("2025-12-01", "2025-12-24")
}

try:

    print("\n=== 1. LECTURE DU DATASET GOLD ===")

    df = spark.read.parquet(args.input)

    total = df.count()

    print("Observations originales :", total)

    if total != 5698922:
        raise ValueError("Nombre d'observations inattendu")

    print("\n=== 2. DECOUPAGE TEMPOREL ===")

    data = df.select(*INPUT_COLUMNS)

    data = data.withColumn(
        "split",
        F.when(
            F.col("date").between(*PERIODS["train"]),
            "train"
        )
        .when(
            F.col("date").between(*PERIODS["validation"]),
            "validation"
        )
        .when(
            F.col("date").between(*PERIODS["test"]),
            "test"
        )
        .otherwise("excluded")
    ).persist(StorageLevel.DISK_ONLY)

    print("\n=== 3. STATISTIQUES PAR ENSEMBLE ===")

    stats = data.groupBy("split").agg(
        F.count("*").alias("observations"),
        F.sum(LABEL).alias("positives"),
        F.count(
            F.when(F.col(LABEL) == 0, 1)
        ).alias("negatives"),
        F.countDistinct(
            "serial_number"
        ).alias("disques"),
        F.countDistinct(
            F.when(
                F.col(LABEL) == 1,
                F.col("serial_number")
            )
        ).alias("disques_positifs"),
        F.min("date").alias("date_debut"),
        F.max("date").alias("date_fin")
    )

    stats.orderBy("date_debut").show(
        truncate=False
    )

    results = {
        row["split"]: row.asDict()
        for row in stats.collect()
    }

    print("\n=== 4. VALIDATION DES ENSEMBLES ===")

    for name in PERIODS:

        result = results.get(name)

        if not result:
            raise ValueError(
                f"Dataset {name} absent"
            )

        if (
            result["positives"] == 0
            or result["negatives"] == 0
        ):
            raise ValueError(
                f"Classe manquante dans {name}"
            )

        rate = (
            100 * result["positives"]
            / result["observations"]
        )

        print(
            f"{name}: "
            f"{result['observations']} observations, "
            f"{result['positives']} positifs, "
            f"{result['disques_positifs']} disques positifs, "
            f"taux={rate:.4f}%"
        )

    print("\n=== 5. CONTROLE DES FUITES TEMPORELLES ===")

    for name in ["train", "validation"]:

        following = (
            "validation"
            if name == "train"
            else "test"
        )

        gap = (
            results[following]["date_debut"]
            - results[name]["date_fin"]
        ).days - 1

        print(
            f"Separation {name}/{following}:",
            gap,
            "jours"
        )

        if gap < 7:
            raise ValueError(
                "Separation temporelle insuffisante"
            )

    overlap = (
        data.filter(
            (F.col(LABEL) == 1) &
            (F.col("split") != "excluded")
        )
        .groupBy("serial_number")
        .agg(
            F.countDistinct("split").alias("nb_splits")
        )
        .filter(F.col("nb_splits") > 1)
        .count()
    )

    print(
        "Disques positifs communs aux ensembles :",
        overlap
    )

    if overlap > 0:
        raise ValueError(
            "Chevauchement des disques positifs"
        )

    print("\n=== 6. SAUVEGARDE DES DATASETS ML ===")

    for name in PERIODS:

        subset = (
            data.filter(F.col("split") == name)
            .drop("split")
        )

        path = f"{args.output}/{name}"

        (
            subset.coalesce(8)
            .write
            .mode("errorifexists")
            .option("compression", "snappy")
            .parquet(path)
        )

        print(f"{name} sauvegarde : {path}")

    print("\n=== 7. BILAN FINAL ===")

    excluded = results.get(
        "excluded", {}
    ).get("observations", 0)

    print("Observations originales :", total)
    print("Observations exclues :", excluded)
    print("Observations ML :", total - excluded)

    print("\nSUCCES : DECOUPAGE TEMPOREL TERMINE")

    data.unpersist()

finally:
    spark.stop()
