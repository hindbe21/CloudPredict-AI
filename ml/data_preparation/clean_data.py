
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    StringType,
    LongType,
    IntegerType,
    DoubleType
)
from pyspark import StorageLevel

parser = argparse.ArgumentParser()

parser.add_argument(
    "--input",
    default="hdfs://namenode:9000/cloudpredict/staging/livrable3/q4_2025_csv"
)

parser.add_argument(
    "--output",
    default="hdfs://namenode:9000/cloudpredict/silver/livrable3/drives_clean"
)

parser.add_argument("--expected-rows", type=int)
parser.add_argument("--expected-failures", type=int)

args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("CloudPredict-L3-DataCleaning")
    .config("spark.sql.shuffle.partitions", "32")
    .config("spark.sql.csv.parser.columnPruning.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

smart_columns = []

for smart in [5, 9, 187, 188, 194, 197, 198]:
    smart_columns.extend([
        f"smart_{smart}_raw",
        f"smart_{smart}_normalized"
    ])

schema = StructType([
    StructField("date", DateType(), True),
    StructField("serial_number", StringType(), True),
    StructField("model", StringType(), True),
    StructField("capacity_bytes", LongType(), True),
    StructField("failure", IntegerType(), True)
] + [
    StructField(c, DoubleType(), True)
    for c in smart_columns
])

try:

    print("\n=== 1. LECTURE DES DONNEES HDFS ===")

    df = (
        spark.read
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("mode", "FAILFAST")
        .option("enforceSchema", "false")
        .schema(schema)
        .csv(args.input)
        .persist(StorageLevel.DISK_ONLY)
    )

    stats = df.agg(
        F.count("*").alias("observations"),
        F.count(F.when(F.col("failure") == 1, 1)).alias("pannes"),
        F.countDistinct("date").alias("jours"),
        F.min("date").alias("date_debut"),
        F.max("date").alias("date_fin")
    ).first()

    print("Observations originales :", stats["observations"])
    print("Pannes originales :", stats["pannes"])
    print("Jours disponibles :", stats["jours"])
    print("Date debut :", stats["date_debut"])
    print("Date fin :", stats["date_fin"])

    if args.expected_rows is not None:
        if stats["observations"] != args.expected_rows:
            raise ValueError("Nombre d'observations inattendu")

    if args.expected_failures is not None:
        if stats["pannes"] != args.expected_failures:
            raise ValueError("Nombre de pannes inattendu")

    print("\n=== 2. SCHEMA DES DONNEES ===")

    df.printSchema()

    print("\n=== 3. NETTOYAGE DES CHAMPS ===")

    clean = (
        df.withColumn("serial_number", F.trim("serial_number"))
          .withColumn("model", F.trim("model"))
    )

    for c in smart_columns:
        clean = clean.withColumn(
            c,
            F.when(
                F.isnan(F.col(c)) |
                (F.abs(F.col(c)) == float("inf")),
                F.lit(None).cast("double")
            ).otherwise(F.col(c))
        )

    print("\n=== 4. CONTROLE DES CHAMPS OBLIGATOIRES ===")

    valid_condition = (
        F.col("date").isNotNull() &
        F.col("serial_number").isNotNull() &
        (F.col("serial_number") != "") &
        F.col("model").isNotNull() &
        (F.col("model") != "") &
        F.col("capacity_bytes").isNotNull() &
        (F.col("capacity_bytes") > 0) &
        F.col("failure").isNotNull() &
        F.col("failure").isin(0, 1)
    )

    clean = clean.filter(valid_condition)

    valid_count = clean.count()

    print(
        "Observations invalides exclues :",
        stats["observations"] - valid_count
    )

    print("\n=== 5. CONTROLE DES DOUBLONS ===")

    clean = clean.dropDuplicates().persist(
        StorageLevel.DISK_ONLY
    )

    clean_count = clean.count()

    print(
        "Doublons exacts supprimes :",
        valid_count - clean_count
    )

    conflicts = (
        clean.groupBy("serial_number", "date")
        .count()
        .filter(F.col("count") > 1)
    )

    conflict_count = conflicts.count()

    print("Cles metier contradictoires :", conflict_count)

    if conflict_count > 0:
        conflicts.show(20, truncate=False)
        raise ValueError("Conflits metier a examiner")

    df.unpersist()

    print("\n=== 6. QUALITE DES VARIABLES SMART ===")

    missing = clean.agg(*[
        F.round(
            F.avg(
                F.when(F.col(c).isNull(), 1).otherwise(0)
            ) * 100,
            2
        ).alias(c)
        for c in smart_columns
    ]).first()

    for c in smart_columns:
        print(f"{c}: {missing[c]}%")

    print("\n=== 7. STATISTIQUES FINALES ===")

    clean.agg(
        F.count("*").alias("observations"),
        F.countDistinct("serial_number").alias("disques"),
        F.countDistinct("model").alias("modeles"),
        F.countDistinct("date").alias("jours"),
        F.sum("failure").alias("pannes")
    ).show(truncate=False)

    print("\n=== 8. SAUVEGARDE PARQUET DANS HDFS ===")

    (
        clean.write
        .mode("errorifexists")
        .option("compression", "snappy")
        .partitionBy("date")
        .parquet(args.output)
    )

    clean.unpersist()

    print("\nChemin de sortie :", args.output)
    print("SUCCES : NETTOYAGE TERMINE")

finally:
    spark.stop()