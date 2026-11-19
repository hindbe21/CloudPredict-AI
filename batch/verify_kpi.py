from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("CloudPredict-Verification-KPI")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://namenode:9000/cloudpredict/tests/kpi_multiday"

try:
    global_kpi = spark.read.parquet(base + "/global")
    daily_kpi = spark.read.parquet(base + "/par_jour")
    model_kpi = spark.read.parquet(base + "/par_modele")

    print("\n=== KPI GLOBAUX ===")
    global_kpi.show(truncate=False)

    print("\n=== KPI PAR JOUR ===")
    daily_kpi.orderBy("date").show(truncate=False)

    print("\n=== MODELES AVEC PANNES ===")
    model_kpi.filter(
        F.col("observations_avec_panne") > 0
    ).orderBy(
        F.desc("observations_avec_panne")
    ).show(50, truncate=False)

    # Verification des totaux
    total = global_kpi.first()
    daily = {
        str(r["date"]): int(r["observations_avec_panne"])
        for r in daily_kpi.collect()
    }

    assert total["observations"] == 1004671
    assert total["observations_avec_panne"] == 26

    assert daily == {
        "2025-10-01": 4,
        "2025-11-01": 14,
        "2025-12-01": 8
    }

    print("\nVALIDATION REUSSIE : TOUS LES KPI CORRESPONDENT")

finally:
    spark.stop()
