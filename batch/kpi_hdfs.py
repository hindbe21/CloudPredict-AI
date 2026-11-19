import argparse
from pyspark.sql import SparkSession, functions as F

parser = argparse.ArgumentParser()
parser.add_argument(
    "--input",
    default="hdfs://namenode:9000/cloudpredict/tests/backblaze_parquet"
)
parser.add_argument(
    "--output",
    default="hdfs://namenode:9000/cloudpredict/tests/kpi_results"
)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("CloudPredict-Batch-SparkSQL")
    .config("spark.hadoop.dfs.replication", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(args.input)

    # Compatible avec les Parquet de test et ceux d'Aya
    date_col = "event_date" if "event_date" in df.columns else "date"

    required = {"serial_number", "model", "failure", date_col}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Colonnes manquantes : {sorted(missing)}")

    total = df.count()

    # Exclure les lignes invalides signalees par le streaming
    if "is_invalid" in df.columns:
        df = df.filter(F.col("is_invalid") == False)

    # Normaliser les colonnes analytiques
    df = df.select(
        F.to_date(F.col(date_col)).alias("event_date"),
        F.trim(F.col("serial_number")).alias("serial_number"),
        F.coalesce(F.col("model"), F.lit("INCONNU")).alias("model"),
        F.col("failure").cast("int").alias("failure")
    )

    df = df.filter(
        F.col("event_date").isNotNull()
        & F.col("serial_number").isNotNull()
        & (F.length("serial_number") > 0)
        & F.col("failure").isin(0, 1)
    )

    valides = df.count()

    print("Observations lues :", total)
    print("Observations valides :", valides)
    print("Observations exclues :", total - valides)

    df.createOrReplaceTempView("drive_stats")

    # KPI globaux
    global_kpi = spark.sql("""
        SELECT
            COUNT(*) AS observations,
            COUNT(DISTINCT serial_number) AS disques_distincts,
            SUM(CASE WHEN failure = 1 THEN 1 ELSE 0 END)
                AS observations_avec_panne,
            CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE
                ROUND(
                    100.0 * SUM(
                        CASE WHEN failure = 1 THEN 1 ELSE 0 END
                    ) / COUNT(*), 4
                )
            END AS pct_observations_avec_panne
        FROM drive_stats
    """)

    # KPI par modele
    model_kpi = spark.sql("""
        SELECT
            model AS modele,
            COUNT(*) AS observations,
            COUNT(DISTINCT serial_number) AS disques_distincts,
            SUM(CASE WHEN failure = 1 THEN 1 ELSE 0 END)
                AS observations_avec_panne
        FROM drive_stats
        GROUP BY model
    """)

    # KPI par jour
    daily_kpi = spark.sql("""
        SELECT
            event_date AS date,
            COUNT(*) AS observations,
            COUNT(DISTINCT serial_number) AS disques_distincts,
            SUM(CASE WHEN failure = 1 THEN 1 ELSE 0 END)
                AS observations_avec_panne
        FROM drive_stats
        GROUP BY event_date
    """)

    print("\n=== KPI GLOBAUX ===")
    global_kpi.show(truncate=False)

    print("\n=== KPI PAR MODELE ===")
    model_kpi.orderBy(
        "observations", ascending=False
    ).show(20, truncate=False)

    print("\n=== KPI PAR JOUR ===")
    daily_kpi.orderBy("date").show(20, truncate=False)

    # Enregistrer les resultats dans HDFS
    global_kpi.write.mode("overwrite").parquet(args.output + "/global")
    model_kpi.write.mode("overwrite").parquet(args.output + "/par_modele")
    daily_kpi.write.mode("overwrite").parquet(args.output + "/par_jour")

    print("\nSUCCES : KPI CALCULES ET ENREGISTRES")
    print("Destination :", args.output)

finally:
    spark.stop()
