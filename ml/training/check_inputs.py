"""Controle en lecture seule des splits Train et Validation fournis par Hind."""
import argparse
from pyspark.sql import SparkSession, functions as F


LABEL = "failure_next_7_days"
SMART = [f"smart_{i}_{kind}" for i in (5, 9, 194, 197, 198)
         for kind in ("raw", "normalized")]
REQUIRED = ["date", "serial_number", "model", "capacity_bytes", *SMART, LABEL]
EXPECTED = {
    "train": (2066288, 432, "2025-10-01", "2025-10-31"),
    "validation": (1075014, 206, "2025-11-08", "2025-11-23"),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="hdfs://namenode:9000/cloudpredict/gold/livrable3/ml/v1")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("CloudPredict-L3-CheckInputs")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    try:
        spark.sparkContext.setLogLevel("WARN")
        for split, (count, positives, start, end) in EXPECTED.items():
            print(f"\n=== {split.upper()} ===", flush=True)
            df = spark.read.parquet(f"{args.base.rstrip('/')}/{split}")
            df.printSchema()
            missing = set(REQUIRED) - set(df.columns)
            if missing:
                raise ValueError(f"Colonnes absentes : {sorted(missing)}")
            date = F.to_date("date")
            bad_label = F.col(LABEL).isNull() | ~F.col(LABEL).isin(0, 1)
            stats = df.agg(
                F.count("*").alias("rows"),
                F.sum(F.when(F.col(LABEL) == 1, 1).otherwise(0)).alias("positives"),
                F.sum(F.when(bad_label, 1).otherwise(0)).alias("invalid_labels"),
                F.sum(F.when(date.isNull(), 1).otherwise(0)).alias("invalid_dates"),
                F.min(date).alias("start"), F.max(date).alias("end"),
                *[F.sum(F.when(F.col(c).cast("double").isNull()
                               | F.isnan(F.col(c).cast("double")), 1).otherwise(0))
                  .alias(f"missing_{c}") for c in ["capacity_bytes", *SMART]],
            ).first().asDict()
            print("Observations :", stats["rows"])
            print("Positives :", stats["positives"])
            print("Negatives :", stats["rows"] - stats["positives"])
            print("Periode :", stats["start"], "->", stats["end"])
            for c in ["capacity_bytes", *SMART]:
                n = stats[f"missing_{c}"]
                print(f"Manquants {c} : {n}/{stats['rows']}")
            if (stats["rows"] != count or stats["positives"] != positives
                    or stats["invalid_labels"] or stats["invalid_dates"]
                    or str(stats["start"]) != start or str(stats["end"]) != end):
                raise ValueError(f"{split} ne correspond pas aux valeurs attendues de Hind.")
            print(f"VALIDATION {split.upper()} REUSSIE", flush=True)
        print("\nDONNEES TRAIN ET VALIDATION CONFORMES", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
