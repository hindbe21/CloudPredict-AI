"""Entrainement d'un candidat et export des predictions Validation."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.ml.classification import (
    GBTClassifier, LogisticRegression, RandomForestClassifier,
)
from pyspark.sql import SparkSession, functions as F

from preprocessing import NUMERIC_FEATURES, build_pipeline
from data_preparation.feature_engineering import RAW_FEATURES, LABEL


def class_counts(df):
    result = {row["label"]: row["count"] for row in df.groupBy("label").count().collect()}
    if set(result) != {0.0, 1.0}:
        raise ValueError(f"Labels invalides ou classe absente : {result}")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["logistic_regression", "random_forest", "gbt"], required=True)
    parser.add_argument("--data-base", default="hdfs://namenode:9000/cloudpredict/gold/livrable3/ml/v1")
    parser.add_argument("--output-base", default="hdfs://namenode:9000/cloudpredict/ml/livrable3/aya/v1")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName(f"CloudPredict-L3-{args.model}")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.hadoop.dfs.replication", "2")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    train = validation = None
    try:
        spark.sparkContext.setLogLevel("WARN")
        if not spark.sparkContext.master.startswith("spark://"):
            raise ValueError("Soumettre cet entrainement au cluster Standalone.")
        destination = f"{args.output_base.rstrip('/')}/{args.model}"
        target = spark._jvm.org.apache.hadoop.fs.Path(destination)
        if target.getFileSystem(spark._jsc.hadoopConfiguration()).exists(target):
            raise ValueError(f"Destination existante : {destination}. Choisir une nouvelle version, sans ecraser le candidat.")

        def read_split(name):
            source = spark.read.parquet(f"{args.data_base.rstrip('/')}/{name}")
            return (source.select("date", "serial_number", *RAW_FEATURES, F.col(LABEL).cast("double").alias("label"))
                    .repartition(8).persist(StorageLevel.DISK_ONLY))

        train, validation = read_split("train"), read_split("validation")
        counts, val_counts = class_counts(train), class_counts(validation)
        if counts != {0.0: 2065856, 1.0: 432} or val_counts != {0.0: 1074808, 1.0: 206}:
            raise ValueError(f"Comptes inattendus : train={counts}, validation={val_counts}")
        total = sum(counts.values())
        weights = {label: total / (2.0 * count) for label, count in counts.items()}
        weighted = train.withColumn("class_weight", F.when(F.col("label") == 1, weights[1.0]).otherwise(weights[0.0]))
        print("Comptes Train :", counts, flush=True)
        print("Poids calcules sur Train :", weights, flush=True)

        common = dict(labelCol="label", featuresCol="features", weightCol="class_weight")
        if args.model == "logistic_regression":
            classifier = LogisticRegression(maxIter=50, regParam=0.1, elasticNetParam=0.0, standardization=False, **common)
        elif args.model == "random_forest":
            classifier = RandomForestClassifier(numTrees=30, maxDepth=5, seed=42, **common)
        else:
            classifier = GBTClassifier(maxIter=20, maxDepth=3, seed=42, **common)
        pipeline = build_pipeline(classifier, scale=args.model == "logistic_regression")
        print("DEBUT ENTRAINEMENT :", args.model, flush=True)
        started = time.perf_counter()
        fitted = pipeline.fit(weighted)
        elapsed = time.perf_counter() - started
        fitted.write().save(destination + "/pipeline")
        print(f"Pipeline sauvegarde ; entrainement : {elapsed:.1f} secondes", flush=True)

        # Controle du modele sauvegarde, sans cible ni poids a l'inference.
        reloaded = PipelineModel.load(destination + "/pipeline")
        probe = validation.drop("label").limit(20)
        rows = reloaded.transform(probe).select("prediction", "probability").collect()
        if len(rows) != 20 or any(r["prediction"] is None for r in rows):
            raise RuntimeError("Echec du controle d'inference sans cible.")
        print("RECHARGEMENT ET INFERENCE SANS LABEL : OK", flush=True)

        predictions_path = destination + "/validation_predictions"
        (reloaded.transform(validation)
         .select("date", "serial_number", "model", "label", "prediction", "probability", "rawPrediction")
         .write.mode("errorifexists").parquet(predictions_path))
        saved = spark.read.parquet(predictions_path)
        prediction_counts = class_counts(saved)
        if prediction_counts != val_counts:
            raise RuntimeError("Comptes des predictions differents de Validation.")
        attributes = fitted.transform(validation.limit(1)).schema["features"].metadata.get("ml_attr", {})
        params = {p.name: value for p, value in classifier.extractParamMap().items()}
        metadata = {
            "model": args.model, "spark_version": spark.version,
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "data_base": args.data_base, "output": destination,
            "target": LABEL, "horizon_days": 7,
            "train_counts": counts, "validation_counts": val_counts,
            "class_weights": weights, "numeric_features": NUMERIC_FEATURES,
            "categorical_features": ["model"], "feature_vector_metadata": attributes,
            "classifier_params": params, "fit_seconds": elapsed,
            "imputation": "median fitted on train only; relativeError=0.001",
            "inference_without_label_checked": True,
            "test_dataset_used": False,
        }
        (spark.createDataFrame([(json.dumps(metadata, ensure_ascii=False, default=str),)], ["value"])
         .coalesce(1).write.mode("errorifexists").text(destination + "/metadata"))
        print("Predictions Validation :", sum(prediction_counts.values()), flush=True)
        print("Destination :", destination, flush=True)
        print("ENTRAINEMENT ET EXPORT VALIDATION REUSSIS", flush=True)
    finally:
        if train is not None:
            train.unpersist()
        if validation is not None:
            validation.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
