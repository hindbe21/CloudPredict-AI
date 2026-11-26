"""Livrable 3 (Zohra) : rechargement des pipelines d'Aya et comparaison sur Validation.
Le dataset Test n'est jamais lu ici."""
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:9000/cloudpredict"
DATA = HDFS + "/gold/livrable3/ml/v1/validation"
AYA = HDFS + "/ml/livrable3/aya/v1"
OUT = HDFS + "/ml/livrable3/zohra/validation_comparison"
MODELS = ["logistic_regression", "random_forest", "gbt"]
TARGET = "failure_next_7_days"


def ratio(a, b):
    return a / b if b else 0.0


def metrics(name, df, positive_disks):
    """df : serial_number, label, prediction, score (score de la classe 1)."""
    counts = {(int(r["label"]), int(r["prediction"])): r["count"]
              for r in df.groupBy("label", "prediction").count().collect()}
    tn, fp = counts.get((0, 0), 0), counts.get((0, 1), 0)
    fn, tp = counts.get((1, 0), 0), counts.get((1, 1), 0)
    precision, recall = ratio(tp, tp + fp), ratio(tp, tp + fn)
    ev = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="score", numBins=0)
    detected = (df.filter("label = 1 AND prediction = 1")
                .select("serial_number").distinct().count())
    return (name, ratio(tp + tn, tp + tn + fp + fn), precision, recall,
            ratio(2 * precision * recall, precision + recall),
            float(ev.evaluate(df, {ev.metricName: "areaUnderROC"})),
            float(ev.evaluate(df, {ev.metricName: "areaUnderPR"})),
            tn, fp, fn, tp, f"{detected}/{positive_disks}")


def main():
    spark = SparkSession.builder.appName("l3-zohra-validation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val = spark.read.parquet(DATA).cache()
    total = val.count()
    positives = val.filter(F.col(TARGET) == 1).count()
    disks = val.filter(F.col(TARGET) == 1).select("serial_number").distinct().count()
    print(f"VALIDATION : {total} lignes, {positives} positives, {disks} disques positifs")

    rows = []
    for name in MODELS:
        # 1) Rechargement du pipeline et inférence sans la cible
        model = PipelineModel.load(f"{AYA}/{name}/pipeline")
        n = model.transform(val.drop(TARGET).limit(20)).select("prediction").count()
        print(f"RECHARGEMENT {name} : OK ({n} predictions sans la cible)")

        # 2) Métriques à partir des prédictions d'Aya
        preds = spark.read.parquet(f"{AYA}/{name}/validation_predictions")
        df = preds.select(
            "serial_number",
            F.col("label").cast("double").alias("label"),
            F.col("prediction").cast("double").alias("prediction"),
            F.col("probability").alias("score"),
        ).cache()
        c, p = df.count(), df.filter("label = 1").count()
        print(f"CONTROLE {name} : {c} lignes, {p} positives, "
              f"{'OK' if (c, p) == (total, positives) else 'ECART'}")
        rows.append(metrics(name, df, disks))
        if name == MODELS[0]:
            base = df.select("serial_number", "label",
                             F.lit(0.0).alias("prediction"), F.lit(0.0).alias("score"))
            rows.insert(0, metrics("baseline_toujours_0", base, disks))
        df.unpersist()

    cols = ["modele", "accuracy", "precision", "rappel", "f1", "roc_auc", "pr_auc",
            "tn", "fp", "fn", "tp", "disques_detectes"]
    result = spark.createDataFrame(rows, cols)
    print("=== COMPARAISON SUR VALIDATION (seuil par defaut) ===")
    for r in rows:
        print(" | ".join(f"{c}={v:.6f}" if isinstance(v, float) else f"{c}={v}"
                         for c, v in zip(cols, r)))
    result.coalesce(1).write.mode("overwrite").option("header", True).csv(OUT)
    print("Resultats enregistres dans", OUT)
    spark.stop()


if __name__ == "__main__":
    main()
