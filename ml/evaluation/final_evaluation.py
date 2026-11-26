"""Livrable 3 (Zohra) : gel du modele final puis evaluation.
--split validation : fixe le seuil, sauvegarde le pipeline final, le recharge.
--split test       : evaluation finale, une seule fois, sans rien modifier."""
import argparse

from pyspark import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:9000/cloudpredict"
DATA = HDFS + "/gold/livrable3/ml/v1"
TUNED = HDFS + "/ml/livrable3/zohra/tuned_logistic/pipeline"
FINAL = HDFS + "/models/failure_prediction/v1"
RESULTS = HDFS + "/ml/livrable3/zohra/final"
TARGET = "failure_next_7_days"
THRESHOLD = 0.99


def ratio(a, b):
    return a / b if b else 0.0


def exists(spark, path):
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(p)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--split", choices=["validation", "test"], required=True)
    split = ap.parse_args().split

    spark = SparkSession.builder.appName("l3-zohra-final-" + split).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    out_path = f"{RESULTS}/{split}"
    if split == "test" and exists(spark, out_path):
        raise SystemExit("Error: le Test a deja ete evalue, voir " + out_path)

    data = spark.read.parquet(f"{DATA}/{split}")
    unlabeled = data.drop(TARGET)

    if split == "validation":
        model = PipelineModel.load(TUNED)
        model.stages[-1].setThreshold(THRESHOLD)
        model.write().overwrite().save(FINAL + "/pipeline")
        print("Pipeline final enregistre dans", FINAL + "/pipeline")

    # Toujours evaluer le pipeline recharge, sans la colonne cible
    model = PipelineModel.load(FINAL + "/pipeline")
    clf = model.stages[-1]
    print(f"RECHARGEMENT OK : seuil={clf.getThreshold()} regParam={clf.getRegParam()} "
          f"elasticNet={clf.getElasticNetParam()}")
    preds = model.transform(unlabeled).select("date", "serial_number", "probability", "prediction")
    labels = data.select("date", "serial_number", F.col(TARGET).cast("double").alias("label"))
    df = preds.join(labels, ["date", "serial_number"]).persist(StorageLevel.MEMORY_AND_DISK)

    total, expected = df.count(), data.count()
    print(f"CONTROLE : {total} lignes evaluees sur {expected} "
          f"{'OK' if total == expected else 'ECART'}")
    c = {(int(r["label"]), int(r["prediction"])): r["count"]
         for r in df.groupBy("label", "prediction").count().collect()}
    tn, fp = c.get((0, 0), 0), c.get((0, 1), 0)
    fn, tp = c.get((1, 0), 0), c.get((1, 1), 0)
    precision, recall = ratio(tp, tp + fp), ratio(tp, tp + fn)
    ev = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability", numBins=0)

    failing = df.filter("label = 1").select("serial_number").distinct()
    alerted = df.filter("prediction = 1").select("serial_number").distinct()
    failing_n = failing.count()
    detected = alerted.join(failing, "serial_number", "inner").count()
    false_disks = alerted.join(failing, "serial_number", "left_anti").count()
    all_disks = df.select("serial_number").distinct().count()

    res = {
        "split": split, "seuil": THRESHOLD, "lignes": total,
        "accuracy": ratio(tp + tn, total), "precision": precision, "rappel": recall,
        "f1": ratio(2 * precision * recall, precision + recall),
        "f2": ratio(5 * precision * recall, 4 * precision + recall),
        "roc_auc": float(ev.evaluate(df, {ev.metricName: "areaUnderROC"})),
        "pr_auc": float(ev.evaluate(df, {ev.metricName: "areaUnderPR"})),
        "tn": tn, "fp": fp, "fn": fn, "tp": tp,
        "disques_total": all_disks, "disques_positifs": failing_n,
        "disques_detectes": detected, "disques_en_fausse_alerte": false_disks,
    }
    for k, v in res.items():
        print(f"RESULTAT {k} = {v}")

    features = []
    for stage in model.stages:
        if isinstance(stage, VectorAssembler):
            features += stage.getInputCols()
    res.update({"algorithme": "logistic_regression", "regParam": clf.getRegParam(),
                "elasticNetParam": clf.getElasticNetParam(), "version": "v1",
                "horizon_jours": 7, "variables": ", ".join(features)})
    mode = "overwrite" if split == "validation" else "errorifexists"
    spark.createDataFrame([res]).coalesce(1).write.mode(mode).json(out_path)
    print("Resultats enregistres dans", out_path)
    spark.stop()


if __name__ == "__main__":
    main()
