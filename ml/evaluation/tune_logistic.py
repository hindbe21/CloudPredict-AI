"""Livrable 3 (Zohra) : tuning de la regression logistique et choix du seuil.
Utilise Train et Validation uniquement. Le Test n'est jamais lu ici."""
from pyspark import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:9000/cloudpredict"
DATA = HDFS + "/gold/livrable3/ml/v1"
AYA = HDFS + "/ml/livrable3/aya/v1/logistic_regression/pipeline"
OUT = HDFS + "/ml/livrable3/zohra/tuned_logistic"
TARGET = "failure_next_7_days"
GRID = [(r, e) for r in (0.01, 0.1, 1.0) for e in (0.0, 0.5)]


def ratio(a, b):
    return a / b if b else 0.0


def main():
    spark = SparkSession.builder.appName("l3-zohra-tuning").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    full = PipelineModel.load(AYA)
    old = full.stages[-1]
    assert isinstance(old, LogisticRegressionModel), type(old)
    # Pretraitement d'Aya, deja ajuste sur Train uniquement
    prep = PipelineModel(stages=full.stages[:-1])
    label, weight, feats = old.getLabelCol(), old.getWeightCol(), old.getFeaturesCol()

    train = spark.read.parquet(DATA + "/train").withColumn(label, F.col(TARGET).cast("double"))
    n = train.count()
    pos = train.filter(F.col(label) == 1).count()
    w1, w0 = n / (2 * pos), n / (2 * (n - pos))
    print(f"POIDS : positif={w1:.6f} negatif={w0:.6f}")
    train = train.withColumn(weight, F.when(F.col(label) == 1, w1).otherwise(w0))
    train = prep.transform(train).select(feats, label, weight).persist(StorageLevel.MEMORY_AND_DISK)

    val = spark.read.parquet(DATA + "/validation").withColumn(label, F.col(TARGET).cast("double"))
    val = (prep.transform(val).select("serial_number", feats, label)
           .persist(StorageLevel.MEMORY_AND_DISK))
    pr = BinaryClassificationEvaluator(labelCol=label, metricName="areaUnderPR", numBins=0)
    roc = BinaryClassificationEvaluator(labelCol=label, metricName="areaUnderROC", numBins=0)

    best = None
    for reg, enet in GRID:
        lr = LogisticRegression(featuresCol=feats, labelCol=label, weightCol=weight,
                                maxIter=old.getMaxIter(), regParam=reg, elasticNetParam=enet,
                                standardization=old.getStandardization())
        model = lr.fit(train)
        out = model.transform(val)
        p, r = pr.evaluate(out), roc.evaluate(out)
        print(f"GRILLE regParam={reg} elasticNet={enet} | pr_auc={p:.6f} roc_auc={r:.6f}")
        if best is None or p > best[0]:
            best = (p, r, reg, enet, model)
    p, r, reg, enet, model = best
    print(f"MEILLEUR regParam={reg} elasticNet={enet} pr_auc={p:.6f} roc_auc={r:.6f}")

    # Choix du seuil sur Validation
    second = F.udf(lambda v: float(v[1]), "double")
    scored = (model.transform(val)
              .select("serial_number", label, second(model.getProbabilityCol()).alias("s"))
              .persist(StorageLevel.MEMORY_AND_DISK))
    positives = scored.filter(F.col(label) == 1).count()
    disks = scored.filter(F.col(label) == 1).select("serial_number").distinct().count()
    quantiles = scored.approxQuantile("s", [0.9, 0.99, 0.999, 0.9999], 0.00001)
    thresholds = sorted(set([0.5, 0.7, 0.9, 0.95, 0.99] + quantiles))
    chosen = None
    for t in thresholds:
        hit = scored.filter(F.col("s") >= t)
        alerts = hit.count()
        tp_df = hit.filter(F.col(label) == 1)
        tp = tp_df.count()
        found = tp_df.select("serial_number").distinct().count()
        prec, rec = ratio(tp, alerts), ratio(tp, positives)
        f1 = ratio(2 * prec * rec, prec + rec)
        f2 = ratio(5 * prec * rec, 4 * prec + rec)
        print(f"SEUIL {t:.6f} | alertes={alerts} tp={tp} fp={alerts - tp} fn={positives - tp} "
              f"precision={prec:.6f} rappel={rec:.6f} f1={f1:.6f} f2={f2:.6f} "
              f"disques={found}/{disks}")
        if chosen is None or f2 > chosen[0]:
            chosen = (f2, t)
    print(f"SEUIL PROPOSE (meilleur F2) : {chosen[1]:.6f}")

    PipelineModel(stages=prep.stages + [model]).write().overwrite().save(OUT + "/pipeline")
    spark.createDataFrame(
        [("logistic_regression", reg, enet, float(chosen[1]), float(p), float(r))],
        ["algorithme", "regParam", "elasticNetParam", "seuil", "pr_auc_val", "roc_auc_val"]
    ).coalesce(1).write.mode("overwrite").json(OUT + "/choix")
    print("Pipeline optimise enregistre dans", OUT)
    spark.stop()


if __name__ == "__main__":
    main()
