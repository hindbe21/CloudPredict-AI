"""Etapes Spark ML communes, sans apprentissage sur Validation ou Test."""
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Imputer, OneHotEncoder, SQLTransformer, StandardScaler,
    StringIndexer, VectorAssembler,
)

from data_preparation.feature_engineering import (
    DERIVED_FEATURES, MISSING_FEATURES, SMART_COLUMNS, feature_transformer,
)


# La capacite est representee en Go ; ni date, ni identifiant, ni cible.
NUMERIC_FEATURES = SMART_COLUMNS + DERIVED_FEATURES + MISSING_FEATURES


def build_pipeline(classifier, scale=False):
    doubles = [f"{c}__double" for c in NUMERIC_FEATURES]
    imputed = [f"{c}__imputed" for c in NUMERIC_FEATURES]
    casts = ", ".join(
        f"CAST(`{c}` AS DOUBLE) AS `{out}`"
        for c, out in zip(NUMERIC_FEATURES, doubles)
    )
    stages = [
        feature_transformer(),
        SQLTransformer(statement=f"SELECT *, {casts} FROM __THIS__"),
        Imputer(strategy="median", inputCols=doubles, outputCols=imputed),
        StringIndexer(inputCol="model", outputCol="model_index", handleInvalid="keep"),
        OneHotEncoder(inputCols=["model_index"], outputCols=["model_encoded"], handleInvalid="keep"),
        VectorAssembler(inputCols=imputed + ["model_encoded"], outputCol="assembled_features", handleInvalid="error"),
    ]
    if scale:
        stages.append(StandardScaler(inputCol="assembled_features", outputCol="features", withMean=False, withStd=True))
    else:
        stages.append(SQLTransformer(statement="SELECT *, assembled_features AS features FROM __THIS__"))
    return Pipeline(stages=stages + [classifier])
