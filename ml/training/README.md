# Livrable 3 — Entraînement des candidats ML (Aya)

## Données vérifiées

Les dossiers Parquet transmis par Hind ont été chargés dans HDFS sous
`/cloudpredict/gold/livrable3/ml/v1`. Le contrôle `check_inputs.py` a confirmé :

| Ensemble | Lignes | Positives | Période |
| --- | ---: | ---: | --- |
| Train | 2 066 288 | 432 | 01/10/2025 au 31/10/2025 |
| Validation | 1 075 014 | 206 | 08/11/2025 au 23/11/2025 |

Les dix colonnes SMART ont des valeurs manquantes, mais aucune n’est
entièrement vide. La capacité est renseignée sur toutes les lignes vérifiées.
Le dataset Test n’est pas utilisé par les scripts d’entraînement.

L’image commune au Master et aux deux Workers utilise Spark 3.5.7,
NumPy 1.24.4 et SciPy 1.10.1. Les imports ont été vérifiés dans les trois
conteneurs. Le montage du dossier `ml/` doit être présent sur le Master.

## Prétraitement

`ml/preprocessing.py` réutilise le SQLTransformer de Hind pour calculer
les dix variables dérivées. Les 20 variables numériques retenues sont les
dix SMART brutes/normalisées, les cinq variables dérivées et les cinq
indicateurs de valeurs manquantes. La capacité est représentée en Go.

Les valeurs sont converties en double puis imputées avec la médiane
(approximation Spark par défaut, erreur relative 0,001). Le modèle de disque
est encodé avec StringIndexer et OneHotEncoder ; les catégories inconnues
sont conservées. La régression logistique utilise une standardisation sans
centrage, pour préserver la représentation creuse.

Toutes les étapes apprises sont ajustées sur Train uniquement. La date,
le numéro de série, la cible et le poids ne sont pas des features.
Les poids équilibrés sont calculés par `N / (2 * N_classe)` sur Train.
Validation conserve sa distribution originale.

## Lancer un candidat

Arrêter les autres applications Spark pour libérer les deux cœurs.
Depuis PowerShell à la racine du dépôt :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.executor.cores=1 --executor-memory 512m --total-executor-cores 2 /opt/project/ml/training/train_model.py --model logistic_regression
```

Après validation du premier entraînement, remplacer la valeur de `--model`
par `random_forest`, puis `gbt`, pour les lancer un par un. Les trois
candidats utilisent les mêmes splits et poids.

| Candidat | Paramètres de départ |
| --- | --- |
| Logistic Regression | maxIter=50, regParam=0.1, elasticNetParam=0 |
| Random Forest | numTrees=30, maxDepth=5, seed=42 |
| GBT | maxIter=20, maxDepth=3, seed=42 |

Ce sont des paramètres initiaux, sans tuning. Les labels sont copiés en
`label` pour les classificateurs. Le code ne réalise aucun randomSplit et
n’équilibre ni ne sous-échantillonne Validation.

## Sorties pour Zohra

Chaque candidat est enregistré sous :

```text
/cloudpredict/ml/livrable3/aya/v1/<nom_du_modele>/
├── pipeline/
├── validation_predictions/
└── metadata/
```

Les prédictions contiennent `date`, `serial_number`, `model`, `label`,
`prediction`, `probability` et `rawPrediction`. Leur nombre doit être
1 075 014, avec 206 labels positifs. Les métadonnées enregistrent les poids,
les paramètres, le temps d’ajustement, les variables et la description du
vecteur. Aucun score n’est présenté comme une probabilité calibrée de panne.

Après sauvegarde, le pipeline est rechargé et testé sur vingt observations
sans label ni colonne de poids. La transformation de Validation utilise ce
pipeline rechargé. Tous les stages sauvegardés sont des composants Spark
standards, y compris les SQLTransformer.

Une destination existante provoque un arrêt pour éviter d’écraser un modèle.
Pour une nouvelle tentative, utiliser par exemple `--output-base
hdfs://namenode:9000/cloudpredict/ml/livrable3/aya/v2`. Un échec peut laisser
des sorties partielles : elles ne constituent pas un candidat validé.

Les dossiers complets des pipelines et prédictions seront à exporter hors
Git pour Zohra. Ses métriques, le tuning et le test final sont distincts de
cet entraînement initial.

## État de validation

Les trois candidats ont été entraînés le 26 novembre 2026 sur le cluster
Standalone. Chaque pipeline a été sauvegardé puis rechargé ; le contrôle
d’inférence sans label a réussi pour chacun.

| Candidat | Temps d’ajustement du pipeline | Prédictions Validation |
| --- | ---: | ---: |
| Logistic Regression | 50,3 s | 1 075 014 |
| Random Forest | 65,0 s | 1 075 014 |
| GBT | 317,7 s | 1 075 014 |

Ces temps comprennent l’ajustement des étapes de prétraitement et du
classificateur ; ils ne comprennent pas l’export et le rechargement.
Le Train contient 2 065 856 négatives et 432 positives. Les poids utilisés
sont respectivement 0,5001045571424145 et 2391,537037037037.

Ces résultats valident l’exécution et les exports. Ils ne permettent pas
encore de désigner le meilleur modèle : les métriques sur Validation,
le tuning et l’évaluation finale sur Test restent à réaliser par Zohra.

Captures à conserver dans `docs/preuves/livrable3/aya/` : contrôle des
entrées, dépendances, application distribuée et fin de chaque entraînement.

Références : [LogisticRegression Spark 3.5.7](https://spark.apache.org/docs/3.5.7/api/python/reference/api/pyspark.ml.classification.LogisticRegression.html),
[Imputer](https://spark.apache.org/docs/3.5.7/api/python/reference/api/pyspark.ml.feature.Imputer.html).
