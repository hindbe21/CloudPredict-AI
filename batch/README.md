# CloudPredict AI - Analyse batch HDFS / Spark SQL

## Objectif
Analyser les donnees historiques Backblaze stockees dans HDFS
au format Parquet, avec Apache Spark SQL.

## Scripts
- build_multiday_sample.py : extraction de trois journees du ZIP Backblaze.
- prepare_backblaze.py : conversion CSV vers Parquet dans HDFS.
- kpi_hdfs.py : calcul des KPI globaux, par modele et par jour.
- verify_kpi.py : validation des KPI du jeu d'essai multijour.

## KPI
- Nombre total d'observations
- Nombre de disques distincts
- Nombre d'observations avec failure = 1
- Pourcentage d'observations avec panne
- Repartition par modele
- Evolution quotidienne des observations avec panne

## Tests effectues
Dataset : Backblaze Q4 2025
Dates : 2025-10-01, 2025-11-01, 2025-12-01

Observations : 1 004 671
Observations avec panne : 26

Validation : reussie.

## Integration Spark Streaming
Le programme kpi_hdfs.py accepte :
- Les Parquet de test avec la colonne date.
- Les Parquet Bronze du streaming avec event_date.
- Le filtrage des lignes is_invalid lorsque cette colonne existe.

Chemin Bronze d'Aya :
hdfs://namenode:9000/cloudpredict/bronze/drives_raw

La validation sur les Parquet produits par Aya reste a effectuer
sur un environnement commun.

## Remarques
Les taux calcules sont des proportions d'observations avec panne,
et non des probabilites de panne par disque.
Les donnees brutes restent exclues du depot Git.
