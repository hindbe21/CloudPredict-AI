# Livrable 2 — Archivage Kafka vers HDFS avec Spark

Pour cette partie du projet, j’ai repris le cluster Docker du premier livrable et développé le traitement qui relie Kafka à HDFS. L’objectif était de récupérer les événements Backblaze publiés par le Producer et de les enregistrer en Parquet pour les analyses historiques.

Le travail se trouve sur `feature/l2-spark-streaming`. Les tests présentés ici ont été réalisés le 15 novembre 2026 sous Windows, avec PowerShell et Docker Desktop/WSL2.

## Configuration

| Élément | Valeur |
| --- | --- |
| Spark | 3.5.7, Scala 2.12 |
| Cluster | Un Master et deux Workers, chacun avec un cœur et 1 Go annoncés |
| Connecteur | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7` |
| Broker depuis Spark | `kafka:29092` |
| Topic | `cloudpredict.metrics.raw`, trois partitions |
| HDFS | `hdfs://namenode:9000`, deux DataNodes |

J’ai ajouté le montage `./streaming:/opt/project/streaming:ro` au Master pour lui donner accès aux scripts.

## Fichiers de cette partie

- `kafka_to_hdfs.py` : lecture du topic, extraction du JSON et écriture en Parquet avec checkpoint.
- `backblaze_schema.json` : schéma des 197 champs présents dans les cinq premiers messages examinés.
- `verify_bronze.py` : relecture des Parquet, comptage et contrôle des doublons d’offsets.

## Données et stockage

J’ai préparé le schéma à partir de cinq événements réels du fichier `2025-10-01.csv`. `date`, `serial_number`, `model`, `datacenter` et `is_legacy_format` sont des chaînes. Dans ces messages, `is_legacy_format` contient par exemple la chaîne `"False"`.

`capacity_bytes` et `failure` sont des entiers longs. Les attributs SMART et les autres champs numériques sont déclarés en double et peuvent être nuls. Les types des champs toujours nuls dans cet échantillon restent à confirmer sur d’autres modèles.

Les colonnes Backblaze restent directement accessibles pour les analyses SQL. Le programme conserve aussi le JSON original dans `raw_json`, la clé Kafka et les métadonnées `kafka_topic`, `kafka_partition`, `kafka_offset` et `kafka_timestamp`. Il ajoute `event_date`, `ingested_at`, `_corrupt_record` et `is_invalid`.

Une ligne est signalée si le parsing échoue, si la date ou le numéro de série manque, ou si `failure` n’est pas égal à 0 ou 1. Les lignes signalées restent dans Bronze. Ces contrôles ne vérifient pas toutes les règles possibles sur les valeurs SMART.

| Usage | Chemin |
| --- | --- |
| Parquet Bronze | `hdfs://namenode:9000/cloudpredict/bronze/drives_raw` |
| Checkpoint | `hdfs://namenode:9000/cloudpredict/checkpoints/kafka_to_hdfs` |

## Lancement

Les commandes sont exécutées dans PowerShell, à la racine du dépôt.

```powershell
docker compose up -d
docker compose ps -a
```

L’interface Spark est accessible sur <http://localhost:8080>. Les deux Workers doivent être actifs.

### Traiter les messages disponibles puis terminer

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.executor.cores=1 --conf spark.jars.ivy=/tmp/ivy --executor-memory 512m --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /opt/project/streaming/kafka_to_hdfs.py --available-now
```

Le premier lancement télécharge le connecteur et ses dépendances. Avec un checkpoint neuf, le programme lit les premiers offsets encore disponibles (`earliest`). Avec un checkpoint existant, il reprend la progression enregistrée.

### Test en continu

J’ai lancé la même commande sans `--available-now`. Le job reste alors actif et déclenche des micro-lots toutes les dix secondes, avec une limite de 1 000 offsets par micro-lot.

Dans un deuxième terminal, j’ai envoyé 1 000 événements :

```powershell
.\kafka\producer\.venv\Scripts\python.exe kafka/producer/producer.py --file data/data_Q4_2025/2025-10-01.csv --limit 1000 --delay 0.02
```

Le Producer a terminé avec `1000 messages, 0 erreurs`, en 20,7 secondes.

### Contrôle des données archivées

Après l’arrêt du Streaming et la libération des ressources du cluster :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.executor.cores=1 --executor-memory 512m --total-executor-cores 2 /opt/project/streaming/verify_bronze.py
```

## Résultats obtenus

Le premier contrôle a confirmé l’archivage de cinq messages sans ligne invalide ni doublon d’offset. Après les envois suivants, dont les 1 000 événements du test en continu, le contrôle final a donné :

```text
Nombre de lignes : 1110
Lignes invalides : 0
Doublons topic/partition/offset : 0
```

| Partition | Lignes | Offset minimum | Offset maximum |
| --- | ---: | ---: | ---: |
| 0 | 399 | 33497 | 33895 |
| 1 | 374 | 33456 | 33829 |
| 2 | 337 | 33047 | 33383 |
| Total | 1110 | — | — |

Les clés Kafka affichées correspondent aux numéros de série. Les données relues comprennent notamment les modèles `CT250MX500SSD1` et `ST8000DM002`.

L’absence de doublons concerne le triplet topic/partition/offset. Un nouvel envoi du même CSV crée de nouveaux offsets : les observations répétées restent donc présentes dans Bronze.

## Vérifications HDFS

```powershell
docker compose exec namenode hdfs dfs -ls -R /cloudpredict/bronze/drives_raw
docker compose exec namenode hdfs dfs -ls -R /cloudpredict/checkpoints/kafka_to_hdfs
docker compose exec namenode hdfs fsck /cloudpredict/bronze/drives_raw -files -blocks -locations
```

Les sorties montrent les fichiers `.snappy.parquet`, le répertoire `_spark_metadata`, ainsi que les répertoires `commits`, `offsets` et `sources` du checkpoint.

| Indicateur HDFS sur le répertoire Bronze | Résultat |
| --- | --- |
| État | `HEALTHY` |
| DataNodes | 2 |
| Fichiers contrôlés, métadonnées comprises | 22 |
| Blocs contrôlés | 22 |
| Réplication moyenne | 2,0 |
| Blocs sous-répliqués | 0 |
| Blocs manquants ou corrompus | 0 |

## Problème rencontré

En lançant le contrôle batch pendant le Streaming, j’ai obtenu `Initial job has not accepted any resources`. Le Streaming occupait les deux cœurs disponibles et le contrôle attendait des ressources.

J’ai arrêté les applications concernées, vérifié que les cœurs étaient libres dans l’interface Spark, puis relancé uniquement `verify_bronze.py`. Le contrôle a alors terminé avec les 1 110 lignes attendues.

## Test de reprise à finaliser

Le checkpoint est bien présent dans HDFS. Il reste à conserver le résultat du test suivant pour terminer la validation de la reprise :

1. Ne publier aucun nouveau message.
2. Relancer le job avec `--available-now` et les mêmes chemins.
3. Attendre sa fin, puis relancer `verify_bronze.py`.
4. Vérifier que le total reste à 1 110 lignes, sans doublon d’offset.

Les Parquet et le checkpoint doivent être conservés entre les lancements. Un même checkpoint ne doit pas être partagé par deux jobs simultanés, ni réutilisé avec un autre topic ou une autre destination.

## Preuves et intégration

Les captures sont regroupées dans `../docs/preuves/livrable2/` : contrôle des 1 110 lignes, fichiers Parquet, checkpoint et réplication HDFS.


