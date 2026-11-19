# Livrable 2 — Intégration du pipeline CloudPredict AI

## Objectif

Nous avons réuni les trois contributions du livrable 2 sur la branche `integration/livrable2` pour tester le pipeline sur un seul PC :

```text
Backblaze CSV → Producer Python → Kafka → Spark Structured Streaming
                                               ↓
                                         HDFS Parquet
                                               ↓
                                         Spark SQL / KPI
```

L’intégration multi-jours a été réalisée le 19 novembre 2026. Elle reprend le cluster Docker du premier livrable : un Spark Master, deux Workers, un NameNode, deux DataNodes et les services Kafka.

## Contributions réunies

| Partie | Branche | Travail intégré |
| --- | --- | --- |
| Spark Streaming | `feature/l2-spark-streaming` | Lecture Kafka, archivage Parquet, checkpoint et contrôle Bronze |
| Kafka | `feature/l2-kafka-tests` | Producer, échantillonnage et documentation du JSON |
| HDFS et batch | `feature/l2-hdfs-batch` | Préparation des données et calcul des KPI Spark SQL |

Les fusions ont été réalisées sur la branche d’intégration. Le dossier `batch/` a été monté dans le Master pour exécuter les scripts sur les données du Streaming.

## Configuration commune

| Élément | Valeur |
| --- | --- |
| Spark | 3.5.7 |
| Adresse du Master | `spark://spark-master:7077` |
| Kafka depuis les conteneurs | `kafka:29092` |
| Topic | `cloudpredict.metrics.raw`, trois partitions |
| HDFS | `hdfs://namenode:9000` |
| Format d’archivage | Parquet |
| Répertoire Bronze de la campagne | `/cloudpredict/bronze/livrable2_run01` |
| Checkpoint de la campagne | `/cloudpredict/checkpoints/livrable2_run01` |
| Résultats SQL | `/cloudpredict/results/livrable2_run01` |

Les données brutes utilisées sont dans `data/data_Q4_2025` sur le PC d’intégration. Elles ne sont pas ajoutées au dépôt Git.

## Premier test avec les données déjà archivées

Avant le test multi-jours, nous avons lancé `batch/kpi_hdfs.py` sur le répertoire `/cloudpredict/bronze/drives_raw`, qui contenait les événements des premiers essais du Streaming.

| Indicateur | Résultat |
| --- | ---: |
| Observations lues | 1 110 |
| Observations valides | 1 110 |
| Observations exclues | 0 |
| Disques distincts | 1 000 |
| Observations avec panne | 0 |

Les données concernaient le 1er octobre 2025. Ce test a confirmé que le script SQL de Hind pouvait lire directement les colonnes produites par le Streaming. Les résultats ont été enregistrés dans `/cloudpredict/results/livrable2_smoke01`.

La différence entre observations et disques distincts vient des réenvois effectués pendant les premiers tests. Les événements ont des offsets Kafka différents, même lorsque l’observation CSV est répétée.

## Envoi de l’échantillon multi-jours

Le Producer parcourt les CSV du dossier Backblaze avec `--every 60 --limit 0`. Il conserve une ligne sur 60 dans chaque fichier, sans limite globale de messages.

Pendant cette étape, Windows a bloqué la bibliothèque native de `confluent-kafka` dans l’environnement Python local. Un service Docker `producer` a donc été ajouté au Compose, avec un montage du dossier `data/` en lecture seule et une connexion interne à `kafka:29092`.

```powershell
docker compose build producer
docker compose run --rm producer
```

Ces commandes déclenchent un nouvel envoi : elles ne doivent pas être relancées simplement pour consulter les résultats.

La dernière ligne du Producer n’a pas été conservée. Nous ne pouvons donc pas attribuer un nombre exact de messages à ce seul lancement. La campagne peut comprendre des événements des premiers essais ; le rapprochement ci-dessous porte sur l’ensemble des offsets disponibles dans le topic.

## Archivage dans HDFS

Le job a été exécuté avec des chemins distincts de ceux des premiers tests :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.executor.cores=1 --conf spark.jars.ivy=/tmp/ivy --executor-memory 512m --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /opt/project/streaming/kafka_to_hdfs.py --output hdfs://namenode:9000/cloudpredict/bronze/livrable2_run01 --checkpoint hdfs://namenode:9000/cloudpredict/checkpoints/livrable2_run01 --available-now
```

Le mode `--available-now` traite les messages disponibles au démarrage, puis termine. Le programme conserve le JSON brut, les colonnes Backblaze et les métadonnées Kafka. Le checkpoint enregistre sa progression.

Après la fin de l’archivage, le contrôle a été lancé séparément pour disposer des ressources du cluster :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.executor.cores=1 --executor-memory 512m --total-executor-cores 2 /opt/project/streaming/verify_bronze.py --path hdfs://namenode:9000/cloudpredict/bronze/livrable2_run01
```

Résultat obtenu :

```text
Nombre de lignes : 516851
Lignes invalides : 0
Doublons topic/partition/offset : 0
```

Le contrôle des lignes invalides porte sur les règles définies dans le script, notamment le parsing, la date, le numéro de série et `failure`. Il ne constitue pas une validation complète de chaque valeur SMART.

## Rapprochement Kafka et HDFS

Nous avons relevé les offsets de début et de fin du topic :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic cloudpredict.metrics.raw --time -2
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic cloudpredict.metrics.raw --time -1
```

| Partition | Offset de début | Offset de fin exclusif | Différence | Lignes HDFS |
| --- | ---: | ---: | ---: | ---: |
| 0 | 33 497 | 206 686 | 173 189 | 173 189 |
| 1 | 33 456 | 205 214 | 171 758 | 171 758 |
| 2 | 33 047 | 204 951 | 171 904 | 171 904 |
| Total | 100 000 | 616 851 | 516 851 | 516 851 |

Les bornes des offsets relus dans HDFS correspondent aux plages Kafka : le maximum archivé est l’offset de fin moins un. Les comptes sont identiques pour chaque partition et aucun doublon d’offset n’a été détecté. Ce rapprochement confirme la couverture de la plage disponible au moment du contrôle.

Il ne permet pas d’isoler les messages du dernier lancement du Producer ni de conclure à l’absence de répétitions métier. Un même enregistrement CSV envoyé deux fois reçoit deux offsets différents.

## Analyses Spark SQL

Le script de Hind a été lancé sur les Parquet de la campagne :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.executor.cores=1 --executor-memory 512m --total-executor-cores 2 /opt/project/batch/kpi_hdfs.py --input hdfs://namenode:9000/cloudpredict/bronze/livrable2_run01 --output hdfs://namenode:9000/cloudpredict/results/livrable2_run01
```

| Indicateur | Résultat |
| --- | ---: |
| Observations lues | 516 851 |
| Observations valides | 516 851 |
| Observations exclues | 0 |
| Disques distincts | 260 650 |
| Observations avec panne | 18 |
| Pourcentage d’observations avec panne | 0,0035 % |

Le pourcentage représente une proportion d’observations signalant une panne, pas une probabilité de panne par disque.

Le programme a aussi produit les répartitions par modèle et par date, puis affiché `SUCCES : KPI CALCULES ET ENREGISTRES`. Les résultats sont écrits dans les sous-répertoires `global`, `par_modele` et `par_jour` de la destination SQL.

La sortie quotidienne affichée dans le terminal est limitée aux vingt premières lignes. Elle ne suffit pas, seule, à vérifier les 92 dates ou les 77 modèles annoncés lors des essais Kafka. Ces totaux restent à contrôler sur les résultats enregistrés.

## Santé HDFS de la campagne

Le contrôle `hdfs fsck` du répertoire `/cloudpredict/bronze/livrable2_run01`, terminé le 19 novembre 2026 à 23:17:36 UTC, indique `HEALTHY`.

| Indicateur | Résultat |
| --- | ---: |
| DataNodes | 2 |
| Fichiers contrôlés, métadonnées comprises | 2 072 |
| Blocs validés | 2 072 |
| Taille logique totale | 393 816 447 octets |
| Réplication moyenne | 2,0 |
| Blocs sous-répliqués | 0 |
| Blocs manquants ou corrompus | 0 |

## Reprise avec le même checkpoint

Nous avons relancé le job avec `--available-now`, sans nouvel envoi et sans supprimer le checkpoint. Le traitement a terminé sans erreur de requête. Le rapport de progression du micro-lot 518 indique `numInputRows: 0` et un retard maximal sur Kafka de zéro.

Pour les trois partitions, les offsets de début, de fin et les derniers offsets disponibles sont identiques :

| Partition | Offset retrouvé à la reprise |
| --- | ---: |
| 0 | 206 686 |
| 1 | 205 214 |
| 2 | 204 951 |

Le contrôle batch exécuté ensuite confirme toujours 516 851 lignes, zéro ligne invalide et zéro doublon topic/partition/offset. Le redémarrage n’a donc pas ajouté une seconde fois les événements déjà archivés.

Une erreur de transport `StacklessClosedChannelException` est apparue après le message de fin, lors de l’envoi d’un JAR à un executor. Sa position dans les logs suggère une fermeture de connexion pendant l’arrêt ; cette cause n’a pas été confirmée. Le contrôle des Parquet a néanmoins terminé avec les comptes attendus. La valeur `numOutputRows: -1` du rapport FileSink est une métrique non renseignée, pas un nombre négatif de lignes écrites.

## Preuves

- [Premier test SQL sur les 1 110 lignes](preuves/livrable2/integration_kpi_1110.png)
- [Résultats SQL multi-jours](preuves/livrable2/integration_kpi_multiday.png)
- [Offsets Kafka de la campagne](preuves/livrable2/kafka_offsets_integration.png)

Les preuves de santé HDFS et de checkpoint issues des premiers tests sont conservées séparément dans `docs/preuves/livrable2/`. Elles ne remplacent pas un contrôle du nouveau répertoire `livrable2_run01`.



Les tests réalisés démontrent la lecture Kafka, l’archivage Parquet dans HDFS et l’analyse SQL sur les mêmes données. Le nombre du dernier envoi Producer reste non documenté ; les volumes validés ici correspondent au contenu Kafka disponible et effectivement archivé.
