\# CloudPredict AI — Livrable 3 : Préparation des données ML



\*\*Responsable : Hind Berma\*\*



\## 1. Objectif



Cette contribution consiste à préparer un historique de données Backblaze exploitable pour la prédiction des pannes de disques à un horizon de sept jours. Les traitements distribués sont réalisés avec PySpark sur le cluster Spark du projet, composé d'un Master et de deux Workers. Les données sont conservées dans HDFS avec un facteur de réplication de 2.



\## 2. Audit des données du livrable 2



L'historique initial contenait 1 004 671 observations, 338 907 disques distincts et 26 pannes. Cependant, il ne comportait que trois journées non consécutives et aucune variable SMART.



Ces limitations ont justifié la constitution d'un nouvel historique quotidien adapté à la maintenance prédictive.



\## 3. Constitution de l'historique



Le dataset Backblaze Q4 2025 couvre la période du 1er octobre au 31 décembre 2025.



Un échantillonnage déterministe de 20 % des disques, basé sur le hachage de leur identifiant, permet de conserver leurs observations quotidiennes disponibles tout en limitant le volume des traitements.



| Indicateur | Résultat |

|---|---:|

| Fichiers journaliers | 92 |

| Observations originales | 30 941 708 |

| Observations sélectionnées | 6 190 487 |

| Pannes originales | 945 |

| Pannes sélectionnées | 175 |

| Colonnes conservées | 19 |

| Taille des CSV préparés | 601,74 Mo |



Les fichiers préparés ont été chargés directement dans un répertoire de staging HDFS pour permettre le développement individuel. L'intégration avec le flux Kafka relève de la validation collective.



\## 4. Nettoyage distribué



Les opérations réalisées comprennent :



\- Conversion et contrôle des types.

\- Vérification des champs obligatoires.

\- Nettoyage des identifiants et des modèles.

\- Contrôle des doublons exacts et des conflits métier.

\- Analyse des valeurs SMART manquantes.

\- Sauvegarde au format Parquet.



Le nettoyage a permis de conserver les 6 190 487 observations, représentant 68 334 disques et 69 modèles. Aucun doublon ni enregistrement invalide n'a été détecté par les contrôles réalisés.



Les valeurs manquantes sont conservées afin que l'imputation soit ajustée exclusivement sur les données d'entraînement.



\## 5. Construction de la variable cible



La variable `failure\_next\_7\_days` indique si une panne est observée dans les sept jours suivant un relevé.



La construction utilise des fenêtres temporelles PySpark par identifiant de disque.



Les observations correspondant à des disques déjà en panne, aux sept dernières journées du trimestre ou à des historiques futurs incomplets sans panne connue sont exclues.



| Indicateur | Résultat |

|---|---:|

| Observations labellisées | 5 698 922 |

| Labels positifs | 957 |

| Labels négatifs | 5 697 965 |

| Disques positifs distincts | 156 |

| Observations exclues | 491 565 |



\## 6. Feature Engineering



Les attributs SMART 5, 9, 194, 197 et 198 ont été retenus pour la première version du pipeline ML.



Les attributs SMART 187 et 188 sont conservés dans la couche Silver, mais ne sont pas utilisés dans la première version en raison de leur proportion élevée de valeurs manquantes.



Dix variables supplémentaires ont été construites :



\- capacity\_gb

\- disk\_age\_days

\- log\_reallocated\_sectors

\- log\_pending\_sectors

\- log\_uncorrectable\_sectors

\- smart\_5\_missing

\- smart\_9\_missing

\- smart\_194\_missing

\- smart\_197\_missing

\- smart\_198\_missing



Ces transformations sont implémentées dans un SQLTransformer réutilisable pendant l'entraînement et l'inférence.



Le dataset Gold contient 25 colonnes et 5 698 922 observations. Les mesures SMART brutes et normalisées doivent être interprétées avec prudence, car leur signification peut varier selon le modèle de disque.



\## 7. Découpage temporel



Un découpage chronologique est utilisé afin de respecter la nature prédictive du projet. Des intervalles de sept jours séparent les ensembles.



| Ensemble | Période | Observations | Positifs | Disques positifs |

|---|---|---:|---:|---:|

| Train | 01/10 – 31/10 | 2 066 288 | 432 | 85 |

| Validation | 08/11 – 23/11 | 1 075 014 | 206 | 39 |

| Test | 01/12 – 24/12 | 1 618 193 | 133 | 27 |

| Total | Q4 2025 | 4 759 495 | 771 | 151 |



Aucun disque positif commun aux ensembles n'a été détecté.



Les trois datasets contiennent les 15 colonnes sources nécessaires à l'entraînement, dont les identifiants et la cible. Les variables dérivées seront recalculées dans le pipeline ML.



\## 8. Organisation du stockage HDFS



| Couche | Chemin |

|---|---|

| Staging | /cloudpredict/staging/livrable3/q4\_2025\_csv |

| Silver Clean | /cloudpredict/silver/livrable3/drives\_clean |

| Silver Labeled | /cloudpredict/silver/livrable3/drives\_labeled |

| Gold Features | /cloudpredict/gold/livrable3/drives\_features |

| Train | /cloudpredict/gold/livrable3/ml/v1/train |

| Validation | /cloudpredict/gold/livrable3/ml/v1/validation |

| Test | /cloudpredict/gold/livrable3/ml/v1/test |



\## 9. Scripts développés



Les scripts sont disponibles dans `ml/data\_preparation/` et doivent être exécutés dans l'ordre suivant :



| Script | Fonction |

|---|---|

| audit\_data.py | Audit de l'historique initial |

| prepare\_history.py | Préparation des CSV et échantillonnage des disques |

| clean\_data.py | Nettoyage distribué |

| build\_labels.py | Construction de la cible à sept jours |

| feature\_engineering.py | Construction des variables explicatives |

| split\_dataset.py | Découpage temporel |

| validate\_dataset.py | Validation finale des datasets |



\## 10. Environnement technique



\- Apache Spark 3.5.7

\- PySpark et Spark SQL

\- Spark MLlib

\- NumPy 1.24.4

\- Hadoop HDFS 3.2.1

\- Docker Compose

\- Parquet avec compression Snappy



Les traitements Spark utilisent le Master `spark://spark-master:7077` et les deux Workers.



Le paramètre `spark.hadoop.dfs.replication=2` est utilisé pour les écritures HDFS finales.



\## 11. Validation finale



Les trois datasets ont été relus avec Spark. Leur schéma, leurs volumes, leurs périodes temporelles et leurs labels ont été contrôlés.



Le système HDFS présente un état HEALTHY, sans bloc corrompu, bloc manquant ni réplique manquante pour les datasets ML.



Les fichiers Parquet ont également été exportés afin de permettre leur utilisation sur les environnements des autres membres du groupe.



\## 12. Limites et recommandations



Le dataset présente un déséquilibre important des classes. Il est recommandé d'utiliser une pondération calculée sur Train et de privilégier le Recall, la Precision, le F1-score et la PR-AUC.



Le nombre limité de disques positifs dans le Test impose une interprétation prudente des performances.



Le numéro de série et la date ne doivent pas être intégrés directement au vecteur de caractéristiques. Les transformations apprises doivent être ajustées uniquement sur Train.



La reproductibilité de l'environnement ML nécessite l'intégration de NumPy dans l'image Docker Spark définitive.



L'intégration avec Kafka et la validation du pipeline collectif restent à réaliser.



\## 13. Conclusion



Cette contribution fournit un historique nettoyé, labellisé, enrichi et découpé temporellement, prêt pour l'entraînement et l'évaluation des modèles Spark ML. Elle constitue la base de la phase de maintenance prédictive du projet CloudPredict AI.

