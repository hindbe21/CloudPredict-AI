# CloudPredict AI — Infrastructure Docker et cluster Spark

## Objectif et périmètre

La contribution `feature/spark-docker` fournit un cluster Apache Spark
Standalone conteneurisé et un test de calcul PySpark. La configuration HDFS,
le Producer Kafka et leur intégration sont documentés séparément.

## Architecture

| Service | Rôle | Ressources de calcul annoncées |
| --- | --- | --- |
| `spark-master` | Coordination et allocation des ressources | — |
| `spark-worker-1` | Hébergement des executors | 1 cœur, 1 Go |
| `spark-worker-2` | Hébergement des executors | 1 cœur, 1 Go |

Les trois services utilisent `apache/spark:3.5.7` et le réseau Docker
`cloudpredict-net`. Compose peut préfixer le nom du réseau par celui du projet.

- Adresse interne du Master : `spark://spark-master:7077`.
- Interface web depuis le PC : <http://localhost:8080>.
- Ports publiés sur le PC : `7077` et `8080`.
- Code monté en lecture seule : `./infra/spark:/opt/project/spark:ro`.

Les ressources annoncées à Spark ne sont pas des limites strictes de mémoire
ou de CPU des conteneurs Docker. `depends_on` définit l'ordre de démarrage,
sans garantir que le Master est déjà prêt.

## Prérequis

- Docker Desktop démarré en mode conteneurs Linux avec WSL2.
- Docker Compose disponible.
- Commandes exécutées dans PowerShell à la racine du dépôt.

PySpark s'exécute dans Docker : son installation sur Windows n'est pas requise
pour lancer ce test. Un avertissement Pylance local ne bloque pas l'exécution.

## Démarrage du cluster Spark uniquement

```powershell
docker compose config --quiet
docker compose up -d spark-master spark-worker-1 spark-worker-2
docker compose ps -a
```

Ouvrir <http://localhost:8080> et vérifier les deux Workers `ALIVE`,
deux cœurs au total et 2 GiB de mémoire de calcul annoncée.
L'état `Up` d'un conteneur ne suffit pas à confirmer son enregistrement Spark.

## Vérification réseau

```powershell
docker compose exec spark-worker-1 python3 -c "import socket; print(socket.gethostbyname('spark-master')); socket.create_connection(('spark-master',7077),timeout=5).close(); print('Connexion OK')"
docker compose exec spark-worker-2 python3 -c "import socket; print(socket.gethostbyname('spark-master')); socket.create_connection(('spark-master',7077),timeout=5).close(); print('Connexion OK')"
```

Chaque commande doit afficher une adresse IP interne et `Connexion OK`.

## Test de calcul PySpark

Le programme `infra/spark/test_spark.py` crée 100 000 nombres en huit
partitions, compte les lignes et vérifie leur somme.

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.executor.cores=1 --executor-memory 512m --total-executor-cores 2 /opt/project/spark/test_spark.py
```

Résultat attendu :

```text
Nombre de lignes : 100000
Somme calculee : 4999950000
TEST SPARK REUSSI
```

Le driver tourne dans le Master en mode client. Les executors le joignent
via `spark-master`. Pour confirmer l'utilisation effective des deux Workers,
examiner les executors dans l'interface ou dans les logs de l'application.

## Résultats et preuves

L'interface Master partagée pendant la validation affichait deux Workers
actifs, deux cœurs et 2 GiB disponibles. Les captures associées au calcul et
au réseau sont conservées dans `docs/preuves/` :

- [Workers Spark](preuves/spark_workers.png)
- [Résultat du test PySpark](preuves/spark_test.png)
- [Réseau du premier Worker](preuves/spark_network1.png)
- [Réseau du deuxième Worker](preuves/spark_network2.png)

Les tests croisés avec HDFS et Kafka relèvent de `integration/livrable1`.

## Diagnostic et arrêt

```powershell
docker compose logs --tail 100 spark-master spark-worker-1 spark-worker-2
docker compose stop spark-master spark-worker-1 spark-worker-2
```

Si le port `8080` est occupé, choisir un autre port côté PC dans le Compose,
par exemple `8082:8080`, puis ouvrir `http://localhost:8082`.
