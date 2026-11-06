# CloudPredict AI — HDFS (membre 2)

## Objectif

Mettre en place un cluster HDFS avec un NameNode, deux DataNodes, deux volumes distincts et une réplication de **2**.

## Composants

- `namenode` : métadonnées HDFS et emplacements des blocs ; UI sur http://localhost:9870
- `datanode1` : stockage des blocs, volume `hdfs_datanode1`
- `datanode2` : stockage des blocs, volume `hdfs_datanode2`
- `hadoop.env` : `fs.defaultFS=hdfs://namenode:9000`, `dfs.replication=2`

## Démarrer uniquement HDFS (PowerShell, depuis la racine du dépôt)

```powershell
docker compose -f infra/hadoop/compose.hdfs.yml config
docker compose -f infra/hadoop/compose.hdfs.yml up -d
docker compose -f infra/hadoop/compose.hdfs.yml ps
```

Ouvrir http://localhost:9870 puis chercher les **2 DataNodes en vie**.

## Vérifier la réplication (PowerShell)

```powershell
docker compose -f infra/hadoop/compose.hdfs.yml exec namenode hdfs dfsadmin -report
docker compose -f infra/hadoop/compose.hdfs.yml exec namenode hdfs getconf -confKey dfs.replication
docker compose -f infra/hadoop/compose.hdfs.yml exec namenode sh -c 'echo cloudpredict-hdfs > /tmp/hdfs-test.txt'
docker compose -f infra/hadoop/compose.hdfs.yml exec namenode hdfs dfs -mkdir -p /cloudpredict/test
docker compose -f infra/hadoop/compose.hdfs.yml exec namenode hdfs dfs -put -f /tmp/hdfs-test.txt /cloudpredict/test/hdfs-test.txt
docker compose -f infra/hadoop/compose.hdfs.yml exec namenode hdfs fsck /cloudpredict/test/hdfs-test.txt -files -blocks -locations
```

Résultat attendu : 2 DataNodes disponibles ; `dfs.replication` vaut `2` ; le fichier de test possède **2 répliques réelles**, dans des DataNodes distincts. Conserver la sortie de `fsck` et une capture de l'interface NameNode dans `docs/preuves/`.

## Diagnostic si problème

```powershell
docker compose -f infra/hadoop/compose.hdfs.yml ps
docker compose -f infra/hadoop/compose.hdfs.yml logs --tail 80 namenode
docker compose -f infra/hadoop/compose.hdfs.yml logs --tail 80 datanode1
docker compose -f infra/hadoop/compose.hdfs.yml logs --tail 80 datanode2
```

Si `localhost:9870` est déjà occupé, vérifier qu'un autre cluster HDFS n'a pas été lancé. Ne pas démarrer les deux en même temps sur le même port.

Pour arrêter **sans supprimer les données** :

```powershell
docker compose -f infra/hadoop/compose.hdfs.yml down
```

**NE PAS utiliser `down -v`**, car cela supprime les volumes nommés.

## Intégration avec les autres binômes

Le fichier `compose.hdfs.yml` est une configuration de développement **isolée** ; il n'est pas le Compose final. Lors de l'intégration, réunir les services Hadoop avec Spark et Kafka dans le `docker-compose.yml` commun et sur le même réseau. Ne pas modifier simultanément le Compose commun avec les autres membres. Ne pas lancer simultanément les deux configurations HDFS sur le même port.

Les conteneurs de ce projet pédagogique utilisent une image Hadoop ancienne ; elle ne constitue pas une configuration de production.
