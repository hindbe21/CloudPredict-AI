# CloudPredict AI — Infrastructure Docker et cluster Apache Spark

## 1. Objectif

Mettre en place un cluster Spark Standalone distribué et conteneurisé avec Docker Compose afin d'exécuter les futurs traitements Big Data du projet CloudPredict AI.

## 2. Architecture

Le cluster de calcul comprend :

- 1 Spark Master : coordination des applications et allocation des ressources.
- 2 Spark Workers : exécution des tâches distribuées.
- 1 réseau Docker commun : communication entre les services.

Adresse interne du Master : `spark://spark-master:7077`.

Interface de supervision : `http://localhost:8080`.

## 3. Configuration

Les services Spark sont définis dans le fichier `docker-compose.yml` du projet commun.

Chaque Worker est initialement configuré avec un cœur CPU et 1 Go de mémoire de calcul.

## 4. Démarrage

```powershell
docker compose config
docker compose up -d spark-master spark-worker-1 spark-worker-2
docker compose ps
```

## 5. Tests de validation

Les vérifications à effectuer sont :

1. Les trois conteneurs Spark sont démarrés.
2. Les deux Workers apparaissent comme actifs dans l'interface Master.
3. Les conteneurs communiquent sur le réseau Docker.
4. Une application PySpark est soumise au Master et se termine correctement.
5. Les executors utilisent les ressources du cluster.

## 6. Résultats

À compléter après exécution :

- Nombre de Workers actifs :
- Ressources CPU disponibles :
- Mémoire disponible :
- Résultat du calcul PySpark :
- Date et environnement du test :
- Captures associées :

## 7. Conclusion

Cette infrastructure constitue la couche de calcul distribuée de CloudPredict AI. Elle sera utilisée pour les traitements Spark Structured Streaming, Spark SQL et Spark ML des prochaines étapes du projet.