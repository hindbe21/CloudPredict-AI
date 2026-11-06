# Validation de l’intégration — Livrable 1

## Démarrage
Commande : docker compose up -d

## Résultats
- Spark : deux Workers actifs.
- HDFS : deux DataNodes actifs, réplication configurée à 2.
- Test Spark → HDFS : écriture et relecture Parquet réussies.
- Kafka : topic cloudpredict.metrics.raw avec trois partitions.
- Producer : 100 000 messages Backblaze envoyés, 0 erreur.
- Messages JSON et clés serial_number vérifiés dans Kafka UI.

## Preuves
Captures disponibles dans docs/preuves/.

## Environnement
- Windows avec Docker Desktop et WSL2.
- Date des tests : à compléter.