# Schéma JSON des événements — topic cloudpredict.metrics.raw

Un message = une ligne du CSV Backblaze d'un jour. Clé Kafka = `serial_number` (les messages d'un même disque vont dans la même partition).

## Champs

| Champ | Type JSON | Type Spark conseillé | Remarque |
|---|---|---|---|
| `date` | string (`2025-10-01`) | StringType puis DateType | |
| `serial_number` | string | StringType | jamais vide |
| `model` | string | StringType | |
| `capacity_bytes` | entier | LongType | |
| `failure` | entier (0 ou 1) | IntegerType | |
| `datacenter` | string | StringType | |
| `cluster_id`, `vault_id`, `pod_id`, `pod_slot_num` | nombre (`0.0`) | DoubleType | nullables |
| `is_legacy_format` | **string** (`"False"`) | StringType | arrive en texte, pas en booléen |
| `smart_<N>_normalized`, `smart_<N>_raw` | nombre ou `null` | DoubleType | **nullables** |

## Points d'attention

- Les attributs SMART valent `null` quand le modèle de disque ne les fournit pas. Plusieurs colonnes (par exemple `smart_13`, `smart_15`, `smart_160`, `smart_161`) sont 100 % nulles dans nos échantillons : le schéma Spark doit toutes les déclarer **nullables**.
- Les valeurs vides du CSV sont converties en `null`. `capacity_bytes` et `failure` sont des entiers, les autres champs numériques des flottants.
- Le nombre de colonnes SMART varie selon la version du fichier Backblaze : le schéma Spark doit être explicite et ne pas dépendre de l'inférence.

## Contrôle qualité (`producer/quality_check.py`)

Dataset `data_Q4_2025.zip` : 92 CSV, du 2025-10-01 au 2025-12-31, environ 130 Mo par jour.

| Échantillon | Lignes | Disques distincts | Jours | Pannes | Doublons date+série | Serial manquant |
|---|---|---|---|---|---|---|
| 100 000 premières lignes | 100 000 | 100 000 | 1 | 1 | 0 | 0 |
| 1 000 premières lignes par jour (`--per-file 1000`) | 92 000 | 1 046 | 92 | 0 | 0 | 0 |
| 5 000 premières lignes par jour (`--per-file 5000`) | 460 000 | 5 227 | 92 | 19 | 0 | 0 |
| 1 ligne sur 60 (`--every 60`) | 515 741 | 259 712 | 92 | 18 | 0 | 0 |

Les premières lignes de chaque fichier surreprésentent ST8000DM002 (environ 90 %). L'envoi final utilise donc `--every 60` : 77 modèles distincts (le modèle dominant ne dépasse pas 13 %), 18 pannes réparties sur plusieurs modèles.

Limite : un même disque apparaît peu de fois dans cet échantillon réparti. Il convient aux KPI globaux (total, disques distincts, pannes, taux, répartition par modèle, pannes par date) mais pas au suivi individuel d'un disque dans le temps. Les pannes sont rares (environ 0 à 1 par jour).

## Tests d'ingestion

Envois progressifs avec `--per-file 1000`, sur un topic neuf :

| Envoi | Messages | Erreurs | Durée |
|---|---|---|---|
| 100 | 100 | 0 | 0,1 s |
| 1 000 | 1 000 | 0 | 0,3 s |
| 10 000 | 10 000 | 0 | 2,8 s |

Offsets après les trois envois : 3 973 + 3 670 + 3 457 = 11 100 (aucune perte).

## Relancer le producteur pour la démonstration

```bash
docker compose up -d kafka kafka-init kafka-ui
cd kafka/producer
source .venv/Scripts/activate
python producer.py --file ../data/data_Q4_2025.zip --every 60 --limit 0
```

Vérification du nombre de messages :

```bash
MSYS_NO_PATHCONV=1 docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 --topic cloudpredict.metrics.raw
```

Pour repartir d'un topic vide (évite les doublons avant l'intégration) :

```bash
MSYS_NO_PATHCONV=1 docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --delete --topic cloudpredict.metrics.raw
docker compose up kafka-init
```
## Envoi pour le Machine Learning (livrable 3)

`--every 60` fragmente l'historique des disques. Pour le ML, utiliser la sélection par disque
(même hash que `prepare_history.py`) :

    python producer.py --file ../data/data_Q4_2025.zip --disk-percent 1 --start 2025-12-01 --end 2025-12-07 --limit 0

Essai sans Kafka (`--dry-run`) : 24 088 messages, 3 449 disques, 7 jours.
Envoi réel de contrôle : 100 messages, 0 erreur (offsets de 11 100 à 11 200).
Champs vérifiés : date, serial_number, model, capacity_bytes et SMART 5, 9, 194, 197, 198 (raw et normalized).
Le service `producer` de `docker-compose.yml` utilise encore `--every 60` : à adapter à l'intégration.
