# CloudPredict AI — Jeu de données Backblaze (membre 2)

## Source

Backblaze Drive Stats : https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data

Backblaze publie des archives ZIP, contenant des fichiers CSV de relevés quotidiens des disques ; une ligne correspond à un disque à une date donnée.

## Périmètre retenu

- Année / trimestre : **À renseigner après téléchargement**
- Nom de l'archive ZIP : **À renseigner**
- Taille de l'archive : **À renseigner**
- Nombre de fichiers CSV : **À renseigner**
- Nombre de lignes : **À mesurer / renseigner**

## Colonnes utiles

| Colonne | Signification |
|---|---|
| `date` | Date du relevé |
| `serial_number` | Identifiant du disque |
| `model` | Modèle du disque |
| `capacity_bytes` | Capacité en octets |
| `failure` | Panne observée le jour du relevé (`1`/`0`) |
| `smart_5_raw` | Secteurs réalloués (si disponibles) |
| `smart_9_raw` | Heures de fonctionnement (si disponibles) |
| `smart_194_raw` | Température (si disponible et selon le modèle) |
| `smart_197_raw` | Secteurs instables (si disponibles) |

Certaines colonnes SMART sont manquantes ou changent d'une période à une autre. Elles ne doivent pas être inventées.

## Réutilisation

Backblaze demande de citer la source, de rester responsable de l'usage et de ne pas revendre les données brutes (voir la page officielle pour les conditions exactes).

## Lien avec Kafka

Conserver l'archive ZIP **localement**, hors GitHub ; elle sera déposée dans le dossier `data/raw/` du projet et le producteur Python la lira progressivement. Ne pas envoyer d'archives volumineuses dans le dépôt.

`failure` indique une panne constatée au jour du relevé et **ne correspond pas** directement à la future cible « panne dans les 7 prochains jours » ; la cible ML sera préparée ultérieurement.
## Dataset Backblaze — Q4 2025

### 1. Provenance
Source : Backblaze Hard Drive Stats
Site : https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data

### 2. Période et volume
- Période : 1er octobre au 31 décembre 2025
- Archive : data_Q4_2025.zip
- Nombre de fichiers CSV : 92
- Nombre de colonnes par fichier : 197
- Format : CSV regroupés dans une archive ZIP
- Un enregistrement représente un relevé quotidien d'un disque.

### 3. Variables principales
- date : date du relevé
- serial_number : identifiant du disque
- model : modèle du disque
- capacity_bytes : capacité du disque
- failure : panne observée le jour du relevé
- smart_* : indicateurs S.M.A.R.T. du disque

### 4. Contrôles effectués
- Lecture du ZIP sans extraction complète : OK
- Nombre de CSV : 92
- Nombre de colonnes : 197
- Comparaison des 92 en-têtes : identiques
- Schémas différents détectés : 0

### 5. Utilisation dans CloudPredict AI
Les données seront rejouées dans Apache Kafka.
Spark archivera ensuite les événements dans HDFS.
Le modèle ML estimera le risque de panne dans les 7 jours.

La cible à 7 jours sera construite ultérieurement à partir
de l'historique, et non directement avec la colonne failure.

### 6. Conditions d'utilisation
Backblaze doit être cité comme source.
Le dataset est disponible gratuitement.
La vente du dataset brut n'est pas autorisée.

### 7. Stockage GitHub
Le fichier ZIP est conservé localement dans data/raw/.
Les données volumineuses sont exclues du dépôt GitHub.
