# Livrable 3 — Évaluation, tuning et modèle final (Zohra)

## Protocole
- Données de Hind, découpage temporel inchangé : Train (01–31/10/2025), Validation (08–23/11/2025), Test (01–24/12/2025).
- Modèles candidats et prétraitement d'Aya, ajustés sur Train uniquement.
- Modèle, hyperparamètres et seuil choisis sur Validation. Test évalué une seule fois, après gel des choix.
- Les métriques concernent la classe positive (panne dans les 7 jours).

## Comparaison sur Validation (seuil par défaut)
1 075 014 observations, 206 positives, 39 disques positifs.

| Modèle | Accuracy | Précision | Rappel | F1 | ROC-AUC | PR-AUC | TN | FP | FN | TP | Disques détectés |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Baseline (toujours 0) | 0,9998 | 0 | 0 | 0 | 0,500 | 0,0002 | 1 074 808 | 0 | 206 | 0 | 0/39 |
| Logistic Regression | 0,9294 | 0,0017 | 0,626 | 0,0034 | 0,769 | 0,0250 | 999 001 | 75 807 | 77 | 129 | 25/39 |
| Random Forest | 0,9701 | 0,0030 | 0,471 | 0,0060 | 0,789 | 0,0040 | 1 042 820 | 31 988 | 109 | 97 | 20/39 |
| GBT | 0,9382 | 0,0017 | 0,544 | 0,0034 | 0,761 | 0,0031 | 1 008 424 | 66 384 | 94 | 112 | 23/39 |

La régression logistique est retenue pour sa PR-AUC, mesure adaptée au fort déséquilibre.

## Tuning de la régression logistique (Validation)
| regParam | elasticNetParam | PR-AUC | ROC-AUC |
|---|---|---|---|
| 0,01 | 0,0 | 0,0242 | 0,778 |
| **0,01** | **0,5** | **0,0385** | 0,761 |
| 0,1 | 0,0 | 0,0250 | 0,769 |
| 0,1 | 0,5 | 0,0264 | 0,781 |
| 1,0 | 0,0 | 0,0214 | 0,765 |
| 1,0 | 0,5 | 0,0061 | 0,747 |

## Choix du seuil
Seuil **0,99** : meilleur F2 parmi les seuils testés, validé par l'équipe avant l'ouverture du Test.
Le seuil 0,50 détectait 24/39 disques mais avec 85 592 fausses alertes (observations).

## Résultats du modèle final (seuil 0,99)
| Mesure | Validation | Test |
|---|---|---|
| Observations | 1 075 014 | 1 618 193 |
| Positives | 206 | 133 |
| Accuracy | 0,9953 | 0,9950 |
| Précision | 1,02 % | 0,62 % |
| Rappel | 24,3 % | 37,6 % |
| F1 | 0,0195 | 0,0123 |
| F2 | 0,0435 | 0,0293 |
| ROC-AUC | 0,761 | 0,811 |
| PR-AUC | 0,0385 | 0,0222 |
| TN | 1 069 940 | 1 610 095 |
| FP (observations) | 4 868 | 7 965 |
| FN | 156 | 83 |
| TP | 50 | 50 |
| Disques en panne détectés | 11/39 | 10/27 |
| Disques distincts en fausse alerte | 328 | 363 |

Baseline sur Test (calculée à partir des effectifs) : accuracy 0,9999, rappel 0, aucune panne détectée.

## Analyse
- **Faux négatifs :** sur Test, 17 des 27 disques en panne ne reçoivent aucune alerte.
- **Faux positifs :** environ 330 observations en fausse alerte par jour sur Test ; une alerte correcte pour environ 160.
- La PR-AUC baisse de Validation à Test, mais reste très au-dessus du taux de positifs (environ 0,00008).
- La PR-AUC des deux périodes ne se compare pas directement, car leur proportion de positifs diffère.

## Limites
- Précision très faible : le modèle est un résultat expérimental, pas un système prêt pour la production.
- Peu de disques en panne (39 en Validation, 27 en Test) : les estimations sont incertaines.
- Hyperparamètres et seuil choisis sur la même Validation.
- Grille de tuning réduite, limitée à la régression logistique.
- Le Test a servi aux contrôles de qualité des données, et l'analyse des valeurs manquantes ainsi que le choix des périodes ont porté sur l'historique global. Aucune performance prédictive n'y avait été mesurée avant l'évaluation finale.
- Échantillon de 20 % des disques ; un seul trimestre de données.
- La PR-AUC est celle calculée par Spark (`BinaryClassificationEvaluator`).
- Les scores issus d'un entraînement pondéré ne sont pas des probabilités de panne calibrées.

## Modèle sauvegardé
- Chemin : `/cloudpredict/models/failure_prediction/v1/pipeline` (pipeline complet avec prétraitement).
- Algorithme : régression logistique, regParam=0,01, elasticNetParam=0,5, seuil 0,99, horizon 7 jours, version v1.
- Rechargement vérifié, inférence sans la colonne cible.
- Métadonnées, variables explicatives et métriques : `/cloudpredict/ml/livrable3/zohra/final/`.

## Scripts (`ml/evaluation/`)
1. `evaluate_candidates.py` : rechargement des candidats et comparaison sur Validation.
2. `tune_logistic.py` : tuning et étude des seuils.
3. `final_evaluation.py --split validation` puis `--split test`.
