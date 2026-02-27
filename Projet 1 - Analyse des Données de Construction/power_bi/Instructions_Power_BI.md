# Intégration Power BI - Projet Construction

## Connexion aux données

1. **Option A** : Importer le fichier Excel généré  
   `outputs/Construction_Projects_Analytics.xlsx` (après exécution du script Python)

2. **Option B** : Importer le CSV directement  
   `Construction_Projects.csv` + appliquer les transformations Power Query

## Mesures DAX recommandées

```
Cout_Total = SUM(Construction_Projects[Total_Cost])

Cout_Moyen = AVERAGE(Construction_Projects[Total_Cost])

Nb_Projets = COUNTROWS(Construction_Projects)

Duree_Moyenne = AVERAGE(Construction_Projects[Duration])

Coût par Jour = DIVIDE([Cout_Total], SUM(Construction_Projects[Duration]), 0)
```

## Visuals suggérés

- **Graphique en barres** : Cout total par Année
- **Nuage de points** : Durée vs Coût Total
- **Histogramme** : Distribution des coûts
- **Tableau** : Top 10 projets par coût
