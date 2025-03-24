# Multi-Asset Fund Management System

## Description
Ce projet est un système de gestion de fonds multi-actifs qui permet de simuler et d'analyser différentes stratégies d'investissement. Le système utilise une base de données SQLite pour stocker les données des clients, des portefeuilles et des transactions.

## Fonctionnalités
- Gestion des clients et des portefeuilles
- Trois stratégies d'investissement :
  - Low Risk : Volatilité cible de 10%
  - Medium Risk : Optimisation du ratio de Sharpe avec faible turnover
  - High Risk : Optimisation du ratio de Sharpe avec concentration
- Simulation de performance historique
- Analyse des rendements et de la volatilité
- Gestion des transactions et des positions

## Structure du Projet
```
multi_asset_fund/
├── code_src/
│   ├── main.py           # Point d'entrée du programme
│   ├── base_builder.py   # Classes de base et gestion de la base de données
│   ├── strategies.py     # Implémentation des stratégies d'investissement
│   └── portfolio_builder.py  # Gestion des portefeuilles
├── data/
│   └── database.db       # Base de données SQLite
└── requirements.txt      # Dépendances du projet
```

## Installation
1. Cloner le repository :
```bash
git clone [URL_DU_REPO]
cd multi_asset_fund
```

2. Installer les dépendances :
```bash
pip install -r requirements.txt
```

## Utilisation
1. Lancer le programme :
```bash
python code_src/main.py
```

2. Suivre les instructions du menu interactif pour :
   - Enregistrer un nouveau client
   - Analyser la performance d'un client
   - Analyser la performance d'un manager
   - Quitter le programme

## Stratégies d'Investissement

### Low Risk
- Volatilité cible : 10% annualisée
- Ajustement dynamique des positions pour maintenir la volatilité cible
- Privilégie les actifs moins risqués

### Medium Risk
- Optimisation du ratio de Sharpe
- Limite de 2 transactions par mois
- Gestion prudente du turnover

### High Risk
- Optimisation du ratio de Sharpe avec concentration
- Poids maximum de 20% par actif
- Stratégie plus agressive

## Base de Données
Le système utilise une base de données SQLite avec les tables suivantes :
- Clients : Informations sur les clients
- Managers : Informations sur les gestionnaires
- Portfolios : Détails des portefeuilles
- Products : Informations sur les actifs
- Portfolios_Products : Positions dans les portefeuilles
- Deals : Historique des transactions

## Développement
Pour contribuer au projet :
1. Créer une branche pour votre fonctionnalité
2. Implémenter les modifications
3. Tester le code
4. Soumettre une pull request

## Tests
Pour exécuter les tests :
```bash
python -m pytest tests/
```

## Licence
[Votre Licence]
