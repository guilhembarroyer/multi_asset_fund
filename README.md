# Système de Gestion de Fonds d'Investissement

Ce projet est un système de gestion de fonds d'investissement qui permet de gérer des clients, des portefeuilles et des managers, avec des fonctionnalités d'analyse de performance.

## Structure du Projet

```
multi_asset_fund/
├── code_src/
│   ├── main.py           # Point d'entrée du programme
│   ├── base_builder.py   # Classes de base et gestion de la base de données
│   ├── data_collector.py # Génération de données et création d'entités
│   ├── strategies.py     # Stratégies d'investissement
│   └── performances.py   # Analyse des performances
├── fund_database.db      # Base de données SQLite
└── README.md            # Documentation du projet
```

## Fonctionnalités

### 1. Gestion des Clients
- Création de clients (aléatoire ou manuel)
- Attribution automatique de managers
- Création de portefeuilles personnalisés

### 2. Gestion des Managers
- Attribution automatique basée sur le profil du client
- Gestion des stratégies d'investissement
- Suivi des performances

### 3. Analyse des Performances
- Analyse individuelle des portefeuilles
- Analyse globale du fonds
- Visualisations interactives
- Classements des portefeuilles et managers

## Installation

1. Clonez le repository :
```bash
git clone [URL_DU_REPO]
cd multi_asset_fund
```

2. Installez les dépendances :
```bash
pip install -r requirements.txt
```

## Utilisation

### Version Console
```bash
python code_src/main.py
```

## Structure de la Base de Données

### Tables Principales
- `Clients` : Informations sur les clients
- `Managers` : Informations sur les managers
- `Portfolios` : Portefeuilles des clients
- `Products` : Produits financiers disponibles
- `Deals` : Historique des transactions

### Relations
- Client -> Manager (1:1)
- Client -> Portfolio (1:1)
- Manager -> Portfolio (1:N)
- Portfolio -> Products (N:N)

## Fonctionnalités Techniques

### Gestion des Risques
- Stratégies d'investissement adaptées au profil de risque
- Suivi de la volatilité des portefeuilles
- Rééquilibrage automatique

### Analyse des Performances
- Calcul des rendements
- Analyse de la répartition des actifs
- Visualisations statistiques

## Contribution

Les contributions sont les bienvenues ! N'hésitez pas à :
1. Fork le projet
2. Créer une branche pour votre fonctionnalité
3. Commiter vos changements
4. Pousser vers la branche
5. Ouvrir une Pull Request

## Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.
