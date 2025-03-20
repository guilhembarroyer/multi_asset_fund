from faker import Faker
fake = Faker()

from datetime import datetime, timedelta
import random
import yfinance as yf
import pandas as pd
import sqlite3
import time

import pycountry
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="my_fund_manager")

from base_builder import (
    BaseModel, Client, AssetManager, Portfolio, Product,
    get_eligible_managers, get_next_id
)

from yahooquery import Screener

from typing import Dict, List, Optional, Tuple

import pandas_datareader as pdr

from alpha_vantage.timeseries import TimeSeries



###RANDOM INFOS GENERATOR

def generate_email(name):
    """Génère un email basé sur le nom du client."""
    name_parts = name.lower().replace(" ", ".")  # Remplace les espaces par des points
    domains = ["gmail.com", "yahoo.com", "outlook.com"]
    return f"{name_parts}@{random.choice(domains)}"

def get_random_country():
    """Génère un pays valide et une ville réelle appartenant à ce pays."""
    while True:
        country_name = fake.country()
        country = pycountry.countries.get(name=country_name)
        
        if country:  # Vérifie que le pays est valide
           return country_name

###RANDOM CLIENT GENERATOR

def generate_valid_registration_date():
    """Génère une date d'enregistrement entre le 01-01-2022 et le 31-12-2022 au format YYYY-MM-DD."""
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 12, 31)
    
    # Générer une date aléatoire entre start_date et end_date
    random_timestamp = random.randint(int(start_date.timestamp()), int(end_date.timestamp()))
    random_date = datetime.fromtimestamp(random_timestamp)
    
    # Retourner la date au format YYYY-MM-DD
    return random_date.strftime("%Y-%m-%d")



def generate_random_client(database):
    """Génère un client aléatoire avec des données valides."""
    name = fake.name()
    return {
        "name": name,
        "age": fake.random_int(min=18, max=100),
        "country": get_random_country(),
        "email": generate_email(name),
        "risk_profile": fake.random_element(elements=("Low Risk", "Medium Risk", "High Risk")),
        "registration_date": generate_valid_registration_date(),
        "investment_amount": fake.random_int(min=1000, max=1000000),
        "portfolio_id": get_next_id("Portfolios", database)
    }




###PRECISE CLIENT GENERATOR

def get_client_name():
    """Demande le prénom et le nom du client en imposant un format correct."""
    while True:
        name = input("📌 Entrez le prénom et le nom du client (ex: Jean Dupont) : ").strip()
        parts = name.split()

        if len(parts) < 2:
            print("❌ Vous devez entrer un prénom suivi d'un nom !")
            continue
        
        # Mise en forme : première lettre en majuscule, le reste en minuscule
        formatted_name = " ".join(part.capitalize() for part in parts)
        return formatted_name

def get_age():
    """Demande et valide l'âge (doit être ≥ 18)."""
    while True:
        age = int(input("📌 Entrez l'âge du client (interdit aux mineurs): "))
        if age < 18:
            print("❌ L'âge doit être d'au moins 18 ans.")
        else:
            return age
        
def get_valid_country():
    """Demande un pays valide à l'utilisateur."""
    while True:
        country = input("📌 Entrez le pays : ").strip()
        country_obj = pycountry.countries.get(name=country)

        if country_obj:
            return country
        else:
            print("❌ Pays invalide. Veuillez entrer un pays existant.")
        
def get_risk_profile():
    """Demande à l'utilisateur de choisir un profil de risque valide."""
    valid_profiles = {"Low Risk", "Medium Risk", "High Risk"}
    
    while True:
        risk_profile = input("📌 Entrez le profil de risque ('Low Risk', 'Medium Risk', 'High Risk') : ").strip()
        if risk_profile in valid_profiles:
            return risk_profile
        print("❌ Choix invalide ! Veuillez entrer exactement 'Low Risk', 'Medium Risk' ou 'High Risk'.")
        

def get_email(name):
    """Demande un email valide contenant le nom du client."""
    while (email := input("📌 Entrez l'email : ")): #.lower().replace(" ", ".") not in name.lower().replace(" ", "."):
        print(f"❌ L'email doit contenir le nom du client ({name}).")
    return email


def get_investment_amount():
    """Demande à l'utilisateur d'entrer un montant valide entre 1000 et 1000000."""
    while True:
        try:
            amount = int(input("📌 Entrez le montant d'investissement (entre 1 000 et 1 000 000) : "))
            if 1000 <= amount <= 1000000:
                return amount
            else:
                print("❌ Montant invalide ! Il doit être entre 1 000 et 1 000 000.")
        except ValueError:
            print("❌ Veuillez entrer un nombre valide.")


def get_registration_date():
    """Demande et valide une date d'enregistrement (avant le 01-01-2023)."""
    while True:
        date_str = input("📌 Entrez la date d'enregistrement, doit être antérieure au 01-01-2023 (format YYYY-MM-DD) : ")
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            if date >= datetime(2023, 1, 1):
                print("❌ La date doit être avant le 2023-01-01.")
            else:
                return date_str  # Retourne la date sous forme de chaîne
        except ValueError:
            print("❌ Format invalide ! Utilisez YYYY-MM-DD.")


def generate_precise_client(database):
    """Demande à l'utilisateur d'entrer les informations d'un client avec validation."""
    # Collecte des informations de base
    name = get_client_name()
    age = get_age()
    country = get_valid_country()
    email = get_email(name)
    risk_profile = get_risk_profile()
    registration_date = get_registration_date()
    investment_amount= get_investment_amount()


    return {
        "name": name,
        "age": age,
        "country": country,
        "email": email,
        "risk_profile": risk_profile,
        "registration_date": registration_date,
        "investment_amount": investment_amount,
        "portfolio_id": get_next_id("Portfolios", database)
    }




###MANAGER CREATION & AFFILIATION

def get_client_seniority(client_investment):
    # Définition des niveaux de séniorité en fonction du montant investi
    seniority_levels = {
        "Junior": (1000, 100000),
        "Mid-level": (100000, 500000),
        "Senior": (500000, 1000000)
    }

    # Trouver le niveau de séniorité correspondant
    client_seniority = None
    for level, (min_amount, max_amount) in seniority_levels.items():
        if min_amount <= client_investment < max_amount:
            client_seniority = level
            return client_seniority
    

def manager_affiliation(client, database):
    """
    Associe un manager à un client en fonction du pays, du montant investi et des stratégies compatibles.

    Arguments :
    - client : dictionnaire contenant les infos du client (incluant 'country', 'investment_amount' et 'strategies')
    - database : liste de dictionnaires représentant les managers disponibles

    Retourne :
    - Un dictionnaire représentant le manager assigné, ou None si aucun n'est trouvé.
    """
    
    client_country = client["country"]
    client_seniority = get_client_seniority(client["investment_amount"])
    client_strategie = client["risk_profile"]
    

    
    eligible_managers=get_eligible_managers(database, client_country, client_seniority, client_strategie)

    # Sélectionner un manager au hasard parmi les compatibles
    if eligible_managers:
        return random.choice(eligible_managers)
    
    return None  # Aucun manager trouvé



def create_manager(client, database):
    """Assigner un manager compatible ou proposer la création d'un nouveau manager."""
    client_country = client["country"]
    client_risk_profile = client["risk_profile"]
    client_seniority = get_client_seniority(client["investment_amount"])
    name = fake.name()

    return {
        "name": name,
        "age": fake.random_int(min=25, max=60),
        "country": client_country,
        "email": generate_email(name),
        "seniority": client_seniority,
        "investment_sector": fake.random_element(['ms_basic_materials','ms_communication_services','ms_consumer_cyclical','ms_consumer_defensive','ms_energy','ms_financial_services','ms_healthcare','ms_industrials','ms_real_estate','ms_technology','ms_utilities']),
        "strategies": [client_risk_profile] + [random.choice([profile for profile in ["Low Risk", "Medium Risk", "High Risk"] if profile != client_risk_profile])]
    }


### PORTFOLIO CREATION/CONFIGURATION

def get_corresponding_assets(sector):
    # Initialiser le screener
    s = Screener()

    # Récupérer les actions les plus échangées
    query_results = s.get_screeners(sector, 2)

    tickers=[stock["symbol"] for stock in query_results[sector]["quotes"]]

    return tickers


def create_portfolio(manager, client_data, database):
    """Configure le portefeuille du manager en fonction de la stratégie."""
    
    tickers = get_corresponding_assets(manager["investment_sector"])
    
    # Vérifier et télécharger les données des actifs
    missing_tickers = check_and_download_assets(tickers, database)
    
    size = len(tickers) - len(missing_tickers)
    if missing_tickers:
        print(f"⚠️ {len(missing_tickers)} actifs n'ont pas pu être téléchargés.")
        if size < 1:  # Minimum 1 actif requis
            print("❌ Pas assez d'actifs disponibles pour créer le portefeuille.")
            return None
    
    print("merge")
    Product.merge_returns_tables()
    print("merge reussi")
    portfolio = {
        "manager_id": client_data['manager_id'],
        "client_id": get_next_id("Clients", database),  # Utiliser get_next_id pour obtenir le prochain ID disponible
        "strategy": client_data['risk_profile'],
        "investment_sector": manager["investment_sector"],
        "value": client_data['investment_amount'],
        "size": size,
        "assets": [t for t in tickers if t not in missing_tickers]
    }
    

    return portfolio



###ASSETS DOWNLOADING

def download_asset(ticker: str) -> Optional[Product]:
    """
    Télécharge les données d'un actif depuis Yahoo Finance.
    
    Args:
        ticker: Symbole de l'actif à télécharger
        
    Returns:
        Optional[Product]: L'actif téléchargé ou None en cas d'erreur
    """
    try:
        # Ajout d'un délai aléatoire entre 1 et 3 secondes pour éviter les limites de requêtes
        time.sleep(5)
        
        # Téléchargement des données avec yfinance
        stock = yf.Ticker(ticker)
        info = stock.info

        # Calcul des rendements quotidiens
        start_date = datetime(2022, 1, 1)
        end_date = datetime(2024, 12, 31)
        
        hist = stock.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"⚠️ Aucune donnée historique disponible pour {ticker}")
            return None

        #Calcul des rendements quotidiens
        returns = {}
        for date, row in hist.iterrows():
            if 'Close' in row:
                returns[date.strftime('%Y-%m-%d')] = float(row['Close'])
        
        # Création de l'objet Product
        return Product(
            ticker=ticker,
            sector=info.get('sector'),
            returns=returns,
            market_cap=info.get('marketCap'),
            company_name=info.get('longName'),
            stock_exchange=info.get('exchange')
        )
        
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement des données pour {ticker}: {str(e)}")
        return None


def check_and_download_assets(tickers: List[str], db: sqlite3.Connection) -> List[str]:
    """
    Vérifie les actifs manquants et les télécharge si nécessaire.
    
    Args:
        tickers: Liste des symboles à vérifier
        db: Connexion à la base de données
        
    Returns:
        List[str]: Liste des symboles qui n'ont pas pu être téléchargés
    """
    missing_tickers = []
    
    for ticker in tickers:
        try:
            if not Product.exists(ticker):
                print(f"📥 Téléchargement des données pour {ticker}...")
                product = download_asset(ticker)
                if product:
                    try:
                        product_id = product.save(db)
                        if product_id:
                            print(f"✅ Données de {ticker} sauvegardées avec succès.")
                        else:
                            print(f"❌ Erreur lors de la sauvegarde des données de {ticker}")
                            missing_tickers.append(ticker)
                    except Exception as e:
                        print(f"❌ Erreur lors de la sauvegarde des données de {ticker}: {str(e)}")
                        missing_tickers.append(ticker)
                else:
                    print(f"❌ Impossible de télécharger les données de {ticker}")
                    missing_tickers.append(ticker)
            else:
                print(f"ℹ️ Les données de {ticker} existent déjà.")
            
        except Exception as e:
            print(f"❌ Erreur lors de la vérification/téléchargement de {ticker}: {str(e)}")
            missing_tickers.append(ticker)
    

    return missing_tickers




# def is_database_empty() -> bool:
#     """
#     Vérifie si la base de données est vide.
    
#     Returns:
#         bool: True si la base de données est vide, False sinon
#     """
#     try:
#         with BaseModel.get_db_connection() as db:
#             cursor = db.cursor()
            
#             # Vérifier si les tables principales sont vides
#             tables = ['Clients', 'Managers', 'Portfolios', 'Products', 'Returns']
#             for table in tables:
#                 cursor.execute(f"SELECT COUNT(*) FROM {table}")
#                 count = cursor.fetchone()[0]
#                 if count > 0:
#                     return False
            
#             return True
            
#     except Exception as e:
#         print(f"❌ Erreur lors de la vérification de la base de données : {str(e)}")
#         return False

# def create_sample_data() -> None:
#     """Crée des données d'exemple dans la base de données si elle est vide."""
#     try:
#         # Création de la base de données
#         BaseModel.create_database()
#         print("✅ Toutes les tables ont été créées avec succès.")
        
#         # Vérifier si la base de données est vide
#         if not is_database_empty():
#             print("ℹ️ La base de données n'est pas vide, les données d'exemple ne seront pas créées.")
#             return
            
#         print("📝 Création des données d'exemple...")
        
#         # Création des gestionnaires
#         managers = [
#             {
#                 "name": "John Smith Sr",
#                 "age": 45,
#                 "country": "USA",
#                 "email": "john.smith.manager1@example.com",
#                 "seniority": "Senior",
#                 "investment_sector": "Technology",
#                 "strategies": "Low Risk,Medium Risk"  # Convertir la liste en chaîne
#             },
#             {
#                 "name": "Marie Dupont Jr",
#                 "age": 38,
#                 "country": "France",
#                 "email": "marie.dupont.manager2@example.com",
#                 "seniority": "Mid-level",
#                 "investment_sector": "Healthcare",
#                 "strategies": "Medium Risk,High Risk"  # Convertir la liste en chaîne
#             },
#             {
#                 "name": "David Chen III",
#                 "age": 42,
#                 "country": "China",
#                 "email": "david.chen.manager3@example.com",
#                 "seniority": "Senior",
#                 "investment_sector": "Technology",
#                 "strategies": "High Risk,Medium Risk"  # Convertir la liste en chaîne
#             }
#         ]
        
#         manager_ids = []
#         with BaseModel.get_db_connection() as db:
#             for manager_data in managers:
#                 manager = AssetManager(**manager_data)
#                 manager_id = manager.save(db)
#                 if manager_id is None:
#                     print("❌ Impossible de créer un gestionnaire.")
#                     return
#                 manager_ids.append(manager_id)
            
#             # Création des clients
#             clients = [
#                 {
#                     "name": "Alice Johnson Sr",
#                     "age": 35,
#                     "country": "USA",
#                     "email": "alice.j.client1@example.com",
#                     "risk_profile": "Low Risk",
#                     "investment_amount": 100000.0,
#                     "manager_id": manager_ids[0]
#                 },
#                 {
#                     "name": "Pierre Martin Jr",
#                     "age": 45,
#                     "country": "France",
#                     "email": "pierre.m.client2@example.com",
#                     "risk_profile": "Medium Risk",
#                     "investment_amount": 150000.0,
#                     "manager_id": manager_ids[1]
#                 },
#                 {
#                     "name": "Li Wei III",
#                     "age": 40,
#                     "country": "China",
#                     "email": "li.w.client3@example.com",
#                     "risk_profile": "High Risk",
#                     "investment_amount": 200000.0,
#                     "manager_id": manager_ids[2]
#                 }
#             ]
            
#             client_ids = []
#             for client_data in clients:
#                 client = Client(**client_data)
#                 client_id = client.save(db)
#                 if client_id is None:
#                     print("❌ Impossible de créer un client.")
#                     return
#                 client_ids.append(client_id)
            
#             # Création des portefeuilles
#             portfolios = [
#                 {
#                     "manager_id": manager_ids[0],
#                     "client_id": client_ids[0],
#                     "strategy": "Low Risk",
#                     "investment_sector": "Technology",
#                     "size": 5,
#                     "value": 100000.0,
#                     "assets": ["AAPL", "MSFT"]
#                 },
#                 {
#                     "manager_id": manager_ids[1],
#                     "client_id": client_ids[1],
#                     "strategy": "Medium Risk",
#                     "investment_sector": "Healthcare",
#                     "size": 4,
#                     "value": 150000.0,
#                     "assets": ["JNJ", "PFE"]
#                 },
#                 {
#                     "manager_id": manager_ids[2],
#                     "client_id": client_ids[2],
#                     "strategy": "High Risk",
#                     "investment_sector": "Technology",
#                     "size": 6,
#                     "value": 200000.0,
#                     "assets": ["BABA", "JD"]
#                 }
#             ]
            
#             portfolio_ids = []
#             for portfolio_data in portfolios:
#                 portfolio = Portfolio(**portfolio_data)
#                 portfolio_id = portfolio.save(db)
#                 if portfolio_id is None:
#                     print("❌ Impossible de créer un portefeuille.")
#                     return
#                 portfolio_ids.append(portfolio_id)
            
#             # Mise à jour des IDs de portefeuille pour les clients
#             cursor = db.cursor()
#             for client_id, portfolio_id in zip(client_ids, portfolio_ids):
#                 cursor.execute("""
#                     UPDATE Clients
#                     SET portfolio_id = ?
#                     WHERE id = ?
#                 """, (portfolio_id, client_id))
            
#             db.commit()
#             print("✅ Base de données initialisée avec succès.")
            
#     except Exception as e:
#         print(f"❌ Une erreur inattendue s'est produite : {str(e)}")
#         return

# if __name__ == "__main__":
#     # Création de la base de données et des données d'exemple
#     create_sample_data()
    
#     # Téléchargement des données boursières
#     print("\n📥 Téléchargement des données boursières...")
#     tickers = ["AAPL", "MSFT", "JNJ", "PFE", "BABA", "JD"]
#     data = download_stock_data(tickers)
#     if data is not None:
#         save_stock_data(data)
#     else:
#         print("⚠️ Aucune donnée boursière n'a pu être téléchargée.")