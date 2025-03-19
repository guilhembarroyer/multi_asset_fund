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

from pandas_datareader import data as pdr

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



def generate_random_client():
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


def generate_precise_client():
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
        "investment_amount": investment_amount
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
    query_results = s.get_screeners(sector, 10)

    tickers=[stock["symbol"] for stock in query_results[sector]["quotes"]]

    return tickers


def create_portfolio(manager, client_data, database):
    """Configure le portefeuille du manager en fonction de la stratégie."""
    
    size = 10
    tickers = get_corresponding_assets(manager["investment_sector"])
    
    # Vérifier et télécharger les données des actifs
    missing_tickers = check_and_download_assets(tickers, database)
    
    if missing_tickers:
        print(f"⚠️ {len(missing_tickers)} actifs n'ont pas pu être téléchargés.")
        if len(tickers) - len(missing_tickers) < 1:  # Minimum 5 actifs requis
            print("❌ Pas assez d'actifs disponibles pour créer le portefeuille.")
            return None
    
    portfolio = {
        "manager_id": client_data['manager_id'],
        "client_id": get_next_id("Clients", database),
        "strategy": client_data['risk_profile'],
        "investment_sector": manager["investment_sector"],
        "value": client_data['investment_amount'],
        "size": size,
        "assets": tickers,
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
        time.sleep(random.uniform(1, 3))
        
        # Téléchargement des données avec yfinance
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Calcul des rendements quotidiens
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        hist = stock.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"⚠️ Aucune donnée historique disponible pour {ticker}")
            return None
            
        # Calcul des rendements quotidiens
        returns = {}
        for date, row in hist.iterrows():
            if 'Close' in row:
                returns[date.strftime('%Y-%m-%d')] = float(row['Close'])
        
        # Création de l'objet Product
        return Product(
            ticker=ticker,
            sector=info.get('sector', 'Unknown'),
            returns=returns
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
                    product.save(db)
                else:
                    missing_tickers.append(ticker)
        except Exception as e:
            print(f"❌ Erreur lors de la vérification/téléchargement de {ticker}: {str(e)}")
            missing_tickers.append(ticker)
    
    return missing_tickers

def download_stock_data(tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Télécharge les données boursières pour une liste de tickers.
    
    Args:
        tickers: Liste des symboles boursiers
        start_date: Date de début au format 'YYYY-MM-DD'
        end_date: Date de fin au format 'YYYY-MM-DD'
        
    Returns:
        pd.DataFrame: DataFrame contenant les données boursières
    """
    try:
        # Initialisation de l'API Alpha Vantage
        ts = TimeSeries(key='demo', output_format='pandas')
        
        # Téléchargement des données avec un délai entre chaque ticker
        all_data = []
        for ticker in tickers:
            try:
                # Ajout d'un délai aléatoire entre 1 et 3 secondes
                time.sleep(random.uniform(1, 3))
                
                # Téléchargement des données pour un seul ticker
                data, _ = ts.get_daily_adjusted(symbol=ticker, outputsize='full')
                
                # Filtrer les données selon la plage de dates
                data = data[start_date:end_date]
                
                if not data.empty:
                    # Calcul des rendements pour ce ticker
                    data['Return'] = data['5. adjusted close'].pct_change()
                    data = data[['Return']].copy()
                    data['Ticker'] = ticker
                    data.reset_index(inplace=True)
                    all_data.append(data)
                    print(f"✅ Données téléchargées pour {ticker}")
                else:
                    print(f"⚠️ Aucune donnée disponible pour {ticker}")
                    
            except Exception as e:
                print(f"❌ Erreur lors du téléchargement de {ticker}: {str(e)}")
                continue
        
        if not all_data:
            print("❌ Aucune donnée n'a pu être téléchargée")
            return pd.DataFrame()
        
        # Combinaison de toutes les données
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # Conversion des dates en format string
        combined_data['Date'] = combined_data['date'].dt.strftime('%Y-%m-%d')
        combined_data = combined_data[['Date', 'Return', 'Ticker']]
        
        return combined_data
        
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement des données : {str(e)}")
        return pd.DataFrame()

def save_stock_data(data: pd.DataFrame, db: sqlite3.Connection) -> None:
    """
    Sauvegarde les données boursières dans la base de données.
    
    Args:
        data: DataFrame contenant les données boursières
        db: Connexion à la base de données
    """
    try:
        cursor = db.cursor()
        
        # Parcours des données
        for _, row in data.iterrows():
            date = row['Date']
            ticker = row['Ticker']
            return_value = row['Return']
            
            # Vérification de l'existence du produit
            if not Product.exists(ticker):
                # Création du produit avec un secteur par défaut
                product = Product(ticker=ticker, sector="Unknown")
                product.save(db)
            
            # Sauvegarde du rendement
            cursor.execute("""
                INSERT OR REPLACE INTO Returns (date, ticker, return_value)
                VALUES (?, ?, ?)
            """, (date, ticker, return_value))
        
        db.commit()
        print("✅ Données boursières sauvegardées avec succès.")
        
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des données : {str(e)}")
        db.rollback()

def create_sample_data() -> None:
    """Crée des données d'exemple dans la base de données."""
    try:
        # Création de la base de données
        BaseModel.create_database()
        
        # Connexion à la base de données
        with BaseModel.get_db_connection() as db:
            # Création des gestionnaires
            managers = [
                AssetManager(
                    name="John Smith",
                    age=45,
                    country="USA",
                    email="john.smith.manager@example.com",
                    seniority="Senior",
                    investment_sector="Technology",
                    strategies=["Growth", "Value"]
                ),
                AssetManager(
                    name="Marie Dupont",
                    age=38,
                    country="France",
                    email="marie.dupont.manager@example.com",
                    seniority="Mid",
                    investment_sector="Healthcare",
                    strategies=["Value", "Income"]
                ),
                AssetManager(
                    name="David Chen",
                    age=42,
                    country="China",
                    email="david.chen.manager@example.com",
                    seniority="Senior",
                    investment_sector="Technology",
                    strategies=["Growth", "Momentum"]
                )
            ]
            
            manager_ids = []
            for manager in managers:
                manager_id = manager.save(db)
                manager_ids.append(manager_id)
            
            # Création des clients
            clients = [
                Client(
                    name="Alice Johnson",
                    age=35,
                    country="USA",
                    email="alice.j.client@example.com",
                    risk_profile="Moderate",
                    investment_amount=100000.0,
                    manager_id=manager_ids[0]
                ),
                Client(
                    name="Pierre Martin",
                    age=45,
                    country="France",
                    email="pierre.m.client@example.com",
                    risk_profile="Conservative",
                    investment_amount=150000.0,
                    manager_id=manager_ids[1]
                ),
                Client(
                    name="Li Wei",
                    age=40,
                    country="China",
                    email="li.w.client@example.com",
                    risk_profile="Aggressive",
                    investment_amount=200000.0,
                    manager_id=manager_ids[2]
                )
            ]
            
            client_ids = []
            for client in clients:
                client_id = client.save(db)
                client_ids.append(client_id)
            
            # Création des portefeuilles
            portfolios = [
                Portfolio(
                    manager_id=manager_ids[0],
                    client_id=client_ids[0],
                    strategy="Growth",
                    investment_sector="Technology",
                    size=5,
                    value=100000.0,
                    assets=["AAPL", "MSFT"]
                ),
                Portfolio(
                    manager_id=manager_ids[1],
                    client_id=client_ids[1],
                    strategy="Value",
                    investment_sector="Healthcare",
                    size=4,
                    value=150000.0,
                    assets=["JNJ", "PFE"]
                ),
                Portfolio(
                    manager_id=manager_ids[2],
                    client_id=client_ids[2],
                    strategy="Growth",
                    investment_sector="Technology",
                    size=6,
                    value=200000.0,
                    assets=["BABA", "JD"]
                )
            ]
            
            portfolio_ids = []
            for portfolio in portfolios:
                portfolio_id = portfolio.save(db)
                portfolio_ids.append(portfolio_id)
            
            # Mise à jour des IDs de portefeuille pour les clients
            for client_id, portfolio_id in zip(client_ids, portfolio_ids):
                cursor = db.cursor()
                cursor.execute("""
                    UPDATE Clients
                    SET portfolio_id = ?
                    WHERE id = ?
                """, (portfolio_id, client_id))
            
            db.commit()
            print("✅ Données d'exemple créées avec succès.")
            
    except Exception as e:
        print(f"❌ Une erreur inattendue s'est produite : {str(e)}")
        if 'db' in locals():
            db.rollback()

def main():
    """Fonction principale du programme."""
    try:
        # Création des données d'exemple
        create_sample_data()
        
        # Téléchargement des données boursières
        tickers = ["AAPL", "MSFT", "JNJ", "PFE""BABA", "JD"]
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        data = download_stock_data(tickers, start_date, end_date)
        
        if not data.empty:
            # Sauvegarde des données
            with BaseModel.get_db_connection() as db:
                save_stock_data(data, db)
        
    except Exception as e:
        print(f"❌ Une erreur inattendue s'est produite : {str(e)}")

if __name__ == "__main__":
    main()