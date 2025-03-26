from faker import Faker
fake = Faker()

from datetime import datetime
import random
import yfinance as yf
import sqlite3
import time

import pycountry
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="my_fund_manager")

from base_builder import (Product, get_eligible_managers, get_next_id)

from yahooquery import Screener

from typing import List, Optional



###RANDOM CLIENT GENERATOR

def generate_email(name):
    """Génère un email basé sur le nom du client."""
    name_parts = name.lower().replace(" ", ".")  # Remplace les espaces par des points
    domains = ["gmail.com", "yahoo.com", "outlook.com"]
    return f"{name_parts}@{random.choice(domains)}"

def get_random_country():
    """Génère un pays valide et une ville réelle appartenant à ce pays."""
    while True:
        country_name = "France" #fake.country()
        country = pycountry.countries.get(name=country_name)
        
        if country:  # Vérifie que le pays est valide
           return country_name

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
    while True:
        email = input("📌 Entrez l'email : ").lower()
        # Convertir le nom en format email (avec points)
        name_email = name.lower().replace(" ", ".")
        if name_email in email:
            return email
        print(f"❌ L'email doit contenir le nom du client ({name}).")


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
    """Demande et valide une date d'enregistrement (avant le 2023-01-01)."""
    while True:
        date_str = input("📌 Entrez la date d'enregistrement, doit être antérieure au 2023-01-01 (format YYYY-MM-DD) : ")
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
    query_results = s.get_screeners(sector, 20)

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
        if size < 10:  # Minimum 5 actifs requis
            print("❌ Pas assez d'actifs disponibles pour créer le portefeuille.")
            return None
    
    
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
        ticker: Symbole de l'actif
        
    Returns:
        Optional[Product]: L'actif téléchargé ou None en cas d'erreur
    """
    try:
        # Ajout d'un délai aléatoire entre 1 et 3 secondes pour éviter les limites de requêtes
        time.sleep(1)
        
        # Téléchargement des données avec yfinance
        stock = yf.Ticker(ticker)
        info = stock.info

        # Calcul des rendements hebdomadaires (vendredi soir)
        start_date = datetime(2022, 1, 1)
        end_date = datetime(2024, 12, 31)
        
        # Télécharger les données avec un intervalle hebdomadaire
        data = stock.history(start=start_date, end=end_date, interval='1wk')
        
        if data.empty:
            print(f"⚠️ Aucune donnée historique disponible pour {ticker}")
            return None

        # Calculer les rendements hebdomadaires
        data['returns'] = data['Close'].pct_change()
        
        # Renommer la colonne Close en price
        data = data.rename(columns={'Close': 'price'})
        
        # Sélectionner uniquement les colonnes nécessaires et réinitialiser l'index pour avoir la date
        data = data[['price', 'returns']].reset_index()
        
        # Renommer la colonne Date en date
        data = data.rename(columns={'Date': 'date'})
        
        # Création de l'objet Product
        return Product( 
            ticker=ticker,
            sector=info.get('sector'),
            returns=data,
            market_cap=info.get('marketCap'),
            company_name=info.get('longName'),
            stock_exchange=info.get('exchange')
        )
        
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement de {ticker}: {str(e)}")
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
                if product is None:
                    print(f"❌ Erreur lors de la récupération des données de {ticker}")
                    missing_tickers.append(ticker)
                else:
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
        except Exception as e:
            print(f"❌ Erreur lors de la vérification/téléchargement de {ticker}: {str(e)}")
            missing_tickers.append(ticker)
    
    return missing_tickers


