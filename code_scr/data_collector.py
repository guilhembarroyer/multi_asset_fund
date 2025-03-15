from faker import Faker
fake = Faker()

from datetime import datetime, timedelta
import random

import pycountry
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="geo_validator")

from base_builder import get_eligible_managers, get_next_id

from yahooquery import Screener



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
    while (email := input("📌 Entrez l'email : ")).lower().replace(" ", ".") not in name.lower().replace(" ", "."):
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
                print("❌ La date doit être avant le 01-01-2023.")
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
    name=fake.name()


    return {
        "name": name,
        "age": fake.random_int(min=25, max=60),
        "country": client_country,
        "email": generate_email(name),
        "seniority": client_seniority,
        "investment_sector": fake.random_element(['ms_basic_materials','ms_communication_services','ms_consumer_cyclical','ms_consumer_defensive','ms_energy','ms_financial_services','ms_healthcare','ms_industrials','ms_real_estate','ms_technology','ms_utilities']),
        "strategies": [client_risk_profile] + [random.choice([profile for profile in ["Low Risk", "Medium Risk", "High Risk"] if profile != client_risk_profile])],
        "clients_id": [get_next_id("Clients", database)],
        "portfolios_id": [1] #[get_next_id("Portfolios", database)]
    }


### PORTFOLIO CREATION/CONFIGURATION

def get_corresponding_assets(sector, size):
    # Initialiser le screener
    s = Screener()

    # Récupérer les actions les plus échangées
    query_results = s.get_screeners(sector, size)

    tickers=[stock["symbol"] for stock in query_results[sector]["quotes"]]

    return tickers


def config_portfolio(manager, client_data, database):
    """Configure le portefeuille du manager en fonction de la stratégie."""
    
    size=random.randint(10, 50)
    portfolio = {
        "manager_id": manager["id"],
        "client_id": get_next_id("Clients", database),
        "strategy": client_data['risk_profile'],
        "investment_sector": manager["investment_sector"],
        "value": client_data['investment_amount'],
        "size": size,
        "assets": get_corresponding_assets(manager["investment_sector"], size),
    }
    
    return portfolio