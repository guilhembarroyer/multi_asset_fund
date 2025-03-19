import os
import sqlite3
from typing import Optional

from data_collector import (
    generate_precise_client,
    generate_random_client,
    manager_affiliation,
    create_manager,
    create_portfolio,
    check_and_download_assets
)
from base_builder import Client, AssetManager, Portfolio, BaseModel


def get_db_path() -> str:
    """
    Retourne le chemin de la base de données dans le dossier parent.
    
    Returns:
        str: Chemin absolu vers la base de données
    """
    # Obtenir le chemin du dossier parent (un niveau au-dessus du dossier code_scr)
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(parent_dir, "fund_database.db")


def create_database() -> None:
    """
    Crée la base de données et ses tables.
    
    Cette fonction initialise la structure de la base de données avec toutes les tables nécessaires
    pour le système de gestion de fonds d'investissement.
    """
    db_file = get_db_path()
    
    try:
        # Connexion à la base de données SQLite
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Création des tables
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS Clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                age INTEGER NOT NULL,
                country TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                risk_profile TEXT NOT NULL,
                registration_date TEXT NOT NULL,
                investment_amount REAL NOT NULL,
                manager_id INTEGER NOT NULL,
                portfolio_id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Managers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                age INTEGER NOT NULL,
                country TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                seniority TEXT NOT NULL,
                investment_sector TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Manager_Strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER NOT NULL,
                strategy TEXT NOT NULL,
                FOREIGN KEY (manager_id) REFERENCES Managers(id),
                UNIQUE (manager_id, strategy)
            );

            CREATE TABLE IF NOT EXISTS Manager_Clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER NOT NULL,
                client_id INTEGER NOT NULL,
                FOREIGN KEY (manager_id) REFERENCES Managers(id),
                FOREIGN KEY (client_id) REFERENCES Clients(id),
                UNIQUE (manager_id, client_id)
            );

            CREATE TABLE IF NOT EXISTS Manager_Portfolios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER NOT NULL,
                portfolio_id INTEGER NOT NULL,
                FOREIGN KEY (manager_id) REFERENCES Managers(id),
                FOREIGN KEY (portfolio_id) REFERENCES Portfolios(id),
                UNIQUE (manager_id, portfolio_id)
            );

            CREATE TABLE IF NOT EXISTS Portfolios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER NOT NULL,
                client_id INTEGER NOT NULL,
                strategy TEXT NOT NULL,
                investment_sector TEXT NOT NULL,
                size INTEGER NOT NULL,
                value REAL NOT NULL,
                FOREIGN KEY (client_id) REFERENCES Clients(id),
                FOREIGN KEY (manager_id) REFERENCES Managers(id)
            );

            CREATE TABLE IF NOT EXISTS Portfolio_Products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                FOREIGN KEY (portfolio_id) REFERENCES Portfolios(id),
                FOREIGN KEY (product_id) REFERENCES Products(id),
                UNIQUE (portfolio_id, product_id)
            );

            CREATE TABLE IF NOT EXISTS Products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL UNIQUE,
                sector TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Returns (
                date TEXT PRIMARY KEY,
                ticker TEXT NOT NULL,
                return_value REAL NOT NULL,
                FOREIGN KEY (ticker) REFERENCES Products(ticker)
            );
        """)
        print("✅ Toutes les tables ont été créées avec succès.")
        conn.commit()
        
    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite : {e}")
    finally:
        if conn:
            conn.close()


def register_new_client() -> None:
    """
    Enregistre un nouveau client dans le système.
    
    Cette fonction guide l'utilisateur à travers le processus d'enregistrement d'un nouveau client,
    incluant la création du portefeuille et l'attribution d'un manager.
    """
    try:
        db = BaseModel.get_db_connection()
        sortie = False

        # Demande à l'utilisateur son choix
        choice = input("Voulez-vous créer un client aléatoire (1) ou manuel (2) ? 📌 Entrez 1 ou 2 : ")

        if choice == "1":
            client_data = generate_random_client()
        elif choice == "2":
            client_data = generate_precise_client()
        else:
            print("❌ Choix invalide ! Par défaut, on crée un client aléatoire.")
            client_data = generate_random_client()

        print(client_data)

        # Attribution d'un manager
        assigned_manager = manager_affiliation(client_data, db)

        if assigned_manager:
            client_data["manager_id"] = assigned_manager["id"]
            print(f"✅ Manager attribué : {assigned_manager['name']}")
        else:
            print("⚠️ Aucun asset manager opérant dans le pays du client, opérant la stratégie adéquate, "
                  "et/ou ayant le niveau de séniorité adapté au client proposé n'a été trouvé.")
            choice = input("Voulez-vous que le fonds recrute un asset manager adéquat (Oui) ou préférez-vous "
                         "annuler l'enregistrement du client (Non) 📌 Entrez 'Oui' ou 'Non' : ")
            
            if choice == "Oui":
                assigned_manager = create_manager(client_data, db)
            elif choice == "Non":
                client_data = None
                print("❌ Enregistrement du client annulé.")
                sortie = True
            else:
                print("❌ Choix invalide ! Par défaut, on trouve un Asset Manager adapté.")
                assigned_manager = create_manager(client_data, db)
            
            if not sortie:
                print(f"✅ Un Manager a le profil correspondant: {assigned_manager['name']}")
                choice = input("Devons-nous le recruter? 📌 Entrez 'Oui' ou 'Non' : ")

                if choice == "Oui":
                    print(assigned_manager)
                    manager = AssetManager(**assigned_manager)
                    client_data["manager_id"] = manager.save(db)
                    print(f"✅ Manager {manager.name} recruté avec succès.")
                else:
                    print("❌ Enregistrement du client annulé.")
                    sortie = True

        if not sortie:
            # Création du portefeuille
            portfolio_data = create_portfolio(assigned_manager, client_data, db)
            
            if portfolio_data is None:
                print("❌ Impossible de créer le portefeuille. Enregistrement du client annulé.")
                sortie = True
            else:
                print("✅ Portefeuille créé avec succès.")
                
                # Création du client
                client = Client(**client_data)
                client_id = client.save(db)
                
                # Mise à jour du portefeuille avec l'ID du client
                portfolio_data["client_id"] = client_id
                portfolio = Portfolio(**portfolio_data)
                portfolio_id = portfolio.save(db)
                
                # Mise à jour du client avec l'ID du portefeuille
                client.portfolio_id = portfolio_id
                client.save(db)
                
                print(f"✅ {client_data['name']} est à présent un client de 'Data Management Project'.")

        db.close()

    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite : {e}")
    except Exception as e:
        print(f"❌ Une erreur inattendue s'est produite : {e}")


def main() -> None:
    """
    Fonction principale du programme.
    
    Cette fonction gère le menu principal et le flux de contrôle du programme.
    """
    print("🏦 Bienvenue dans le système de gestion de fonds d'investissement")
    
    # Création de la base de données si elle n'existe pas
    BaseModel.create_database()
    
    while True:
        print("\n📋 Menu principal :")
        print("1. Enregistrer un nouveau client")
        print("2. Quitter")
        
        choice = input("\n📌 Votre choix : ")
        
        if choice == "1":
            register_new_client()
        elif choice == "2":
            print("👋 Au revoir !")
            break
        else:
            print("❌ Choix invalide !")


if __name__ == "__main__":
    main()
