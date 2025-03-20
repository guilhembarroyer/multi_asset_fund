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
            client_data = generate_random_client(db)
        elif choice == "2":
            client_data = generate_precise_client(db)
        else:
            print("❌ Choix invalide ! Par défaut, on crée un client aléatoire.")
            client_data = generate_random_client(db)

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
            print("Le manager crée un portefeuille adapté au client, il doit donc récupérer les actifs du portefeuille")
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
                portfolio.save(db)
                
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
