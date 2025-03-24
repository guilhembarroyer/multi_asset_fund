import sqlite3
from datetime import datetime, timedelta


from data_collector import (
    generate_precise_client,
    generate_random_client,
    manager_affiliation,
    create_manager,
    create_portfolio
)
from base_builder import Client, AssetManager, Portfolio, BaseModel
from strategies import Simulation




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
                
                print(f"✅ {client_data['name']} est à présent un(e) client(e) de 'Data Management Project'.")

        db.close()

    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite : {e}")
    except Exception as e:
        print(f"❌ Une erreur inattendue s'est produite : {e}")





def analyze_performance():
    """Fonction pour analyser les performances."""
    print("\n=== Analyse des Performances ===")
    print("1. Analyser un client spécifique")
    #print("2. Analyser un manager spécifique")
    #print("3. Analyser le fonds globalement")
    print("4. Retour au menu principal")
    
    choice = input("\nVotre choix : ")
    
    if choice == "1":
        analyze_client_performance()
    #elif choice == "2":
    #    analyze_manager_performance()
    #elif choice == "3":
        #analyze_fund_performance()
    elif choice == "4":
        main()
    else:
        print("\nChoix invalide. Veuillez réessayer.")
        analyze_performance()


def analyze_client_performance():
    """Fonction pour analyser les performances d'un client spécifique."""
    db = BaseModel.get_db_connection()
    cursor = db.cursor()
    
    # Récupérer le dernier client inscrit
    cursor.execute("""
        SELECT c.id, c.name, c.registration_date, p.id as portfolio_id, p.strategy
        FROM Clients c
        LEFT JOIN Portfolios p ON c.id = p.client_id
        ORDER BY c.id DESC
        LIMIT 1
    """)
    last_client = cursor.fetchone()
    
    print(f"\nDernier client inscrit : {last_client[1]} (inscrit le {last_client[2]})")
    print("Voulez-vous analyser ce client ? (o/n)")
    choice = input()
    
    if choice.lower() == 'o':
        client_id = last_client[0]
        portfolio_id = last_client[3]
        strategy = last_client[4]
        client_registration_date = last_client[2]
    else:
        print("Entrez l'ID du client à analyser :")
        client_id = int(input())
        
        cursor.execute("""
            SELECT c.registration_date, p.id, p.strategy
            FROM Clients c
            LEFT JOIN Portfolios p ON c.id = p.client_id
            WHERE c.id = ?
        """, (client_id,))
        result = cursor.fetchone()
        if not result:
            print("Client non trouvé.")
            return
        
        client_registration_date = result[0]
        portfolio_id = result[1]
        strategy = result[2]
    
    # Récupérer le montant initial investi
    cursor.execute("SELECT investment_amount FROM Clients WHERE id = ?", (client_id,))
    initial_amount = cursor.fetchone()[0]
    
    print(f"\nAnalyse du portefeuille {portfolio_id} (Stratégie: {strategy})")
    print(f"Début de l'analyse à partir du: {client_registration_date}")
    print(f"Montant initial investi : {initial_amount:,.2f} €")
    
    # Créer une instance de Simulation
    simulation = Simulation(db, portfolio_id, strategy, client_registration_date)
    
    # Simuler la gestion active du portefeuille
    current_date = datetime.strptime(client_registration_date, '%Y-%m-%d')
    #current_date = datetime(2024, 10, 1)
    end_date = datetime(2024, 12, 31)
    
    while current_date <= end_date:
        # Trouver le prochain vendredi
        while current_date.weekday() != 4:  # 4 = vendredi
            current_date += timedelta(days=1)
        
        # Exécuter la stratégie pour cette date
        simulation.execute_strategy(current_date)
        
        # Passer à la semaine suivante
        current_date += timedelta(days=7)
    
    

    # Afficher le résumé final
    print("\n=== Résumé de la gestion active ===")
    print(f"Période : du {datetime.strptime(client_registration_date, '%Y-%m-%d').strftime('%Y-%m-%d')} au {end_date.strftime('%Y-%m-%d')}")
    print(f"Nombre de semaines : {(end_date - datetime.strptime(client_registration_date, '%Y-%m-%d')).days // 7}")
    
    # Calculer la performance finale
    final_positions, cash = simulation.get_portfolio_positions(portfolio_id, end_date)
    if final_positions:
        portfolio_value = sum(position['value'] for position in final_positions)+cash['value']
        
        # Calculer la performance
        performance = (portfolio_value - initial_amount) / initial_amount * 100
        
        print("\n=== Performance du portefeuille ===")
        print(f"Valeur initiale : {initial_amount:,.2f} €")
        print(f"Valeur finale : {portfolio_value:,.2f} €")
        print(f"Performance : {performance:+.2f}%")
        print(f"Gain/Perte : {(portfolio_value - initial_amount):+,.2f} €")
    
    BaseModel.reinitialize_portfolio(db, portfolio_id)
    db.close()



def main() -> None:
    """
    Fonction principale du programme.
    
    Cette fonction gère le menu principal et le flux de contrôle du programme.
    """
    # Création de la base de données si elle n'existe pas
    BaseModel.create_database()
    
    print("\n=== Système de Gestion de Fonds d'Investissement ===")
    print("1. Enregistrer un nouveau client")
    print("2. Analyser les performances")
    print("3. Quitter")
    
    choice = input("\nVotre choix : ")
    
    if choice == "1":
        register_new_client()
    elif choice == "2":
        analyze_performance()
    elif choice == "3":
        print("\nAu revoir !")
    else:
        print("\nChoix invalide. Veuillez réessayer.")
        main()


if __name__ == "__main__":
    main()
