import os
import sqlite3
from tabulate import tabulate


def get_db_path() -> str:
    """Retourne le chemin de la base de données dans le dossier parent."""
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(parent_dir, "fund_database.db")


def view_table(db: sqlite3.Connection, table_name: str) -> None:
    """
    Affiche le contenu d'une table de manière formatée.
    
    Args:
        db: Connexion à la base de données
        table_name: Nom de la table à afficher
    """
    cursor = db.cursor()
    
    # Récupérer les noms des colonnes
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    
    # Récupérer les données
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    
    if not rows:
        print(f"\n📊 Table {table_name}: Aucune donnée")
        return
    
    print(f"\n📊 Table {table_name}:")
    print(tabulate(rows, headers=columns, tablefmt="grid"))


def main() -> None:
    """Affiche le contenu de toutes les tables de la base de données."""
    db_path = get_db_path()
    
    try:
        db = sqlite3.connect(db_path)
        
        # Liste des tables à afficher
        tables = [
            "Clients",
            "Managers",
            "Manager_Strategies",
            "Manager_Clients",
            "Manager_Portfolios",
            "Portfolios",
            "Portfolio_Products",
            "Products",
            "Returns"
        ]
        
        print("\n🏦 Contenu de la base de données :")
        print("=" * 80)
        
        for table in tables:
            view_table(db, table)
        
        db.close()
        
    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite : {e}")
    except Exception as e:
        print(f"❌ Une erreur inattendue s'est produite : {e}")


if __name__ == "__main__":
    main() 