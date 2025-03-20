import os
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any
import time


def get_db_path() -> str:
    """
    Retourne le chemin de la base de données dans le dossier parent.
    
    Returns:
        str: Chemin absolu vers la base de données
    """
    # Obtenir le chemin du dossier parent (un niveau au-dessus du dossier code_scr)
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(parent_dir, "fund_database.db")


def get_eligible_managers(db: sqlite3.Connection, client_country: str, client_seniority: str, client_strategie: str) -> List[Dict[str, Any]]:
    """
    Récupère les managers compatibles depuis la base de données selon les critères.
    
    Args:
        db: Connexion à la base de données
        client_country: Pays du client
        client_seniority: Niveau de séniorité du client
        client_strategie: Stratégie d'investissement du client
        
    Returns:
        List[Dict[str, Any]]: Liste des managers éligibles
    """
    cursor = db.cursor()
    cursor.execute("""
        SELECT m.id, m.name, m.age, m.country, m.email, m.seniority, m.investment_sector
        FROM Managers m
        INNER JOIN Manager_Strategies ms ON m.id = ms.manager_id
        WHERE m.country = ? AND m.seniority = ? AND ms.strategy LIKE ?
    """, (client_country, client_seniority, f"%{client_strategie}%"))
    
    rows = cursor.fetchall()
    eligible_managers = []
    
    for row in rows:
        manager = {
            'id': row[0],
            'name': row[1],
            'age': row[2],
            'country': row[3],
            'email': row[4],
            'seniority': row[5],
            'investment_sector': row[6]
        }
        eligible_managers.append(manager)
    
    return eligible_managers


class BaseModel:
    """Classe de base pour tous les modèles de données."""
    
    @classmethod
    def create_database(cls) -> None:
        """
        Crée la base de données et ses tables.
        
        Cette méthode initialise la structure de la base de données avec toutes les tables nécessaires
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
                    portfolio_id INTEGER NOT NULL,
                    FOREIGN KEY (manager_id) REFERENCES Managers (id),
                    FOREIGN KEY (portfolio_id) REFERENCES Portfolios (id)
                );

                CREATE TABLE IF NOT EXISTS Managers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    age INTEGER NOT NULL,
                    country TEXT NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    seniority TEXT NOT NULL,
                    investment_sector TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS Manager_Portfolios (
                    manager_id INTEGER,
                    portfolio_id INTEGER,
                    PRIMARY KEY (manager_id, portfolio_id),
                    FOREIGN KEY (manager_id) REFERENCES Managers (id),
                    FOREIGN KEY (portfolio_id) REFERENCES Portfolios (id)
                );
                                 
                CREATE TABLE IF NOT EXISTS Manager_Strategies (
                    manager_id INTEGER,
                    strategy TEXT,
                    PRIMARY KEY (manager_id, strategy),
                    FOREIGN KEY (manager_id) REFERENCES Managers (id)
                );

                CREATE TABLE IF NOT EXISTS Portfolios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    manager_id INTEGER NOT NULL,
                    client_id INTEGER NOT NULL,
                    strategy TEXT NOT NULL,
                    investment_sector TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    value REAL NOT NULL,
                    FOREIGN KEY (manager_id) REFERENCES Managers (id),
                    FOREIGN KEY (client_id) REFERENCES Clients (id)
                );


                CREATE TABLE IF NOT EXISTS Products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL UNIQUE,
                    sector TEXT NOT NULL,
                    market_cap REAL,
                    company_name TEXT,
                    stock_exchange TEXT
                );

                CREATE TABLE IF NOT EXISTS Portfolios_Products (
                    portfolio_id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER NOT NULL,
                    weight REAL NOT NULL DEFAULT 0.0,
                    PRIMARY KEY (portfolio_id, product_id),
                    FOREIGN KEY (portfolio_id) REFERENCES Portfolios (id),
                    FOREIGN KEY (product_id) REFERENCES Products (id)
                );

            """)
            
            conn.commit()
            print("✅ Toutes les tables ont été créées avec succès.")
            
        except Exception as e:
            print(f"❌ Une erreur s'est produite lors de la création de la base de données : {str(e)}")
            if 'conn' in locals():
                conn.rollback()
        finally:
            if 'conn' in locals():
                conn.close()

    @classmethod
    def get_db_connection(cls) -> sqlite3.Connection:   
        """
        Crée et retourne une connexion à la base de données avec timeout.
        
        Returns:
            sqlite3.Connection: Connexion à la base de données
        """
        conn = sqlite3.connect(get_db_path(), timeout=10)  # 30 secondes de timeout
        return conn



class Client(BaseModel):
    """Classe représentant un client du fonds d'investissement."""
    
    def __init__(self, name: str, age: int, country: str, email: str, 
                 risk_profile: str, investment_amount: float, 
                 registration_date: Optional[str] = None,
                 manager_id: Optional[int] = None, portfolio_id: Optional[int] = None):
        self.name = name
        self.age = age
        self.country = country
        self.email = email
        self.risk_profile = risk_profile
        self.registration_date = registration_date or datetime.now().strftime("%Y-%m-%d")
        self.investment_amount = investment_amount
        self.manager_id = manager_id
        self.portfolio_id = portfolio_id

    def save(self, db: sqlite3.Connection) -> int:
        """
        Sauvegarde le client dans la base de données.
        
        Args:
            db: Connexion à la base de données
            
        Returns:
            int: ID du client créé
        """
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO Clients (name, age, country, email, risk_profile, 
                               registration_date, investment_amount, manager_id, portfolio_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.name, self.age, self.country, self.email, self.risk_profile,
              self.registration_date, self.investment_amount, self.manager_id, self.portfolio_id))
        
        client_id = cursor.lastrowid
        
        
        # Création de la relation manager-client
        if self.manager_id and self.portfolio_id:
            cursor.execute("""
                INSERT INTO Manager_Portfolios (manager_id, portfolio_id)
                VALUES (?, ?)
            """, (self.manager_id, self.portfolio_id))
        
        db.commit()
        return client_id

    @classmethod
    def get_by_id(cls, client_id: int) -> Optional['Client']:
        """
        Récupère un client par son ID.
        
        Args:
            client_id: ID du client à récupérer
            
        Returns:
            Optional[Client]: Le client trouvé ou None si non trouvé
        """
        # with cls.get_db_connection() as db:
        #     cursor = db.cursor()
        #     cursor.execute("""
        #         SELECT name, age, country, email, risk_profile, registration_date,
        #                investment_amount, manager_id, portfolio_id
        #         FROM Clients
        #         WHERE id = ?
        #     """, (client_id,))
            
        #     row = cursor.fetchone()
        #     if row:
        #         return cls(*row)
        #     return None
        pass


class AssetManager(BaseModel):
    """Classe représentant un gestionnaire d'actifs."""
    
    def __init__(self, name: str, age: int, country: str, email: str,
                 seniority: str, investment_sector: str, strategies: Optional[List[str]] = None):
        self.name = name
        self.age = age
        self.country = country
        self.email = email
        self.seniority = seniority
        self.investment_sector = investment_sector
        self.strategies = strategies or []

    def save(self, db: sqlite3.Connection) -> int:
        """
        Sauvegarde le gestionnaire dans la base de données.
        
        Args:
            db: Connexion à la base de données
            
        Returns:
            int: ID du gestionnaire créé
        """
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO Managers (name, age, country, email, seniority, investment_sector) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, (self.name, self.age, self.country, self.email, self.seniority, self.investment_sector))

        manager_id = cursor.lastrowid

        # Sauvegarde des stratégies dans la table Manager_Strategies
        for strategy in self.strategies:
            cursor.execute("""
                INSERT INTO Manager_Strategies (manager_id, strategy)
                VALUES (?, ?)
            """, (manager_id, strategy))

        db.commit()
        return manager_id

    @classmethod
    def get_by_id(cls, manager_id: int) -> Optional['AssetManager']:
        """
        Récupère un gestionnaire par son ID.
        
        Args:
            manager_id: ID du gestionnaire à récupérer
            
        Returns:
            Optional[AssetManager]: Le gestionnaire trouvé ou None si non trouvé
        """
        # with cls.get_db_connection() as db:
        #     cursor = db.cursor()
        #     cursor.execute("""
        #         SELECT m.name, m.age, m.country, m.email, m.seniority, m.investment_sector
        #         FROM Managers m
        #         WHERE m.id = ?
        #     """, (manager_id,))
            
        #     row = cursor.fetchone()
        #     if row:
        #         # Récupération des stratégies
        #         cursor.execute("""
        #             SELECT strategy
        #             FROM manager_portfolios
        #             WHERE manager_id = ?
        #         """, (manager_id,))
        #         strategies = [row[0] for row in cursor.fetchall()]
                
        #         return cls(*row, strategies=strategies)
        #     return None
        pass


class Portfolio(BaseModel):
    """Classe représentant un portefeuille d'investissement."""
    
    def __init__(self, manager_id: int, client_id: int, strategy: str,
                 investment_sector: str, size: int, value: float,
                 assets: Optional[List[str]] = None):
        self.manager_id = manager_id
        self.client_id = client_id
        self.strategy = strategy
        self.investment_sector = investment_sector
        self.size = size
        self.value = value
        self.assets = assets or []

    def save(self, db: sqlite3.Connection) -> int:
        """
        Sauvegarde le portefeuille dans la base de données.
        
        Args:
            db: Connexion à la base de données
            
        Returns:
            int: ID du portefeuille créé
        """
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO Portfolios (manager_id, client_id, strategy, investment_sector, size, value) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, (self.manager_id, self.client_id, self.strategy, self.investment_sector,
              self.size, self.value))
        
        portfolio_id = cursor.lastrowid

    
        # Association des produits au portefeuille
        for ticker in self.assets:
            cursor.execute("""
                SELECT id FROM Products WHERE ticker = ?
            """, (ticker,))
            product_id = cursor.fetchone()[0]
            
            cursor.execute("""
                INSERT INTO Portfolios_Products (portfolio_id, product_id, quantity, weight)
                VALUES (?, ?, ?, ?)
            """, (portfolio_id, product_id, 1, 0.0))

        db.commit()
        return portfolio_id 

    @classmethod
    def get_by_id(cls, portfolio_id: int) -> Optional['Portfolio']:
        """
        Récupère un portefeuille par son ID.
        
        Args:
            portfolio_id: ID du portefeuille à récupérer
            
        Returns:
            Optional[Portfolio]: Le portefeuille trouvé ou None si non trouvé
        """
        # with cls.get_db_connection() as db:
        #     cursor = db.cursor()
        #     cursor.execute("""
        #         SELECT p.manager_id, p.client_id, p.strategy, p.investment_sector,
        #                p.size, p.value
        #         FROM Portfolios p
        #         WHERE p.id = ?
        #     """, (portfolio_id,))
            
        #     row = cursor.fetchone()
        #     if row:
        #         # Récupération des actifs
        #         cursor.execute("""
        #             SELECT pr.ticker
        #             FROM portfolios_products pp
        #             JOIN Products pr ON pp.product_id = pr.id
        #             WHERE pp.portfolio_id = ?
        #         """, (portfolio_id,))
        #         assets = [row[0] for row in cursor.fetchall()]
                
        #         return cls(*row, assets=assets)
        #     return None
        pass


class Product(BaseModel):
    """Classe représentant un produit financier."""
    
    def __init__(self, ticker: str, sector: str, returns: Dict[str, float],
                 market_cap: Optional[float] = None, company_name: Optional[str] = None,
                 stock_exchange: Optional[str] = None):
        self.ticker = ticker
        self.sector = sector
        self.returns = returns
        self.market_cap = market_cap
        self.company_name = company_name
        self.stock_exchange = stock_exchange

    def save(self, db: sqlite3.Connection) -> int:
        """
        Sauvegarde le produit dans la base de données.
        
        Args:
            db: Connexion à la base de données
            
        Returns:
            int: ID du produit créé
        """
        try:
            cursor = db.cursor()
            
            # Démarrer une transaction
            db.execute("BEGIN IMMEDIATE")
            
            # Insérer le produit
            cursor.execute("""
                INSERT INTO Products (ticker, sector, market_cap, company_name, stock_exchange)
                VALUES (?, ?, ?, ?, ?)
            """, (self.ticker, self.sector, self.market_cap, self.company_name, self.stock_exchange))
            
            product_id = cursor.lastrowid
            
            # Créer une table spécifique pour ce produit
            table_name = f"Returns_{self.ticker}"
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    date TEXT PRIMARY KEY,
                    returns REAL NOT NULL
                )
            """)
            
            # Insérer les rendements dans la table spécifique
            for date, value in self.returns.items():
                cursor.execute(f"""
                    INSERT OR REPLACE INTO {table_name} (date, returns)
                    VALUES (?, ?)
                """, (date, value))
                
        
            
            # Valider la transaction
            db.commit()
            return product_id
            
        except sqlite3.OperationalError as e:
            # En cas d'erreur, annuler la transaction
            db.rollback()
            raise e

    @classmethod
    def exists(cls, ticker: str) -> bool:
        """
        Vérifie si un produit existe déjà dans la base de données.
        
        Args:
            ticker: Symbole du produit à vérifier
            
        Returns:
            bool: True si le produit existe, False sinon
        """
        with cls.get_db_connection() as db:
            cursor = db.cursor()
            cursor.execute("SELECT 1 FROM Products WHERE ticker = ?", (ticker,))
            return cursor.fetchone() is not None

    @classmethod
    def get_by_ticker(cls, ticker: str) -> Optional['Product']:
        """
        Récupère un produit par son symbole.
        
        Args:
            ticker: Symbole du produit à récupérer
            
        Returns:
            Optional[Product]: Le produit trouvé ou None si non trouvé
        """
        #with cls.get_db_connection() as db:
        #    cursor = db.cursor()
        #    cursor.execute("""
        #        SELECT p.ticker, p.sector
        #        FROM Products p
        #        WHERE p.ticker = ?
        #    """, (ticker,))
            
        #    row = cursor.fetchone()
        #    if row:
        #        # Récupération des rendements
        #        cursor.execute("""
        #            SELECT date, return_value
        #            FROM Returns
        #            WHERE ticker = ?
        #        """, (ticker,))
        #        returns = {row[0]: row[1] for row in cursor.fetchall()}
                
        #        return cls(*row, returns=returns)
        #    return None
        pass

    
    


### Fonctions utilitaires ###

def get_next_id(table: str, db: sqlite3.Connection) -> int:
    """
    Récupère l'ID maximal de la table et retourne l'ID suivant.
    
    Args:
        table: Nom de la table
        db: Connexion à la base de données
        
    Returns:
        int: Prochain ID disponible
    """
    cursor = db.cursor()
    cursor.execute(f"SELECT MAX(id) FROM {table}")
    max_id = cursor.fetchone()[0]
    return 1 if max_id is None else max_id + 1


class PortfolioProduct(BaseModel):
    """Classe représentant la relation entre un portefeuille et un produit."""
    
    def __init__(self, portfolio_id: int, product_id: int, quantity: int, weight: float = 0.0):
        self.portfolio_id = portfolio_id
        self.product_id = product_id
        self.quantity = quantity
        self.weight = weight

    def save(self, db: sqlite3.Connection) -> None:
        """
        Sauvegarde la relation portefeuille-produit dans la base de données.
        
        Args:
            db: Connexion à la base de données
        """
        cursor = db.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO Portfolios_Products (portfolio_id, product_id, quantity, weight)
            VALUES (?, ?, ?, ?)
        """, (self.portfolio_id, self.product_id, self.quantity, self.weight))
        db.commit()

    def update_weight(self, db: sqlite3.Connection, new_weight: float) -> None:
        """
        Met à jour le poids d'un produit dans un portefeuille.
        
        Args:
            db: Connexion à la base de données
            new_weight: Nouveau poids à appliquer
        """
        cursor = db.cursor()
        cursor.execute("""
            UPDATE Portfolios_Products
            SET weight = ?
            WHERE portfolio_id = ? AND product_id = ?
        """, (new_weight, self.portfolio_id, self.product_id))
        db.commit()
        self.weight = new_weight

    @classmethod
    def get_by_portfolio_id(cls, portfolio_id: int, db: sqlite3.Connection) -> List['PortfolioProduct']:
        """
        Récupère toutes les relations portefeuille-produit pour un portefeuille donné.
        
        Args:
            portfolio_id: ID du portefeuille
            db: Connexion à la base de données
            
        Returns:
            List[PortfolioProduct]: Liste des relations portefeuille-produit
        """
        cursor = db.cursor()
        cursor.execute("""
            SELECT portfolio_id, product_id, quantity, weight
            FROM Portfolios_Products
            WHERE portfolio_id = ?
        """, (portfolio_id,))
        
        return [cls(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()]


class Deal(BaseModel):
    """Classe représentant une transaction (deal) dans le fonds."""
    
    def __init__(self, portfolio_id: int, manager_id: int, date: str, ticker: str,
                 action: str, quantity: int, price: float, total_value: float,
                 portfolio_weight: float, manager_weight: float, fund_weight: float):
        self.portfolio_id = portfolio_id
        self.manager_id = manager_id
        self.date = date
        self.ticker = ticker
        self.action = action  # "BUY" ou "SELL"
        self.quantity = quantity
        self.price = price
        self.total_value = total_value
        self.portfolio_weight = portfolio_weight
        self.manager_weight = manager_weight
        self.fund_weight = fund_weight

    def save(self, db: sqlite3.Connection) -> None:
        """
        Sauvegarde le deal dans les différentes tables.
        
        Args:
            db: Connexion à la base de données
        """
        cursor = db.cursor()
        
        # Sauvegarder dans Deals_portfolio
        cursor.execute("""
            INSERT INTO Deals_portfolio 
            (portfolio_id, date, ticker, action, quantity, price, total_value, 
             portfolio_weight, manager_weight, fund_weight)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.portfolio_id, self.date, self.ticker, self.action, self.quantity,
              self.price, self.total_value, self.portfolio_weight, self.manager_weight,
              self.fund_weight))
        
        # Sauvegarder dans Deals_manager
        cursor.execute("""
            INSERT INTO Deals_manager 
            (manager_id, date, ticker, action, quantity, price, total_value, 
             portfolio_weight, manager_weight, fund_weight)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.manager_id, self.date, self.ticker, self.action, self.quantity,
              self.price, self.total_value, self.portfolio_weight, self.manager_weight,
              self.fund_weight))
        
        # Sauvegarder dans Deals_fund
        cursor.execute("""
            INSERT INTO Deals_fund 
            (date, ticker, action, quantity, price, total_value, 
             portfolio_weight, manager_weight, fund_weight)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.date, self.ticker, self.action, self.quantity, self.price,
              self.total_value, self.portfolio_weight, self.manager_weight,
              self.fund_weight))
        
        db.commit()

    @classmethod
    def get_by_portfolio_id(cls, portfolio_id: int, db: sqlite3.Connection) -> List['Deal']:
        """
        Récupère tous les deals d'un portefeuille.
        
        Args:
            portfolio_id: ID du portefeuille
            db: Connexion à la base de données
            
        Returns:
            List[Deal]: Liste des deals du portefeuille
        """
        cursor = db.cursor()
        cursor.execute("""
            SELECT portfolio_id, manager_id, date, ticker, action, quantity, price,
                   total_value, portfolio_weight, manager_weight, fund_weight
            FROM Deals_portfolio
            WHERE portfolio_id = ?
            ORDER BY date
        """, (portfolio_id,))
        
        return [cls(row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                   row[7], row[8], row[9], row[10]) for row in cursor.fetchall()]

    @classmethod
    def get_by_manager_id(cls, manager_id: int, db: sqlite3.Connection) -> List['Deal']:
        """
        Récupère tous les deals d'un manager.
        
        Args:
            manager_id: ID du manager
            db: Connexion à la base de données
            
        Returns:
            List[Deal]: Liste des deals du manager
        """
        cursor = db.cursor()
        cursor.execute("""
            SELECT portfolio_id, manager_id, date, ticker, action, quantity, price,
                   total_value, portfolio_weight, manager_weight, fund_weight
            FROM Deals_manager
            WHERE manager_id = ?
            ORDER BY date
        """, (manager_id,))
        
        return [cls(row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                   row[7], row[8], row[9], row[10]) for row in cursor.fetchall()]

    @classmethod
    def get_all_fund_deals(cls, db: sqlite3.Connection) -> List['Deal']:
        """
        Récupère tous les deals du fonds.
        
        Args:
            db: Connexion à la base de données
            
        Returns:
            List[Deal]: Liste de tous les deals du fonds
        """
        cursor = db.cursor()
        cursor.execute("""
            SELECT portfolio_id, manager_id, date, ticker, action, quantity, price,
                   total_value, portfolio_weight, manager_weight, fund_weight
            FROM Deals_fund
            ORDER BY date
        """)
        
        return [cls(row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                   row[7], row[8], row[9], row[10]) for row in cursor.fetchall()]
