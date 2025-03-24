import os
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any



def get_db_path() -> str:
    """
    Retourne le chemin de la base de données dans le dossier parent.
    
    Returns:
        str: Chemin absolu vers la base de données
    """
    # Obtenir le chemin du dossier parent (un niveau au-dessus du dossier code_scr)
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(parent_dir, "fund_database.db")


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
                    cash_value REAL NOT NULL,
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
                    value REAL NOT NULL DEFAULT 0.0,
                    PRIMARY KEY (portfolio_id, product_id),
                    FOREIGN KEY (portfolio_id) REFERENCES Portfolios (id),
                    FOREIGN KEY (product_id) REFERENCES Products (id)
                );

                CREATE TABLE IF NOT EXISTS Deals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    action TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price REAL NOT NULL,
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

    @classmethod
    def reinitialize_portfolio(cls, db: sqlite3.Connection, portfolio_id: int) -> None:
        """
        Réinitialise les positions d'un portefeuille à 0 et remet sa valeur à sa valeur initiale.
        
        Args:
            db: Connexion à la base de données
            portfolio_id: ID du portefeuille à réinitialiser
        """
        cursor = db.cursor()
        
        # Récupérer l'investissement initial du client
        cursor.execute("""
            SELECT c.investment_amount
            FROM Clients c
            JOIN Portfolios p ON c.portfolio_id = p.id
            WHERE p.id = ?
        """, (portfolio_id,))
        
        result = cursor.fetchone()
        if not result:
            raise ValueError(f"Portefeuille {portfolio_id} non trouvé")
            
        initial_value = result[0]
        
        # Réinitialiser les positions à 0
        cursor.execute("""
            UPDATE Portfolios_Products
            SET quantity = 0,
                weight = 0.0,
                value = 0.0
            WHERE portfolio_id = ?
        """, (portfolio_id,))
        
        # Mettre à jour la valeur du portefeuille
        cursor.execute("""
            UPDATE Portfolios
            SET value = ?, cash_value = ?
            WHERE id = ?
        """, (initial_value, initial_value, portfolio_id))
        
        db.commit()



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
        self.cash_value = value
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
            INSERT INTO Portfolios (manager_id, client_id, strategy, investment_sector, size, value, cash_value) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (self.manager_id, self.client_id, self.strategy, self.investment_sector,
              self.size, self.value, self.cash_value))
        
        portfolio_id = cursor.lastrowid

    
        # Association des produits au portefeuille
        for ticker in self.assets:
            cursor.execute("""
                SELECT id FROM Products WHERE ticker = ?
            """, (ticker,))
            product_id = cursor.fetchone()[0]
            
            cursor.execute("""
                INSERT INTO Portfolios_Products (portfolio_id, product_id, quantity, weight, value)
                VALUES (?, ?, ?, ?, ?)
            """, (portfolio_id, product_id, 0, 0.0, 0.0))

        db.commit()
        return portfolio_id 
    
    @classmethod
    def update_positions(cls, db: sqlite3.Connection, portfolio_id: int, positions: List[Dict[str, Any]], cash: Dict[str, Any]) -> float:
        """
        Met à jour les positions du portefeuille et sa valeur totale.
        
        """
        cursor = db.cursor()

        for position in positions:
            cursor.execute("""
                UPDATE Portfolios_Products
                SET quantity = ?,
                    weight = ?,
                    value = ?
                WHERE portfolio_id = ? AND product_id = ?
            """, (position['quantity'], position['weight'], position['value'], portfolio_id, position['product_id']))
        
        total_value = sum(position['value'] for position in positions) + cash['value']
        print("total value", total_value, "cash value in update_positions", cash['value'])
        cursor.execute("""
            UPDATE Portfolios
            SET value = ?, cash_value = ?
            WHERE id = ?
        """, (total_value, cash['value'], portfolio_id))
        
        db.commit()
        return total_value
        
        

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

    def save(self, db: sqlite3.Connection) -> Optional[int]:
        """
        Sauvegarde l'actif dans la base de données.
        
        Args:
            db: Connexion à la base de données
            
        Returns:
            Optional[int]: ID de l'actif ou None en cas d'erreur
        """
        try:
            cursor = db.cursor()
            
            # Insérer l'actif dans la table Products
            cursor.execute("""
                INSERT INTO Products (ticker, sector, market_cap, company_name, stock_exchange)
                VALUES (?, ?, ?, ?, ?)
            """, (
                self.ticker,
                self.sector,
                self.market_cap,
                self.company_name,
                self.stock_exchange
            ))
            
            product_id = cursor.lastrowid
            
            # Créer la table Returns_{ticker} si elle n'existe pas
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS Returns_{self.ticker} (
                    date TEXT PRIMARY KEY,
                    price REAL,
                    returns REAL
                )
            """)
            
            # Insérer les rendements dans la table Returns_{ticker}
            for _, row in self.returns.iterrows():
                cursor.execute(f"""
                    INSERT OR REPLACE INTO Returns_{self.ticker} (date, price, returns)
                    VALUES (?, ?, ?)
                """, (
                    row['date'].strftime('%Y-%m-%d'),
                    row['price'],
                    row['returns']
                ))
            
            db.commit()
            return product_id
            
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde de {self.ticker}: {str(e)}")
            db.rollback()
            return None

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


class Deal(BaseModel):
    """Classe représentant une transaction sur un portefeuille."""
    
    def __init__(self, portfolio_id: int, product_id: int, date: str, 
                 action: str, quantity: int, price: float):
        """
        Initialise une nouvelle transaction.
        
        Args:
            portfolio_id: ID du portefeuille concerné
            product_id: ID du produit concerné
            date: Date d'exécution de la transaction
            action: Type d'action (BUY/SELL)
            quantity: Quantité (positive pour achat, négative pour vente)
            price: Prix d'exécution
        """
        self.portfolio_id = portfolio_id
        self.product_id = product_id
        self.date = date
        self.action = action
        self.quantity = quantity
        self.price = price
    
    def save(self, db: sqlite3.Connection) -> int:
        """
        Sauvegarde la transaction dans la base de données.
        
        Args:
            db: Connexion à la base de données
            
        Returns:
            int: ID de la transaction créée
        """
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO Deals (portfolio_id, product_id, date, action, quantity, price)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            self.portfolio_id,
            self.product_id,
            self.date,
            self.action,
            self.quantity,
            self.price
        ))
        
        deal_id = cursor.lastrowid
        db.commit()
        return deal_id
    
    @classmethod
    def save_multiple(cls, deals: List['Deal'], db: sqlite3.Connection) -> None:
        """
        Sauvegarde plusieurs deals dans la base de données.
        
        Args:
            deals: Liste des deals à sauvegarder
            db: Connexion à la base de données
        """
        cursor = db.cursor()
        
        # Vérifier les deals existants pour éviter les doublons
        for deal in deals:
            cursor.execute("""
                SELECT COUNT(*)
                FROM Deals
                WHERE portfolio_id = ?
                AND product_id = ?
                AND date = ?
                AND action = ?
                AND quantity = ?
                AND price = ?
            """, (deal.portfolio_id, deal.product_id, deal.date, 
                  deal.action, deal.quantity, deal.price))
            
            if cursor.fetchone()[0] == 0:  # Si le deal n'existe pas déjà
                cursor.execute("""
                    INSERT INTO Deals (portfolio_id, product_id, date, action, quantity, price)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (deal.portfolio_id, deal.product_id, deal.date, 
                      deal.action, deal.quantity, deal.price))
        
        db.commit()
    
    @classmethod
    def get_portfolio_deals(cls, portfolio_id: int, db: sqlite3.Connection) -> List[Dict[str, Any]]:
        """
        Récupère toutes les transactions d'un portefeuille.
        
        Args:
            portfolio_id: ID du portefeuille
            db: Connexion à la base de données
            
        Returns:
            List[Dict[str, Any]]: Liste des transactions
        """
        cursor = db.cursor()
        cursor.execute("""
            SELECT d.id, d.date, d.action, d.quantity, d.price, p.ticker
            FROM Deals d
            JOIN Products p ON d.product_id = p.id
            WHERE d.portfolio_id = ?
            ORDER BY d.date
        """, (portfolio_id,))
        
        deals = []
        for row in cursor.fetchall():
            deals.append({
                'id': row[0],
                'date': row[1],
                'action': row[2],
                'quantity': row[3],
                'price': row[4],
                'ticker': row[5]
            })
        
        return deals

