import sqlite3


### Fonctions utilitaires ###

def get_next_id(table, db):
    """Récupère l'ID maximal de la table et retourne l'ID suivant."""
    cursor = db.cursor()
    cursor.execute(f"SELECT MAX(id) FROM {table}")  # Remplace 'manager_id' par le nom de la colonne de l'ID
    max_id = cursor.fetchone()[0]
    
    if max_id is None:
        return 1  # Si la table est vide, le premier ID sera 1
    return max_id + 1



### Classes ###

## Client

class Client:
    """Représente un client du fond."""
    def __init__(self, 
                 name, 
                 age, 
                 country, 
                 email, 
                 risk_profile, 
                 registration_date, 
                 investment_amount, 
                 manager_id, 
                 portfolio_id):
        
        self.name = name
        self.age = age
        self.country = country
        self.email = email
        self.risk_profile = risk_profile
        self.registration_date = registration_date
        self.investment_amount = investment_amount
        self.manager_id = manager_id
        self.portfolio_id = portfolio_id

    def client_exists(self, db):
        """Vérifie si un client existe déjà avec cet email et ce nom."""
        cursor = db.cursor()
        cursor.execute("SELECT COUNT(*) FROM Clients WHERE email = ? AND name = ?", (self.email, self.name))
        result = cursor.fetchone()
        return result[0] > 0

    def save(self, db):
        """Ajoute le client dans la base de données."""
        if self.client_exists(db):
            print(f"❌ Un client avec cet email et ce nom existe déjà.")
        else:
            cursor = db.cursor()
            cursor.execute("""
                INSERT INTO Clients (name, age, country, email, risk_profile, registration_date, investment_amount, manager_id, portfolio_id) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                (self.name, self.age, self.country, self.email, self.risk_profile, self.registration_date, self.investment_amount, self.manager_id, self.portfolio_id))

            db.commit()
            print(f"✅ Client {self.name} ajouté avec succès.")





## Manager

def get_eligible_managers(db, client_country, client_seniority, client_strategie):
    """Récupère les managers compatibles depuis la base de données selon les critères."""
    
    # Requête SQL pour filtrer les managers selon le pays, la séniorité et la stratégie
    cursor = db.cursor()
    cursor.execute("""
        SELECT * 
        FROM Managers
        INNER JOIN Manager_Strategies ON Managers.id = Manager_Strategies.manager_id           
        WHERE Managers.country = ? AND Managers.seniority = ? AND Manager_Strategies.strategy LIKE ?
    """, (client_country, client_seniority, f"%{client_strategie}%"))
    
    # Récupération des résultats
    rows = cursor.fetchall()
    
    eligible_managers = []
    
    # Parcours des résultats et conversion en dictionnaire
    for row in rows:
        manager = {
            'id': row[0],
            'name': row[1],
            'age': row[2],
            'country': row[3],
            'email': row[4],
            'seniority': row[5],
            'investment_sector': row[6],
            #'strategies': row[7],
            #'clients_id': row[8],  
            #'portfolios_id': row[9]
        }
        eligible_managers.append(manager)
    
    return eligible_managers




class AssetManager:
    """Représente un asset manager du fond."""
    def __init__(self, name, age, country, email, seniority, investment_sector, strategies, clients_id, portfolios_id):
        self.name = name
        self.age = age
        self.country = country
        self.email = email
        self.seniority = seniority
        self.investment_sector = investment_sector
        self.strategies = strategies  # Liste des stratégies
        self.clients_id = clients_id  # Liste des clients
        self.portfolios_id = portfolios_id  # Liste des portefeuilles


    def save(self, db):
        """Ajoute l'asset manager dans la base de données et gère les relations."""
        cursor = db.cursor()

        # 🔹 Insérer le manager
        cursor.execute("""
            INSERT INTO Managers (name, age, country, email, seniority) 
            VALUES (?, ?, ?, ?, ?)
        """, (self.name, self.age, self.country, self.email, self.seniority))

        manager_id = cursor.lastrowid  # ✅ Récupérer l'ID

        # 🔹 Associer les stratégies
        for strategy in self.strategies:
            cursor.execute("INSERT INTO Manager_Strategies (manager_id, strategy) VALUES (?, ?)", (manager_id, strategy))

        # 🔹 Associer les clients
        for client_id in self.clients_id:
            cursor.execute("INSERT INTO Manager_Clients (manager_id, client_id) VALUES (?, ?)", (manager_id, client_id))

        #🔹 Associer les portefeuilles
        for portfolio_id in self.portfolios_id:
            cursor.execute("INSERT INTO Manager_Portfolios (manager_id, portfolio_id) VALUES (?, ?)", (manager_id, portfolio_id))

        db.commit()
        return manager_id




## Portfolio

class Portfolio:
    """Représente un portefeuille."""
    def __init__(self, id, manager_id, client_id, strategy, investment_sector, size, value, assets):
        self.id = id
        self.manager_id = manager_id
        self.client_id=client_id
        self.strategy = strategy
        investment_sector = investment_sector
        size = size
        self.value = value
        self.assets = assets  # Liste des produits
        
    def save(self, db):
        """Ajoute le portefeuille dans la base de données et gère les relations."""
        cursor = db.cursor()

        # 🔹 Insérer le portefeuille principal
        cursor.execute("""
            INSERT INTO Portfolios (manager_id, client_id, strategy, investment_sector, size, value) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, ( self.manager_id, self.client_id, self.strategy, self.investment_sector, self.size, self.value))
        
        # ✅ Récupérer l'ID auto-incrémenté
        portfolio_id = cursor.lastrowid

        # 🔹 Associer les produits
        for asset in self.assets:
            #vérifier si le produit existe déjà dans Products
            cursor.execute("SELECT id FROM Products WHERE ticker = ?", (asset))
            #créer une colonne pour chaque product, on associe des poids pour chaque portefeuille
            cursor.execute("""
                INSERT INTO Portfolio_Products (id, product_id)
                VALUES (?, ?)
            """, (portfolio_id, product_id))

        db.commit()
        return portfolio_id 









class Product:
    """Représente un produit financier."""
    def __init__(self, product_id, ticker, category, stock_exchange):
        self.product_id = product_id
        self.ticker = ticker
        self.category = category
        self.stock_exchange = stock_exchange

    def save(self, db):
        """Ajoute le produit dans la base de données."""
        db.execute("""
        INSERT INTO Products ( ticker, category, stock_exchange) 
        VALUES (?, ?, ?)""", ( self.ticker, self.category, self.stock_exchange))







class Returns:
    """Représente un produit financier."""
    def __init__(self, product_id, ticker, category, stock_exchange):
        self.product_id = product_id
        self.ticker = ticker
        self.category = category
        self.stock_exchange = stock_exchange
        
    def add_product_column(self, ticker):
        """Ajoute une colonne pour un nouveau produit (ticker) si elle n'existe pas encore."""
        self.cursor.execute(f"PRAGMA table_info(Returns)")
        columns = [col[1] for col in self.cursor.fetchall()]

        if ticker not in columns:
            self.cursor.execute(f"ALTER TABLE Returns ADD COLUMN {ticker} REAL")
            self.conn.commit()

    def insert_return(self, date, returns_dict):
        """
        Insère ou met à jour les returns pour une date donnée.
        returns_dict : dict {ticker: return_value}
        """
        # Vérifie que toutes les colonnes existent
        for ticker in returns_dict.keys():
            self.add_product_column(ticker)

        # Vérifie si la date existe déjà
        self.cursor.execute("SELECT * FROM Returns WHERE date = ?", (date,))
        existing = self.cursor.fetchone()

        if existing:
            # Mise à jour des valeurs
            update_query = "UPDATE Returns SET " + ", ".join([f"{ticker} = ?" for ticker in returns_dict.keys()]) + " WHERE date = ?"
            values = list(returns_dict.values()) + [date]
            self.cursor.execute(update_query, values)
        else:
            # Insertion d'une nouvelle ligne
            columns = ", ".join(["date"] + list(returns_dict.keys()))
            placeholders = ", ".join(["?"] * (len(returns_dict) + 1))
            insert_query = f"INSERT INTO Returns ({columns}) VALUES ({placeholders})"
            values = [date] + list(returns_dict.values())
            self.cursor.execute(insert_query, values)

        self.conn.commit()




class Deal:
    """Représente un deal (transaction) entre actifs dans un portefeuille."""

    def __init__(self, deal_id, portfolio_id, date, asset_sold, quantity_sold, price_sold, asset_bought, quantity_bought, price_bought):
        self.deal_id = deal_id
        self.portfolio_id = portfolio_id
        self.date = date
        self.asset_sold = asset_sold
        self.quantity_sold = quantity_sold
        self.price_sold = price_sold
        self.asset_bought = asset_bought
        self.quantity_bought = quantity_bought
        self.price_bought = price_bought

    def save(self, db):
        """Enregistre la transaction en base de données."""
        db.execute("""
            INSERT INTO Deals (portfolio_id, date, asset_sold, quantity_sold, price_sold, asset_bought, quantity_bought, price_bought)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)""",
            ( self.portfolio_id, self.date, self.asset_sold, self.quantity_sold, self.price_sold, 
             self.asset_bought, self.quantity_bought, self.price_bought))
