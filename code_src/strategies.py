import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from enum import Enum

from base_builder import (
    BaseModel, Portfolio, Product, PortfolioProduct, Deal,
    get_db_path
)

class RiskProfile(Enum):
    LOW_RISK = "Low Risk"
    MID_RISK = "Medium Risk"
    HIGH_RISK = "High Risk"

class StrategyManager:
    def __init__(self):
        self.db = BaseModel.get_db_connection()
        self.cursor = self.db.cursor()
        
    def create_deals_tables(self) -> None:
        """Crée les tables nécessaires pour stocker les deals."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Deals_portfolio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER,
                date TEXT,
                ticker TEXT,
                action TEXT,
                quantity INTEGER,
                price REAL,
                total_value REAL,
                portfolio_weight REAL,
                manager_weight REAL,
                fund_weight REAL,
                FOREIGN KEY (portfolio_id) REFERENCES Portfolios(id)
            )
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Deals_manager (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER,
                date TEXT,
                ticker TEXT,
                action TEXT,
                quantity INTEGER,
                price REAL,
                total_value REAL,
                portfolio_weight REAL,
                manager_weight REAL,
                fund_weight REAL,
                FOREIGN KEY (manager_id) REFERENCES AssetManagers(id)
            )
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Deals_fund (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                ticker TEXT,
                action TEXT,
                quantity INTEGER,
                price REAL,
                total_value REAL,
                portfolio_weight REAL,
                manager_weight REAL,
                fund_weight REAL
            )
        """)
        
        self.db.commit()
    
    def get_portfolio_data(self, portfolio_id: int) -> Dict:
        """Récupère les données d'un portefeuille."""
        portfolio = Portfolio.get_by_id(portfolio_id, self.db)
        if not portfolio:
            raise ValueError(f"Portfolio {portfolio_id} not found")
        
        return {
            'id': portfolio.id,
            'manager_id': portfolio.manager_id,
            'client_id': portfolio.client_id,
            'strategy': portfolio.strategy,
            'investment_sector': portfolio.investment_sector,
            'size': portfolio.size,
            'value': portfolio.value
        }
    
    def get_portfolio_holdings(self, portfolio_id: int) -> List[Dict]:
        """Récupère les positions actuelles d'un portefeuille."""
        portfolio_products = PortfolioProduct.get_by_portfolio_id(portfolio_id, self.db)
        holdings = []
        
        for pp in portfolio_products:
            product = Product.get_by_id(pp.product_id, self.db)
            if product:
                holdings.append({
                    'ticker': product.ticker,
                    'quantity': pp.quantity,
                    'weight': pp.weight
                })
        
        return holdings
    
    def get_weekly_returns(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Récupère les rendements hebdomadaires d'un actif."""
        self.cursor.execute("""
            SELECT date, returns_CVX as returns
            FROM Returns
            WHERE date BETWEEN ? AND ?
            ORDER BY date
        """, (start_date, end_date))
        return pd.DataFrame(self.cursor.fetchall(), columns=['date', 'returns'])
    
    def calculate_portfolio_volatility(self, portfolio_id: int, start_date: str, end_date: str) -> float:
        """Calcule la volatilité annualisée d'un portefeuille."""
        holdings = self.get_portfolio_holdings(portfolio_id)
        returns_data = []
        
        for holding in holdings:
            returns = self.get_weekly_returns(holding['ticker'], start_date, end_date)
            returns_data.append(returns['returns'] * holding['weight'])
        
        portfolio_returns = pd.concat(returns_data, axis=1).sum(axis=1)
        weekly_vol = portfolio_returns.std()
        return weekly_vol * np.sqrt(52)  # Annualisation
    
    def execute_strategy(self, portfolio_id: int, date: str) -> List[Deal]:
        """Exécute la stratégie appropriée pour un portefeuille."""
        portfolio_data = self.get_portfolio_data(portfolio_id)
        risk_profile = RiskProfile(portfolio_data['strategy'])
        
        if risk_profile == RiskProfile.LOW_RISK:
            return self._execute_low_risk_strategy(portfolio_id, date)
        elif risk_profile == RiskProfile.MID_RISK:
            return self._execute_mid_risk_strategy(portfolio_id, date)
        else:  # HIGH_RISK
            return self._execute_high_risk_strategy(portfolio_id, date)
    
    def _execute_low_risk_strategy(self, portfolio_id: int, date: str) -> List[Deal]:
        """Exécute la stratégie Low Risk (volatilité cible de 10%)."""
        deals = []
        holdings = self.get_portfolio_holdings(portfolio_id)
        current_vol = self.calculate_portfolio_volatility(portfolio_id, 
                                                        (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d'),
                                                        date)
        
        if current_vol > 0.10:  # Si la volatilité est supérieure à 10%
            # Réduire les positions les plus volatiles
            for holding in holdings:
                if holding['weight'] > 0.1:  # Si l'actif représente plus de 10% du portefeuille
                    deals.append(self._create_deal(portfolio_id, holding['ticker'], "SELL", 
                                                 int(holding['quantity'] * 0.2)))  # Vendre 20% de la position
        
        return deals
    
    def _execute_mid_risk_strategy(self, portfolio_id: int, date: str) -> List[Deal]:
        """Exécute la stratégie Mid Risk (maximum 2 deals par mois)."""
        # Vérifier le nombre de deals du mois
        self.cursor.execute("""
            SELECT COUNT(*) FROM Deals_portfolio
            WHERE portfolio_id = ? AND date LIKE ?
        """, (portfolio_id, f"{date[:7]}%"))
        monthly_deals = self.cursor.fetchone()[0]
        
        if monthly_deals >= 2:
            return []  # Ne pas faire de deals si on a déjà atteint la limite
        
        # Logique de trading (à implémenter selon vos critères)
        return []
    
    def _execute_high_risk_strategy(self, portfolio_id: int, date: str) -> List[Deal]:
        """Exécute la stratégie High Risk (maximisation du rendement)."""
        # Logique de trading agressive (à implémenter selon vos critères)
        return []
    
    def _create_deal(self, portfolio_id: int, ticker: str, action: str, quantity: int) -> Deal:
        """Crée un objet Deal avec les informations nécessaires."""
        # Récupérer le prix actuel
        self.cursor.execute("""
            SELECT returns FROM Returns
            WHERE ticker = ? AND date = (SELECT MAX(date) FROM Returns)
        """, (ticker,))
        price = self.cursor.fetchone()[0]
        
        # Calculer les poids
        portfolio_data = self.get_portfolio_data(portfolio_id)
        total_value = quantity * price
        
        return Deal(
            portfolio_id=portfolio_id,
            manager_id=portfolio_data['manager_id'],
            date=datetime.now().strftime('%Y-%m-%d'),
            ticker=ticker,
            action=action,
            quantity=quantity,
            price=price,
            total_value=total_value,
            portfolio_weight=0.0,  # À calculer
            manager_weight=0.0,    # À calculer
            fund_weight=0.0        # À calculer
        )
    
    def save_deal(self, deal: Deal) -> None:
        """Sauvegarde un deal dans les différentes tables."""
        deal.save(self.db)
    
    def close(self) -> None:
        """Ferme la connexion à la base de données."""
        self.db.close() 