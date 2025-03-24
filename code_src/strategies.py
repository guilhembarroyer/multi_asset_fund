from typing import Dict, List, Tuple, Any
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from base_builder import BaseModel, Deal
from scipy.optimize import minimize
from base_builder import Portfolio


class Simulation:
    """Classe pour simuler la gestion active d'un portefeuille."""
    
    def __init__(self, db: sqlite3.Connection, portfolio_id: int, strategy: str, registration_date: str):
        """
        Initialise la simulation.
        
        Args:
            db: Connexion à la base de données
            portfolio_id: ID du portefeuille à simuler
            strategy: Stratégie d'investissement à utiliser
            registration_date: Date d'enregistrement du client
        """
        self.db = db
        self.cursor = db.cursor()
        self.portfolio_id = portfolio_id
        self.strategy = strategy
        self.registration_date = datetime.strptime(registration_date, '%Y-%m-%d')
        
        # Récupérer les informations du portefeuille
        self.cursor.execute("""
            SELECT p.value, p.size
            FROM Portfolios p
            WHERE p.id = ?
        """, (portfolio_id,))
        portfolio_info = self.cursor.fetchone()
        if portfolio_info:
            self.portfolio_value = portfolio_info[0]
            self.portfolio_size = portfolio_info[1]
        else:
            raise ValueError(f"Portefeuille {portfolio_id} non trouvé")
        
        self.cursor.execute("""
            UPDATE Portfolios_Products
            SET quantity = ?,
                weight = ?,
                value = ?
            WHERE portfolio_id = ?
        """, (0,0, 0, portfolio_id))
    
        
        

        # Compteur de deals par mois
        self.deals_count = 0
        self.current_month = None
    
    def execute_strategy(self, current_date: datetime) -> List[Dict[str, Any]]:
        """
        Exécute la stratégie d'investissement pour une date donnée.
        
        Args:
            current_date: Date d'analyse (vendredi)
            
        Returns:
            List[Dict[str, Any]]: Liste des deals à exécuter
        """
        # Mettre à jour l'historique des rendements
        current_returns = self.get_asset_returns(current_date)

        # Mettre à jour le compteur de deals mensuel
        current_month = current_date.month
        if self.current_month != current_month:
            self.deals_count = 0
            self.current_month = current_month
            print("new month", current_month)

        # Récupérer les positions actuelles
        positions, cash = self.get_portfolio_positions(self.portfolio_id, current_date)

        print("old positions", positions)
        print("cash", cash)
        print("current_date", current_date)
        # Calculer les décisions d'investissement selon la stratégie
        deals,cash,positions = self._calculate_deals(positions, cash, current_returns)
        print("deals", deals)
        print("new positions", positions)
        print("new cash", cash)
        # Enregistrer les deals dans la base de données
        if deals:
            self._save_deals_positions(deals, positions, cash, current_date)
            

        print("deals count", self.deals_count)
        return deals
    
    def get_asset_returns(self, date: datetime) -> pd.DataFrame:
        """Get returns for each asset as a DataFrame with the last 12 returns"""
        # Récupérer tous les tickers du portefeuille
        self.cursor.execute("""
            SELECT DISTINCT p.ticker
            FROM Portfolios_Products pp
            JOIN Products p ON pp.product_id = p.id
            WHERE pp.portfolio_id = ?
        """, (self.portfolio_id,))
        
        tickers = [row[0] for row in self.cursor.fetchall()]
        
        # Créer un dictionnaire pour stocker les rendements par ticker
        returns_dict = {}
        
        # Récupérer les rendements pour chaque ticker
        for ticker in tickers:
            self.cursor.execute(f"""
                SELECT date, returns
                FROM Returns_{ticker}
                WHERE date <= ?
                ORDER BY date DESC
                LIMIT 12
            """, (date.strftime("%Y-%m-%d"),))
            
            results = self.cursor.fetchall()
            if results:
                # Créer une liste de rendements dans l'ordre chronologique
                returns_list = [row[1] for row in reversed(results)]
                returns_dict[ticker] = returns_list
        
        # Créer la DataFrame
        returns_df = pd.DataFrame(returns_dict)
        
        return returns_df
    
    def get_portfolio_positions(self, portfolio_id, current_date):
        """
        Récupère les positions actuelles du portefeuille depuis la table Portfolios_Products.
        """
        cursor = self.db.cursor()
        
        # Récupérer les positions depuis Portfolios_Products
        cursor.execute("""
            SELECT p.ticker, pp.quantity, pp.weight, p.id as product_id
            FROM Portfolios_Products pp
            JOIN Products p ON pp.product_id = p.id
            WHERE pp.portfolio_id = ?
        """, (portfolio_id,))
        
        positions = []
        total_value = 0

        for row in cursor.fetchall():

            ticker, quantity, weight, product_id = row
            
            # Récupérer le dernier prix connu
            cursor.execute(f"""
                SELECT price
                FROM Returns_{ticker}
                WHERE date <= ?
                ORDER BY date DESC
                LIMIT 1
            """, (current_date.strftime('%Y-%m-%d'),))
            
            price = cursor.fetchone()
            if price is None:
                continue
            
            position_value = quantity * price[0]
            total_value += position_value
            

            positions.append({
                'ticker': ticker,
                'quantity': quantity,
                'weight': weight,
                'price': price[0],
                'value': position_value,
                'product_id': product_id
            })

        cursor.execute("""
                SELECT cash_value
                FROM Portfolios
                WHERE id = ?
            """, (self.portfolio_id,))
        cash_value = cursor.fetchone()[0]
            
        print("cash value in get_portfolio_positions", cash_value)
        
        cash = {
            'ticker': 'CASH',
            'weight': cash_value/self.portfolio_value,
            'price': 1,
            'value': cash_value} 
        
        print("positions in get_portfolio_positions", positions)
        print("total value in get_portfolio_positions", total_value+cash_value)

        return positions, cash


    
    def _calculate_deals(self, positions: List[Dict[str, Any]], cash: Dict[str, Any], current_returns: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Calcule les deals à effectuer selon la stratégie.
        
        Args:
            positions: Positions actuelles du portefeuille
            current_date: Date d'analyse
            
        Returns:
            List[Dict[str, Any]]: Liste des deals à effectuer
        """
        deals = []

        if self.strategy == "Low Risk":

            # Calculer la volatilité actuelle du portefeuille
            portfolio_returns = pd.Series(0.0, index=current_returns.index)
            for position in positions:
                if position['ticker'] in current_returns.columns:
                    portfolio_returns += current_returns[position['ticker']] * position['weight']
            
            current_volatility = portfolio_returns.std() * np.sqrt(252)  # Volatilité annualisée
            
            print("current volatility", current_volatility)
            
            # Si la volatilité est supérieure à 10%, réduire les positions risquées
            if current_volatility > 0.10:
                print("current volatility is greater than 10%")
                # Trier les actifs par volatilité décroissante
                asset_volatilities = current_returns.std() * np.sqrt(252)
                risky_assets = asset_volatilities[asset_volatilities > 0.10].index
                
                # Réduire les positions des actifs les plus risqués
                for position in positions:
                    if position['ticker'] in risky_assets:
                        # Calculer la réduction nécessaire
                        target_weight = round(position['weight'] * (0.10 / current_volatility), 2)
                        weight_diff = target_weight - position['weight']
                        print(position['ticker'], position['weight'], target_weight)
                        
                    quantity = int(weight_diff * self.portfolio_value / position['price'])
                    if quantity < 0:
                        deals.append({
                            'product_id': position['product_id'],
                            'action': 'SELL',
                            'quantity': quantity,
                            'price': position['price']
                        })
                        position['quantity'] += quantity
                        position['weight'] += quantity * position['price']/self.portfolio_value
                        position['value'] += quantity * position['price']
                        cash['value']-= quantity * position['price']
                        cash['weight']= cash['value']/self.portfolio_value

            
            # Si la volatilité est inférieure à 10%, augmenter les positions des actifs moins risqués
            elif current_volatility < 0.10:
                print("current volatility is less than 10%")
                # Obtenir les poids optimaux
                target_weights = self.optimize(current_returns)
                
                # Augmenter les positions des actifs les moins risqués
                for position in positions:

                    current_weight = position['weight']
                    target_weight = round(target_weights.get(position['ticker'], 0), 2)
                    weight_diff = target_weight - current_weight

                    print(position['ticker'], current_weight, target_weight)
                
                    quantity = int((weight_diff) * self.portfolio_value / position['price'])

                    if quantity !=0:
                         
                        action = 'BUY' if weight_diff > 0 else 'SELL'
                        
                        if (action == 'BUY' and quantity * position['price'] <= cash['value']) or (action == 'SELL' and quantity * position['price'] <= position['value']):
                            print("deal", action, quantity, position['price'])
            
                            self.deals_count += 1
                            deals.append({
                            'product_id': position['product_id'],
                            'action': action,
                            'quantity': quantity,
                            'price': position['price']
                            })
                            position['quantity'] += quantity
                            position['weight'] += quantity * position['price']/self.portfolio_value
                            position['value'] += quantity * position['price']
                            cash['value']-= quantity * position['price']
                            cash['weight']= cash['value']/self.portfolio_value
                    
        elif self.strategy == "Medium Risk": #Low Turnover
            # Vérifier le nombre de deals du mois
            print("deals count", self.deals_count)

            if self.deals_count >= 2:  # Maximum 2 deals par mois
                print("no deals")
                return [], cash, positions
            
            else:
          
                # Obtenir les poids optimaux
                target_weights = self.optimize(current_returns)

                # Calculer les ajustements nécessaires
                for position in positions:
                    current_weight = position['weight']
                    target_weight = round(target_weights.get(position['ticker'], 0), 2)
                    print(position['ticker'], current_weight, target_weight)
                    
                    # Calculer la quantité à acheter/vendre
                    weight_diff = target_weight - current_weight
                    quantity = int((weight_diff) * self.portfolio_value / position['price'])
                
                    if quantity !=0 and self.deals_count <2:
                         
                        action = 'BUY' if weight_diff > 0 else 'SELL'
                        
                        if (action == 'BUY' and quantity * position['price'] <= cash['value']) or (action == 'SELL' and quantity * position['price'] <= positions['value']):
                            self.deals_count += 1
                            deals.append({
                            'product_id': position['product_id'],
                            'action': action,
                            'quantity': quantity,
                            'price': position['price']
                            })
                            position['quantity'] += quantity
                            position['weight'] += quantity * position['price']/self.portfolio_value
                            position['value'] += quantity * position['price']
                            cash['value']-= quantity * position['price']
                            cash['weight']= cash['value']/self.portfolio_value
                        
                
    
        elif self.strategy == "High Yield Equity Only":
           pass

        return deals, cash, positions
    

    
    def optimize(self, returns_df: pd.DataFrame, risk_free_rate: float = 0.02, max_weight: float = 0.20) -> Dict[str, float]:
        """
        Optimize portfolio weights to maximize Sharpe ratio
        
        Args:
            returns_df: DataFrame with historical returns (one column per asset)
            risk_free_rate: Annual risk-free rate (default: 2%)
            max_weight: Maximum weight per asset (default: 20%)
            
        Returns:
            Dictionary mapping asset tickers to their optimal weights
        """
        # Calculer la matrice de covariance
        cov_matrix = returns_df.cov()
        
        # Calculer les rendements moyens
        mean_returns = returns_df.mean()
        
        # Fonction objective : maximiser le ratio de Sharpe
        def objective(weights):
            portfolio_return = np.sum(mean_returns * weights) * 252  # Annualisé
            portfolio_volatility = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights))) * np.sqrt(252)
            sharpe_ratio = (portfolio_return - risk_free_rate) / portfolio_volatility
            return -sharpe_ratio  # On minimise le négatif pour maximiser le ratio
        
        # Contraintes
        n_assets = len(returns_df.columns)
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1},  # Somme des poids = 1
            {'type': 'ineq', 'fun': lambda x: x}  # Poids >= 0
        ]
        bounds = tuple((0, max_weight) for _ in range(n_assets))  # Maximum max_weight par actif
        
        # Poids initiaux (égaux)
        initial_weights = np.array([1/n_assets] * n_assets)
        
        # Optimisation
        result = minimize(
            objective,
            initial_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints
        )
        
        # Obtenir les poids optimaux
        optimal_weights = result.x
        
        # Créer un dictionnaire des poids optimaux
        target_weights = dict(zip(returns_df.columns, optimal_weights))
        
        return target_weights 

    
    def _get_product_id(self, ticker: str) -> int:
        """
        Récupère l'ID d'un produit par son ticker.
        
        Args:
            ticker: Symbole du produit
            
        Returns:
            int: ID du produit
        """
        self.cursor.execute("""
            SELECT id FROM Products WHERE ticker = ?
        """, (ticker,))
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def _get_current_price(self, ticker: str, date: datetime) -> float:
        """
        Récupère le prix actuel d'un actif.
        
        Args:
            ticker: Symbole de l'actif
            date: Date d'analyse
            
        Returns:
            float: Prix de l'actif
        """
        self.cursor.execute(f"""
            SELECT price FROM Returns_{ticker}
            WHERE date = ?
        """, (date.strftime("%Y-%m-%d"),))
        result = self.cursor.fetchone()
        return result[0] if result else None

    

    def _save_deals_positions(self, deals: List[Dict[str, Any]], positions: List[Dict[str, Any]], cash: Dict[str, Any], date: datetime) -> None:
        """
        Enregistre les deals dans la base de données et met à jour les positions.
        
        Args:
            deals: Liste des deals à enregistrer
            positions: Liste des positions mises à jour
            date: Date d'exécution des deals
        """
        # Créer et sauvegarder les deals
        deal_objects = []
        for deal in deals:
            deal_obj = Deal(
                portfolio_id=self.portfolio_id,
                product_id=deal['product_id'],
                date=date.strftime("%Y-%m-%d"),
                action=deal['action'],
                quantity=deal['quantity'],
                price=deal['price']
            )
            deal_objects.append(deal_obj)
        
        Deal.save_multiple(deal_objects, self.db)
        
        # Mettre à jour les positions du portefeuille
        self.portfolio_value=Portfolio.update_positions(self.db, self.portfolio_id, positions, cash)
        print("new portfolio value", self.portfolio_value)
        
    

