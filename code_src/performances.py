import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from strategies import Simulation
from base_builder import BaseModel
import sqlite3

def analyze_portfolio_performance(portfolio_df, benchmark_df=None):
    """
    Analyse la performance d'un portefeuille à partir d'un DataFrame contenant les valeurs hebdomadaires.
    
    Args:
        portfolio_df: DataFrame contenant les valeurs du portefeuille avec la colonne 'portfolio_value'
        benchmark_df: (facultatif) DataFrame contenant les valeurs de l'indice de référence, avec la colonne 'benchmark_value'
    """
    portfolio_df.sort_values('date', inplace=True)
    
    # Calcul des rendements hebdomadaires
    portfolio_df['return'] = portfolio_df['portfolio_value'].pct_change()
    
    # Calcul des statistiques de performance
    sharpe_ratio = portfolio_df['return'].mean() / portfolio_df['return'].std() * (52 ** 0.5)  # Annualisé
    volatility = portfolio_df['return'].std() * (52 ** 0.5)  # Annualisée
    cumulative_return = (portfolio_df['portfolio_value'].iloc[-1] / portfolio_df['portfolio_value'].iloc[0]) - 1
    
    print("\n=== Analyse des performances ===")
    print(f"Ratio de Sharpe: {sharpe_ratio:.2f}")
    print(f"Volatilité annualisée: {volatility:.2%}")
    print(f"Rendement cumulé: {cumulative_return:.2%}")
    
    # Si un benchmark est fourni, calcul des autres statistiques
    if benchmark_df is not None:
        benchmark_df['date'] = pd.to_datetime(benchmark_df['date'])
        benchmark_df.sort_values('date', inplace=True)
        benchmark_df['return'] = benchmark_df['benchmark_value'].pct_change()

        # Alpha et Beta par rapport au benchmark
        covariance = np.cov(portfolio_df['return'].dropna(), benchmark_df['return'].dropna())[0, 1]
        benchmark_volatility = benchmark_df['return'].std()
        beta = covariance / benchmark_volatility**2
        alpha = portfolio_df['return'].mean() - beta * benchmark_df['return'].mean()
        
        print(f"Alpha: {alpha:.2%}")
        print(f"Beta: {beta:.2f}")
    
    # Maximum Drawdown
    running_max = portfolio_df['portfolio_value'].cummax()
    drawdown = (portfolio_df['portfolio_value'] - running_max) / running_max
    max_drawdown = drawdown.min()
    print(f"Maximum Drawdown: {max_drawdown:.2%}")
    
    # Sortino Ratio
    downside_returns = portfolio_df['return'][portfolio_df['return'] < 0]
    sortino_ratio = portfolio_df['return'].mean() / downside_returns.std() * (52 ** 0.5)
    print(f"Sortino Ratio: {sortino_ratio:.2f}")
    
    # Tracking Error
    if benchmark_df is not None:
        tracking_error = np.std(portfolio_df['return'] - benchmark_df['return'])
        print(f"Tracking Error: {tracking_error:.2%}")
    
    # Tracé de la valeur du portefeuille
    plt.figure(figsize=(10, 5))
    plt.plot(portfolio_df.index, portfolio_df['portfolio_value'], label='Valeur du portefeuille', color='b')
    plt.xlabel('Date')
    plt.ylabel('Valeur du portefeuille (€)')
    plt.title('Évolution de la valeur du portefeuille')
    plt.xlim(portfolio_df.index.min(), portfolio_df.index.max()) # Définir les limites de l'axe des x en fonction des dates présentes dans le DataFrame
    plt.legend()
    plt.grid()
    plt.show()

    # Calculer la moyenne des valeurs des produits (tickers) sur toute la période du portefeuille
    average_data = portfolio_df.drop(['cash', 'portfolio_value'], axis=1).mean()

    # Clip pour s'assurer qu'aucune valeur négative n'est présente
    average_data = average_data.clip(lower=0)

    # Calculer la répartition en pourcentage pour chaque produit
    total_value = portfolio_df['portfolio_value'].mean()  # Total moyen du portefeuille sur la période
    product_percentage = average_data / total_value * 100

    # Affichage du diagramme en secteurs pour la répartition moyenne
    plt.figure(figsize=(8, 8))
    plt.pie(product_percentage, labels=product_percentage.index, autopct='%1.1f%%', startangle=90, colors=plt.cm.Paired.colors)
    plt.title("Répartition moyenne du portefeuille")
    plt.axis('equal')  # Assurer un cercle parfait
    plt.show()
    

def get_portfolio_performance_df(portfolio_id: int, strategy: str, start_date: str, end_date: datetime = None) -> pd.DataFrame:
    """
    Génère un DataFrame contenant l'historique des positions et valeurs du portefeuille.
    
    Args:
        portfolio_id (int): ID du portefeuille à analyser
        strategy (str): Stratégie utilisée pour le portefeuille
        start_date (str): Date de début de l'analyse (format: 'YYYY-MM-DD')
        end_date (datetime, optional): Date de fin de l'analyse. Par défaut: 31/12/2024
    
    Returns:
        pd.DataFrame: DataFrame contenant l'historique des positions avec:
            - Index: dates
            - Colonnes: cash, portfolio_value, et une colonne par produit (ticker)
    """
    if end_date is None:
        end_date = datetime(2024, 12, 31)
    
    # Créer une instance de Simulation
    db = BaseModel.get_db_connection()
    simulation = Simulation(db, portfolio_id, strategy, start_date)
    
    # DataFrame pour stocker les performances du portefeuille
    portfolio_performance_df = pd.DataFrame(columns=["date", "cash", "portfolio_value"])
    
    # Variable pour stocker les tickers (produits uniques) rencontrés
    all_tickers = set()

    # Simuler la gestion active du portefeuille
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    
    while current_date <= end_date:
        # Trouver le prochain lundi
        while current_date.weekday() != 0:  # 0 = lundi
            current_date += timedelta(days=1)
        
        # Exécuter la stratégie pour ce lundi
        positions, cash = simulation.execute_strategy(current_date)
        
        # Ajouter les tickers rencontrés à la liste
        for position in positions:
            all_tickers.add(position['ticker'])
        
        # Créer une ligne pour stocker la valeur de chaque produit et la valeur totale du portefeuille
        row = {
            'date': current_date,
            'cash': cash['value'],
            'portfolio_value': sum(p['value'] for p in positions) + cash['value']
        }
        
        # Ajouter les valeurs des produits (tickers) dynamiquement dans le DataFrame
        for ticker in all_tickers:
            ticker_value = sum(p['value'] for p in positions if p['ticker'] == ticker)
            row[ticker] = ticker_value
        
        # Ajouter la ligne au DataFrame
        new_row = pd.DataFrame([row])
        portfolio_performance_df = pd.concat([portfolio_performance_df, new_row], ignore_index=True)
        
        # Passer à la semaine suivante
        current_date += timedelta(days=7)
    
    # Définir la date comme index
    portfolio_performance_df.set_index('date', inplace=True)
    
    return portfolio_performance_df


def get_portfolio_rankings(db: sqlite3.Connection, start_date: str, end_date: datetime = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcule les classements des portefeuilles et des managers par performance.
    
    Args:
        db: Connexion à la base de données
        start_date: Date de début de l'analyse
        end_date: Date de fin de l'analyse (optionnel)
    
    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: 
            - DataFrame des classements des portefeuilles
            - DataFrame des classements des managers
    """
    cursor = db.cursor()
    
    # Récupérer tous les portefeuilles avec leurs informations
    cursor.execute("""
        SELECT p.id, p.strategy, c.name as client_name, m.name as manager_name,
               c.investment_amount as initial_value
        FROM Portfolios p
        JOIN Clients c ON p.client_id = c.id
        JOIN Managers m ON p.manager_id = m.id
    """)
    portfolios = cursor.fetchall()
    
    # Créer les DataFrames pour stocker les performances
    portfolio_performances = []
    manager_performances = {}
    
    # Calculer la performance pour chaque portefeuille
    for portfolio in portfolios:
        portfolio_id, strategy, client_name, manager_name, initial_value = portfolio
        
        try:
            # Obtenir le DataFrame des performances
            performance_df = get_portfolio_performance_df(
                portfolio_id=portfolio_id,
                strategy=strategy,
                start_date=start_date,
                end_date=end_date
            )
            
            # Calculer la performance
            final_value = performance_df['portfolio_value'].iloc[-1]
            performance = (final_value - initial_value) / initial_value * 100
            
            # Ajouter au classement des portefeuilles
            portfolio_performances.append({
                'Portfolio ID': portfolio_id,
                'Client': client_name,
                'Manager': manager_name,
                'Strategy': strategy,
                'Initial Value': initial_value,
                'Final Value': final_value,
                'Performance (%)': performance
            })
            
            # Ajouter au classement des managers
            if manager_name not in manager_performances:
                manager_performances[manager_name] = {
                    'Manager': manager_name,
                    'Number of Portfolios': 0,
                    'Average Performance (%)': 0,
                    'Total AUM': 0
                }
            
            manager_performances[manager_name]['Number of Portfolios'] += 1
            manager_performances[manager_name]['Average Performance (%)'] += performance
            manager_performances[manager_name]['Total AUM'] += final_value
            
        except Exception as e:
            print(f"⚠️ Erreur lors du calcul de la performance du portefeuille {portfolio_id}: {str(e)}")
            continue
    
    # Créer le DataFrame des classements des portefeuilles
    portfolio_rankings = pd.DataFrame(portfolio_performances)
    portfolio_rankings = portfolio_rankings.sort_values('Performance (%)', ascending=False)
    
    # Calculer les moyennes pour les managers
    for manager in manager_performances.values():
        manager['Average Performance (%)'] /= manager['Number of Portfolios']
    
    # Créer le DataFrame des classements des managers
    manager_rankings = pd.DataFrame(list(manager_performances.values()))
    manager_rankings = manager_rankings.sort_values('Average Performance (%)', ascending=False)
    
    return portfolio_rankings, manager_rankings
    
