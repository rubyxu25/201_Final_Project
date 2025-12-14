"""
Visualization & Reporting Module
Person in Charge: Ruby Xu
Tasks: 
- Creating all charts using Matplotlib 
- Producing a visualization dashboard 
- Writing the analytical conclusions and project summary
"""

import matplotlib.pyplot as plt
import csv
import json
import math
from datetime import datetime, date
from database import connect_database, get_date_range, get_exchange_rate, get_stock_data
from metrics_calculation import calculate_exchange_rate_changes, calculate_stock_price_changes
from sqlite3 import Cursor, Connection

def plot_exchange_rate_trends(base_currency: str, target_currency: str, cur: Cursor, conn: Connection) -> None:

    data = get_exchange_rate(base_currency, target_currency, cur, conn) 
    dates = [d[0] for d in data]
    rates = [d[3] for d in data]
    plt.plot(dates, rates)
    plt.title(f"{base_currency}_{target_currency} Exchange Rate Trends")
    plt.xlabel("Date")
    plt.ylabel(f"{base_currency} to {target_currency} Exchange Rate")
    plt.savefig(f"{base_currency}_{target_currency}_exchange_rate_trends.png")
    plt.close()
    print(f"{base_currency}_{target_currency}_exchange_rate_trends.png saved")

def plot_stock_price_trends(stock_symbol: str, cur: Cursor, conn: Connection) -> None:
    """
    Stock Closing Price Trend - Line chart showing the daily closing prices 
    of stocks such as AAPL, MSFT
    
    Args:
        stock_symbol: Stock symbol (e.g., 'AAPL')
        cur: Database cursor object
        conn: Database connection object
    """
    data = get_stock_data(stock_symbol, cur, conn)
    dates = [d[0] for d in data]
    prices = [d[4] for d in data]
    plt.plot(dates, prices)
    plt.title(f"{stock_symbol} Stock Price Trends")
    plt.xlabel("Date")
    plt.ylabel(f"{stock_symbol} Stock Price")
    plt.savefig(f"{stock_symbol}_stock_price_trends.png")
    plt.close()
    print(f"{stock_symbol}_stock_price_trends.png saved")

def plot_exchange_rate_vs_stock_comparison(base_currency: str, target_currency: str, stock_symbol: str, cur: Cursor, conn: Connection) -> None:
    """
    Exchange Rate vs Stock Price Comparison - Chart comparing the trend 
    of exchange rates and a specific stock price over the same period
    
    Args:
        base_currency: Base currency code (e.g., 'USD') 
        target_currency: Target currency code (e.g., 'EUR') 
        stock_symbol: Stock symbol (e.g., 'AAPL')
        cur: Database cursor object
        conn: Database connection object    
    """
    exchange_data = calculate_exchange_rate_changes(base_currency, target_currency, cur, conn)
    stock_data = calculate_stock_price_changes(stock_symbol, cur, conn)
    dates = [d[0] for d in exchange_data]
    exchange_rates = [d[1] for d in exchange_data]
    stock_prices = [d[1] for d in stock_data]
    plt.plot(dates, exchange_rates, label=f"{base_currency} to {target_currency} Exchange Rate")
    plt.plot(dates, stock_prices, label=f"{stock_symbol} Stock Price")
    plt.title(f"Comparison of Exchange Rate vs Stock Price")
    plt.xlabel(f"Date")
    plt.ylabel(f"Change Percent")
    plt.legend()
    plt.savefig(f"{base_currency}_{target_currency}_{stock_symbol}_exchange_rate_vs_stock_comparison.png")
    plt.close()
    print(f"{base_currency}_{target_currency}_{stock_symbol}_exchange_rate_vs_stock_comparison.png saved")

def plot_exchange_rate_vs_stock_scatter(base_currency: str, target_currency: str, stock_symbol: str, cur: Cursor, conn: Connection) -> None:
    """
    Exchange Rate vs Stock Price Scatter Plot - Scatter plot to analyze the 
    statistical relationship between exchange rates and stock prices
    
    Args:
        base_currency: Base currency code (e.g., 'USD') 
        target_currency: Target currency code (e.g., 'EUR') 
        stock_symbol: Stock symbol (e.g., 'AAPL')
        cur: Database cursor object
        conn: Database connection object
    """
    exchange_data = calculate_exchange_rate_changes(base_currency, target_currency, cur, conn)
    stock_data = calculate_stock_price_changes(stock_symbol, cur, conn)
    exchange_rates = [d[1] for d in exchange_data]
    stock_prices = [d[1] for d in stock_data]
    plt.scatter(exchange_rates, stock_prices)
    plt.title(f"Correlation Between Exchange Rate vs Stock Price")
    plt.xlabel(f"Exchange Rate Change ({base_currency} to {target_currency})")
    plt.ylabel(f"Stock Price Change ({stock_symbol})")       
    plt.savefig(f"{base_currency}_{target_currency}_{stock_symbol}_exchange_rate_vs_stock_scatter.png")
    plt.close()
    print(f"{base_currency}_{target_currency}_{stock_symbol}_exchange_rate_vs_stock_scatter.png saved")

def main_visualization():
    """
    Main function to orchestrate the entire visualization and reporting process
    """
    cur, conn = connect_database("data.db")

    plot_exchange_rate_trends('USD', 'CNY', cur, conn)
    plot_exchange_rate_trends('USD', 'GBP', cur, conn)

    plot_stock_price_trends('AAPL', cur, conn)
    plot_stock_price_trends('NVDA', cur, conn)

    plot_exchange_rate_vs_stock_comparison('USD', 'GBP', 'AAPL', cur, conn)
    plot_exchange_rate_vs_stock_comparison('USD', 'CNY', 'NVDA', cur, conn)

    plot_exchange_rate_vs_stock_scatter('USD', 'GBP', 'AAPL', cur, conn)
    plot_exchange_rate_vs_stock_scatter('USD', 'CNY', 'NVDA', cur, conn)

    conn.close()

if __name__ == "__main__":
    main_visualization()
