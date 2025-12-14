"""
Module 3: Metrics Calculation Module
Person in Charge: Ruby Xu
Tasks:
- Calculating daily exchange rate changes and stock price changes
- Performing correlation analysis and simple regression analysis
- Outputting results for visualization and reporting
"""

import sqlite3
import csv
import json
import math
from datetime import datetime, date
from database import connect_database, get_date_range, get_exchange_rate, get_stock_data
from sqlite3 import Cursor, Connection

# Use the same DB as download.py so results align
DB_PATH = "fresh.db"

def calculate_exchange_rate_changes(base_currency: str, target_currency: str, cur:Cursor, conn:Connection) -> list[tuple[date, float]]:
    """
    Calculate daily exchange rate changes from database
    (Today's Rate - Yesterday's Rate) / Yesterday's Rate x 100

    Args:
        base_currency: Base currency code (e.g., 'USD')
        target_currency: Target currency code (e.g., 'EUR') 
        cur: Database cursor object
        conn: Database connection object

    Returns:
        List of daily exchange rate changes (floats)
    """
    data = get_exchange_rate(base_currency, target_currency, cur, conn)
    result = []
    for i in range(len(data) - 1):
        # 3 is the column index of the rate
        result.append((data[i+1][0], (data[i+1][3] - data[i][3]) * 100 / data[i][3]))
    return result

def find_max_exchange_rate_change_date(base_currency: str, target_currency: str, cur:Cursor, conn:Connection) -> tuple[date, float]:
    """
    Find the date with the maximum exchange rate change

    Args:
        base_currency: Base currency code (e.g., 'USD')
        target_currency: Target currency code (e.g., 'EUR') 
        cur: Database cursor object
        conn: Database connection object

    Returns:
        Tuple of the date and the maximum exchange rate change (float)
    """
    data = get_exchange_rate(base_currency, target_currency, cur, conn)
    max_change = 0
    max_change_date = None
    for i in range(len(data) - 1):
        # 3 is the column index of the rate         
        change = (data[i+1][3] - data[i][3]) * 100 / data[i][3]
        if abs(change) > abs(max_change):
            max_change = change
            max_change_date = data[i+1][0]
    return max_change_date, max_change  

def calculate_stock_price_changes(stock_symbol: str, cur:Cursor, conn:Connection) -> list[tuple[date, float]]:
    """
    Calculate daily stock price changes from database
    (Today's Rate - Yesterday's Rate) / Yesterday's Rate x 100

    Args:
        stock_symbol: Stock symbol (e.g., 'AAPL')
        cur: Database cursor object
        conn: Database connection object
        
    Returns:
        List of daily stock price changes (floats)
    """
    data = get_stock_data(stock_symbol, cur, conn)
    result = []
    for i in range(len(data) - 1):
        # 4 is the column index of the close price
        price_index = 4
        result.append((data[i+1][0], (data[i+1][price_index] - data[i][price_index]) * 100 / data[i][price_index]))
    return result

def find_max_stock_change_date(stock_symbol: str, cur:Cursor, conn:Connection) -> tuple[date, float]:
    """
    Find the date with the maximum stock price change

    Args:
        stock_symbol: Stock symbol (e.g., 'AAPL')
        cur: Database cursor object
        conn: Database connection object

    Returns:
        Tuple of the date and the maximum stock price change (float)
    """
    data = get_stock_data(stock_symbol, cur, conn)
    max_change = 0
    max_change_date = None
    for i in range(len(data) - 1):
        # 4 is the column index of the close price         
        change = (data[i+1][4] - data[i][4]) * 100 / data[i][4]
        if abs(change) > abs(max_change):
            max_change = change
            max_change_date = data[i+1][0]
    return max_change_date, max_change      

def perform_correlation_analysis(exchange_data: list[tuple[date, float]], 
                               stock_data: list[tuple[date, float]]) -> float:
    """
    Perform correlation analysis between exchange rates and stock prices
    Reference: https://www.scribbr.com/statistics/pearson-correlation-coefficient/

    Args:
        exchange_data: List of exchange rate changes
        stock_data: List of stock price changes
        
    Returns:
        Correlation coefficient (float)

    """
    
    x_vals = [d[1] for d in exchange_data]
    y_vals = [d[1] for d in stock_data]
    n = len(x_vals)

    # Compute the necessary summation terms
    sum_x = sum(x_vals)
    sum_y = sum(y_vals)
    sum_xy = sum(x * y for x, y in zip(x_vals, y_vals))
    sum_x2 = sum(x ** 2 for x in x_vals)
    sum_y2 = sum(y ** 2 for y in y_vals)

    # Calculate the numerator of the Pearson formula
    numerator = n * sum_xy - sum_x * sum_y

    # Calculate the two parts of the denominator
    denominator_part1 = n * sum_x2 - sum_x ** 2
    denominator_part2 = n * sum_y2 - sum_y ** 2
 
    denominator = (denominator_part1 * denominator_part2) ** 0.5

    # Avoid division by zero when variance is 0
    if denominator == 0:
        return 0.0

    correlation_coefficient = numerator / denominator
    return float(correlation_coefficient)

def main_metrics_calculation():
    """
    Main function to orchestrate the entire metrics calculation process
    """
    metrics_file = open("metrics.txt", "w", encoding="utf-8")

    # connect to the database
    cur, conn = connect_database(DB_PATH)
    
    # print the correlation between exchange rate and stock price to a readable table
    metrics_file.write("Correlation between exchange rate and stock price:\n")
    metrics_file.write("\t\t" + "\t\t".join(["CNY", "EUR", "GBP"]) + "\n")
    for symbol in ["AAPL", "NVDA", "TSLA"]:
        metrics_file.write(f"{symbol}")
        for currency in ["CNY", "EUR", "GBP"]:
            exchange_data = calculate_exchange_rate_changes("USD", currency, cur, conn)
            stock_data = calculate_stock_price_changes(symbol, cur, conn)
            correlation = perform_correlation_analysis(exchange_data, stock_data)
            metrics_file.write(f"\t{round(correlation, 2)}")
        metrics_file.write(f"\n")

    # print the max change date and the change rate for each currency
    metrics_file.write("\nMax change of exchange rates:\n")
    metrics_file.write("\t\t\t" + "\t\t\t".join(["Date", "Change"]) + "\n")
    for currency in ["CNY", "EUR", "GBP"]:
        max_change_date, max_change = find_max_exchange_rate_change_date("USD", currency, cur, conn)
        metrics_file.write(f"{currency} \t\t{max_change_date}\t\t{max_change:+.2f}")
        metrics_file.write("\n")

    # print the max change date and the change rate for each stock symbol
    metrics_file.write("\nMax change of stocks:\n")
    metrics_file.write("\t\t\t" + "\t\t\t".join(["Date", "Change"]) + "\n")
    for symbol in ["AAPL", "NVDA", "TSLA"]:
        max_change_date, max_change = find_max_stock_change_date(symbol, cur, conn)
        metrics_file.write(f"{symbol}\t\t{max_change_date}\t\t{max_change:+.2f}")
        metrics_file.write(f"\n")     

    # close the output file
    metrics_file.close()

    print("Metrics calculation completed and output to metrics.txt")

if __name__ == "__main__":
    main_metrics_calculation()
