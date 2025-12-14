"""
Module 2: Database
Person in Charge: Bojun Zhang
Tasks: 
- Creating the database schema (table structure) 
- Storing the data into SQLite 
- Select data from SQLite
"""

import sqlite3
from sqlite3 import Cursor, Connection
import csv
import json
from datetime import datetime, date
import math
import os

def connect_database(db_name) -> tuple[Cursor, Connection]:
    """
    Connects to a SQLite database connection and cursor.

    Parameters
    -----------------------
    db_name: str
        The name of the SQLite database.

    Returns
    -----------------------
    tuple (Cursor, Connection):
        A tuple containing the database cursor and connection objects.
    """
 
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()

    return cur, conn

def set_up_database(db_name) -> tuple[Cursor, Connection]:
    """
    Sets up a SQLite database connection and cursor.

    Parameters
    -----------------------
    db_name: str
        The name of the SQLite database.

    Returns
    -----------------------
    tuple (Cursor, Connection):
        A tuple containing the database cursor and connection objects.
    """

    cur, conn = connect_database(db_name)

    # register date converter and adapter (convert date to string and vice versa)
    sqlite3.register_adapter(date, lambda d: d.isoformat())
    sqlite3.register_converter("DATE", lambda t: date.fromisoformat(t.decode()))
 
    # set up currency and symbol table
    set_up_currency_symbol_table(["USD","EUR","CNY","GBP"], ["AAPL","TSLA","NVDA"], cur, conn)

    # create exchange_rate and stock table
    create_database_tables(cur, conn)
    return cur, conn

def set_up_currency_symbol_table(currencies: list[str], symbols: list[str], cur: Cursor, conn: Connection) -> None:
    """
    Sets up the tickers table in the database using the provided tickers data.

    Parameters
    -----------------------
    currencies: list
        List of currency(USD,EUR,GBP,JPY) strings.
    symbols: list
        List of ticker(AAPL,TSLA,NVDA) strings.
    cur: Cursor
        The database cursor object.

    conn: Connection
        The database connection object.

    Returns
    -----------------------
    None
    """
    cur.execute(
        "CREATE TABLE IF NOT EXISTS currency (id INTEGER PRIMARY KEY, name TEXT UNIQUE)"
    )
    for i in range(len(currencies)):
        cur.execute(
            "INSERT OR IGNORE INTO currency (id, name) VALUES (?,?)", (i, currencies[i])
        )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS symbol (id INTEGER PRIMARY KEY, name TEXT UNIQUE)"
    )
    for i in range(len(symbols)):
        cur.execute(
            "INSERT OR IGNORE INTO symbol (id, name) VALUES (?,?)", (i, symbols[i])
        )

    conn.commit()

def create_database_tables(cur: Cursor, conn: Connection) -> None:
    """
    Create SQLite database schema for storing exchange rates and stock data
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        None
    """
    cur.execute('''
        CREATE TABLE IF NOT EXISTS exchange_rate (
            collect_date DATE, 
            base_currency INTEGER, 
            target_currency INTEGER, 
            rate REAL NOT NULL,
            PRIMARY KEY (collect_date, base_currency, target_currency)
            )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS stock (
            collect_date DATE, 
            symbol INTEGER, 
            open REAL, 
            high REAL, 
            low REAL, 
            close REAL NOT NULL, 
            volume REAL, 
            exchange TEXT, 
            currency TEXT,
            PRIMARY KEY (collect_date, symbol)
        )   
    ''')
    conn.commit()

def get_currency_id(currency: str, cur: Cursor, conn: Connection) -> int:
    """
    Get the id of a currency from the database.

    Parameters
    -----------------------
    currency: str
        The currency string.
    cur: Cursor
        The database cursor object.
    conn: Connection
        The database connection object.

    Returns
    -----------------------
    int: The id of the currency.
    """
    cur.execute(
        "SELECT id FROM currency WHERE name = ?", (currency,)
    )
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        return None

def get_symbol_id(symbol: str, cur: Cursor, conn: Connection) -> int:
    """
    Get the id of a symbol from the database.

    Parameters
    -----------------------
    symbol: str
        The symbol string.
    cur: Cursor
        The database cursor object.
    conn: Connection
        The database connection object.

    Returns
    -----------------------         
    int: The id of the symbol.
    """
    cur.execute(
        "SELECT id FROM symbol WHERE name = ?", (symbol,)
    )
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        return None     

def store_exchange_rates_to_db(data: list[tuple], cur: Cursor, conn: Connection) -> None:
    """
    Store exchange rate data into SQLite database
    
    Args:
        data: List of tuples containing exchange rate data
        cur: Cursor the database cursor object.
        conn: Connection the database connection object.
    Returns:
        None
    """
    for row in data:
        base_currency_id = get_currency_id(row[1], cur, conn)
        target_currency_id = get_currency_id(row[2], cur, conn)
        if base_currency_id is None :
            raise Exception(f"Error(store_exchange_rates_to_db): base currency {row[1]} not found")
        if target_currency_id is None:
            raise Exception(f"Error(store_exchange_rates_to_db): target currency {row[2]} not found")

        collect_date = datetime.strptime(row[0], '%Y-%m-%d').date()
        cur.execute(
            "INSERT OR IGNORE INTO exchange_rate (collect_date, base_currency, target_currency, rate) VALUES (?, ?, ?, ?)",
            (collect_date, base_currency_id, target_currency_id, row[3])
        ) 
    conn.commit()

def store_stock_data_to_db(data: list[tuple], cur: Cursor, conn: Connection) -> None:
    """
    Store stock market data into SQLite database
    
    Args:
        data: List of tuples containing stock data
        cur: Cursor the database cursor object.
        conn: Connection the database connection object.
    Returns:
        None
    """
    for row in data:
        collect_date = datetime.strptime(row[0], '%Y-%m-%d').date()
        symbol_id = get_symbol_id(row[1], cur, conn)
        cur.execute(
            "INSERT OR IGNORE INTO stock (collect_date, symbol, open, high, low, close, volume, exchange, currency) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (collect_date, symbol_id, row[2], row[3], row[4], row[5], row[6], row[7], row[8])
        )
    conn.commit()


def get_date_range(cur: Cursor, conn: Connection) -> tuple[tuple[date, date], tuple[date, date]]:
    """
    Get the date range of the exchange rate and stock data
    
    Args:
        cur: Cursor the database cursor object.
        conn: Connection the database connection object.
    Returns:
        tuple[tuple[date, date], tuple[date, date]]: (exchange_rate_start_date, exchange_rate_end_date), (stock_start_date, stock_end_date)
    """
    
    # get min and max date of currency
    cur.execute("SELECT MIN(collect_date), MAX(collect_date) FROM exchange_rate")
    row = cur.fetchone()
    exchange_rate_start_date = row[0]
    exchange_rate_end_date = row[1]

    # get min and max date of stock
    cur.execute("SELECT MIN(collect_date), MAX(collect_date) FROM stock")
    row = cur.fetchone()
    stock_start_date = row[0]
    stock_end_date = row[1] 

    return (exchange_rate_start_date, exchange_rate_end_date), (stock_start_date, stock_end_date)
    
def get_exchange_rate(base_currency: str, target_currency: str, cur: Cursor, conn: Connection) -> list[tuple[date, str, str, float]]:
    """
    Get the exchange rate between two currencies
    
    Args:
        base_currency: Base currency code (e.g., 'USD')
        target_currency: Target currency code (e.g., 'EUR')
        cur: Cursor the database cursor object.
        conn: Connection the database connection object.
    Returns:
        list[tuple[date, float]]: List of tuples containing date and exchange rate
    """
    # Stock data is only available on business days (Monday–Friday). 
    # Therefore, we filter the exchange rate data to match these trading dates.
    # This is why we add the condition: "AND exchange_rate.collect_date IN (SELECT collect_date FROM stock)"
    cur.execute('''
        SELECT exchange_rate.collect_date, base.name, target.name, rate 
        FROM exchange_rate 
        JOIN currency AS base ON base.id = exchange_rate.base_currency
        JOIN currency AS target ON target.id = exchange_rate.target_currency
        WHERE base.name = ? AND target.name = ? AND exchange_rate.collect_date IN (SELECT collect_date FROM stock)
        ORDER BY exchange_rate.collect_date ASC
    ''', (base_currency, target_currency)
    )
    return cur.fetchall()

def get_stock_data(symbol: str, cur: Cursor, conn: Connection) -> list[tuple[date, float, float, float, float, float, float, str, str]]:
    """
    Get the stock data for a given symbol
    
    Args:
        symbol: Stock symbol (e.g., 'AAPL')
        cur: Cursor the database cursor object.
        conn: Connection the database connection object.
    Returns:
        list[tuple[date, float, float, float, float, float, float, str, str]]: 
        List of tuples containing date, open, high, low, close, volume, exchange, currency
    """
    cur.execute('''
        SELECT collect_date, symbol.name, open, high, low, close, volume, exchange, currency 
        FROM stock 
        JOIN symbol ON symbol.id = stock.symbol
        WHERE symbol.name = ?
        ORDER BY collect_date ASC
    ''', (symbol,)
    )
    return cur.fetchall()

if __name__ == "__main__":
    # create database
    cur, conn = set_up_database("test.db")

    # delete all data
    cur.execute("delete from exchange_rate")
    cur.execute("delete from stock")
    conn.commit()

    # insert data
    exchange_rates = [
        ('2025-01-01', 'USD', 'CNY', 0.1),
        ('2025-01-01', 'USD', 'EUR', 0.2),
        ('2025-01-01', 'USD', 'GBP', 0.3),
        ('2025-01-02', 'USD', 'CNY', 0.4),
        ('2025-01-02', 'USD', 'EUR', 0.5),
        ('2025-01-02', 'USD', 'GBP', 0.6),
        ('2025-01-03', 'USD', 'CNY', 0.8),
        ('2025-01-03', 'USD', 'EUR', 0.9),
        ('2025-01-03', 'USD', 'GBP', 0.0),
    ]
    stocks = [
        ('2025-01-03', 'AAPL', 102, 103, 101, 102, 10000, 'NASDAQ', 'USD'),
        ('2025-01-02', 'AAPL', 101, 102, 100, 101, 10000, 'NASDAQ', 'USD'),
        ('2025-01-01', 'AAPL', 100, 101, 99, 100, 10000, 'NASDAQ', 'USD'),
        ('2025-01-01', 'TSLA', 80, 81, 79, 80, 10000, 'NASDAQ', 'USD'),
        ('2025-01-02', 'TSLA', 81, 82, 80, 81, 10000, 'NASDAQ', 'USD'),
        ('2025-01-03', 'TSLA', 82, 83, 81, 82, 10000, 'NASDAQ', 'USD'),
        ('2025-01-01', 'NVDA', 90, 91, 89, 90, 10000, 'NASDAQ', 'USD'),
        ('2025-01-02', 'NVDA', 91, 92, 90, 91, 10000, 'NASDAQ', 'USD'),
        ('2025-01-03', 'NVDA', 92, 93, 91, 92, 10000, 'NASDAQ', 'USD'),
    ]   
    store_exchange_rates_to_db(exchange_rates, cur, conn)
    store_stock_data_to_db(stocks, cur, conn)
    print("Create database and insert sample data")
    
    # get date range
    print()
    print("Get date range:")
    date_ranges = get_date_range(cur, conn)
    print(f"exchange_rate_start_date: {date_ranges[0][0]}")
    print(f"exchange_rate_end_date: {date_ranges[0][1]}")
    print(f"stock_start_date: {date_ranges[1][0]}")
    print(f"stock_end_date: {date_ranges[1][1]}")

    # get exchange rate data
    print()
    print("Data for USD->CNY:")
    data = get_exchange_rate('USD', 'CNY', cur, conn)
    for row in data:
        print(row)

    # get stock data
    print()
    print("Data for AAPL:")
    data = get_stock_data('AAPL', cur, conn)
    for row in data:
        print(row)

    conn.close()
