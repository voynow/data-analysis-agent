#!/usr/bin/env python3
"""
Simple seed script for in-memory stock database.
Generates basic stock data and market prices.
"""

import random
import sqlite3
from datetime import datetime, timedelta

import polars as pl


def create_tables(conn: sqlite3.Connection) -> None:
    """Create database tables for stock data."""

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            symbol TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            sector TEXT NOT NULL,
            market_cap REAL NOT NULL,
            current_price REAL NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            price REAL NOT NULL,
            volume INTEGER NOT NULL,
            FOREIGN KEY (symbol) REFERENCES stocks (symbol),
            UNIQUE(symbol, timestamp)
        )
    """)


def generate_stocks_data() -> pl.DataFrame:
    """Generate basic stock data."""

    companies = [
        ("AAPL", "Apple Inc.", "Technology", 2.8e12, 175.50),
        ("MSFT", "Microsoft Corporation", "Technology", 2.9e12, 380.25),
        ("GOOGL", "Alphabet Inc.", "Technology", 1.8e12, 140.75),
        ("AMZN", "Amazon.com Inc.", "Consumer Discretionary", 1.6e12, 145.80),
        ("TSLA", "Tesla Inc.", "Consumer Discretionary", 800e9, 245.30),
        ("NVDA", "NVIDIA Corporation", "Technology", 1.2e12, 485.90),
        ("JPM", "JPMorgan Chase & Co.", "Financial Services", 450e9, 165.40),
        ("JNJ", "Johnson & Johnson", "Healthcare", 380e9, 155.20),
        ("V", "Visa Inc.", "Financial Services", 520e9, 275.60),
        ("PG", "Procter & Gamble Co.", "Consumer Staples", 340e9, 145.80),
        ("UNH", "UnitedHealth Group Inc.", "Healthcare", 480e9, 485.30),
        ("HD", "The Home Depot Inc.", "Consumer Discretionary", 320e9, 325.40),
        ("MA", "Mastercard Inc.", "Financial Services", 380e9, 425.70),
        ("DIS", "The Walt Disney Company", "Communication Services", 180e9, 85.20),
        ("PYPL", "PayPal Holdings Inc.", "Financial Services", 70e9, 65.40),
        ("NFLX", "Netflix Inc.", "Communication Services", 240e9, 485.60),
        ("CRM", "Salesforce Inc.", "Technology", 220e9, 245.80),
        ("INTC", "Intel Corporation", "Technology", 180e9, 45.30),
        ("VZ", "Verizon Communications Inc.", "Communication Services", 160e9, 35.60),
        ("KO", "The Coca-Cola Company", "Consumer Staples", 250e9, 55.40),
    ]

    return pl.DataFrame(
        companies,
        schema=["symbol", "company_name", "sector", "market_cap", "current_price"],
        orient="row",
    )


def generate_market_data(stocks_df: pl.DataFrame, days: int) -> pl.DataFrame:
    """Generate 15-second granularity market data for all stocks."""

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    # Generate 15-second timestamps (market hours only: 9:30 AM - 4:00 PM ET)
    timestamps = []
    current_date = start_date

    while current_date <= end_date:
        # Skip weekends
        if current_date.weekday() < 5:  # Monday = 0, Friday = 4
            # Market hours: 9:30 AM to 4:00 PM (6.5 hours = 23400 seconds = 1560 intervals of 15 seconds)
            for interval in range(1560):
                timestamp = current_date.replace(
                    hour=9, minute=30, second=0, microsecond=0
                ) + timedelta(seconds=interval * 15)
                timestamps.append(timestamp)
        current_date += timedelta(days=1)

    market_data = []

    for stock in stocks_df.iter_rows(named=True):
        symbol = stock["symbol"]
        base_price = stock["current_price"]

        for timestamp in timestamps:
            # Smaller price movements for 15-second data (0.025% volatility per 15 seconds)
            price_change = random.normalvariate(0, base_price * 0.00025)
            price = base_price + price_change
            # Lower volume for 15-second data
            volume = random.randint(100, 10000)

            market_data.append(
                {
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "price": round(price, 2),
                    "volume": volume,
                }
            )

            base_price = price

    return pl.DataFrame(market_data)


def insert_data(conn: sqlite3.Connection, df: pl.DataFrame, table_name: str) -> None:
    """Insert DataFrame into SQLite table."""

    records = df.to_dicts()
    if not records:
        return

    columns = list(records[0].keys())
    placeholders = ", ".join(["?" for _ in columns])
    column_names = ", ".join(columns)

    insert_sql = (
        f"INSERT OR REPLACE INTO {table_name} ({column_names}) VALUES ({placeholders})"
    )

    cursor = conn.cursor()

    # Convert datetime objects to ISO format strings
    processed_records = []
    for record in records:
        processed_record = {}
        for key, value in record.items():
            if isinstance(value, datetime):
                processed_record[key] = value.isoformat()
            else:
                processed_record[key] = value
        processed_records.append(tuple(processed_record.values()))

    cursor.executemany(insert_sql, processed_records)
    conn.commit()


def get_db(filename: str = "financial.db") -> sqlite3.Connection:
    """Get the database connection. Loads existing file or creates new one."""

    import os

    if os.path.exists(filename):
        print(f"Loading existing database from {filename}...")
        conn = sqlite3.connect(filename)
    else:
        print(f"Creating new database and saving to {filename}...")
        # Create in memory first, then save to file
        conn = sqlite3.connect(":memory:")
        create_tables(conn)

        stocks_df = generate_stocks_data()
        insert_data(conn, stocks_df, "stocks")

        market_df = generate_market_data(stocks_df, days=90)
        insert_data(conn, market_df, "market_data")

        # Save the in-memory database to the file
        backup_conn = sqlite3.connect(filename)
        conn.backup(backup_conn)
        backup_conn.close()

    return conn
