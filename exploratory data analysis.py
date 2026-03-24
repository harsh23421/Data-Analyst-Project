
import pandas as pd
import sqlite3

conn = sqlite3.connect('inventory.db')


def list_tables(conn):
    """Print all tables present in the database."""
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table'", conn
    )
    print(f"Found {len(tables)} table(s): {tables['name'].tolist()}\n")
    return tables


def preview_tables(tables, conn):
    """Display row counts and first 5 rows for every table."""
    for table in tables['name']:
        print('-' * 50, table, '-' * 50)
        count = pd.read_sql(f"SELECT COUNT(*) AS count FROM {table}", conn)['count'].values[0]
        print(f"Record count: {count}")
        df = pd.read_sql(f"SELECT * FROM {table} LIMIT 5", conn)
        print(df.to_string(index=False))
        print()


def analyze_purchases(conn):
    """Summarise purchase quantities and spend grouped by Brand and PurchasePrice."""
    purchases = pd.read_sql("SELECT * FROM purchases", conn)   # FIXED: now properly loaded
    summary = (
        purchases
        .groupby(['Brand', 'PurchasePrice'])[['Quantity', 'Dollars']]
        .sum()
        .reset_index()
        .sort_values('Dollars', ascending=False)
    )
    print("=== Purchase Summary by Brand & Price ===")
    print(summary.head(10).to_string(index=False))
    return summary


def analyze_sales(conn):
    """Summarise sales grouped by Brand."""
    sales = pd.read_sql("SELECT * FROM sales", conn)
    summary = (
        sales
        .groupby('Brand')[['SalesDollars', 'SalesPrice', 'SalesQuantity']]
        .sum()
        .reset_index()
        .sort_values('SalesDollars', ascending=False)
    )
    print("\n=== Sales Summary by Brand ===")
    print(summary.head(10).to_string(index=False))
    return summary


def analyze_freight(conn):
    """Summarise total freight cost per vendor."""
    freight = pd.read_sql_query(
        """
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
        ORDER BY FreightCost DESC
        """, conn
    )
    print("\n=== Freight Cost by Vendor (Top 10) ===")
    print(freight.head(10).to_string(index=False))
    return freight


def analyze_purchase_vs_actual_price(conn):
    """Compare purchase price paid vs actual market price by vendor."""
    df = pd.read_sql_query(
        """
        SELECT
            p.VendorNumber, p.VendorName, p.Brand,
            p.PurchasePrice, pp.Price AS ActualPrice, pp.Volume,
            SUM(p.Quantity) AS TotalQuantity,
            SUM(p.Dollars)  AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand
        ORDER BY TotalPurchaseDollars DESC
        """, conn
    )
    print("\n=== Purchase Price vs Actual Price (Top 10) ===")
    print(df.head(10).to_string(index=False))
    return df


if __name__ == '__main__':
    tables = list_tables(conn)
    preview_tables(tables, conn)
    analyze_purchases(conn)
    analyze_sales(conn)
    analyze_freight(conn)
    analyze_purchase_vs_actual_price(conn)
    conn.close()
