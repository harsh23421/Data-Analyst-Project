import sqlite3
import pandas as pd
import logging
from ingestion import ingest_db
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)
def create_vendor_summary(conn):
    '''this function will merge the different tables to get overall vendor summary and adding new columns in the resultant data'''
    # REPLACE WITH:
    query = """
        WITH FreightSummary AS (
            SELECT VendorNumber, SUM(Freight) AS FreightCost
            FROM vendor_invoice
            GROUP BY VendorNumber
        ),
        PurchaseSummary AS (
            SELECT
                p.VendorNumber, p.VendorName, p.Brand, p.Description,
                p.PurchasePrice, pp.Price AS ActualPrice, pp.Volume,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars)  AS TotalPurchaseDollars
            FROM purchases p
            JOIN purchase_prices pp ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY p.VendorNumber, p.VendorName, p.Brand,
                     p.Description, p.PurchasePrice, pp.Price, pp.Volume
        ),
        SalesSummary AS (
            SELECT VendorNo, Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars)  AS TotalSalesDollars,
                SUM(SalesPrice)    AS TotalSalesPrice,
                SUM(ExciseTax)     AS TotalExciseTax
            FROM Sales
            GROUP BY VendorNo, Brand
        )
        SELECT
            ps.VendorNumber, ps.VendorName, ps.Brand, ps.Description,
            ps.PurchasePrice, ps.ActualPrice, ps.Volume,
            ps.TotalPurchaseQuantity, ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity, ss.TotalSalesDollars,
            ss.TotalSalesPrice, ss.TotalExciseTax, fs.FreightCost
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs
            ON ps.VendorNumber = fs.VendorNumber
        ORDER BY ps.TotalPurchaseDollars DESC
    """
    return pd.read_sql(query, conn)
def clean_data(df):
    '''this function will clean the data'''
    #change datatype to float
    df = df.copy()
    df['Volume'] = df['Volume'].astype('float')
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalPurchaseDollars']) * 100
    df['StockTurnover'] = df.apply(
        lambda r: r['TotalSalesQuantity'] / r['TotalPurchaseQuantity']
        if r['TotalPurchaseQuantity'] > 0 else 0, axis=1
    )
    df['SalesToPurchaseRatio'] = df.apply(   # FIXED: was vendor_sales_number (typo)
        lambda r: r['TotalSalesDollars'] / r['TotalPurchaseDollars']
        if r['TotalPurchaseDollars'] > 0 else 0, axis=1
    )
    return df
if __name__=='__main__':
    #creating database connection
    conn=sqlite3.connect('inventory.db')
    logging.info('Creating Vendor Summary Table')
    summary_df=create_vendor_summary(conn)
    logging.info(summary_df.head())
    logging.info('Cleaning Data')
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())
    logging.info("ingesting data...")
    ingest_db(clean_df,"vendor_sales_summary",conn)
    logging.info("completed")
