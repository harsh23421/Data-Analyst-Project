import pandas as pd
import sqlite3
conn=sqlite3.connect('inventory.db')
#checking tables present inside database
tables=pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'",conn)
tables
pd.read_sql("select count(*) from purchases",conn)
for table in tables['name']:
    print('-'*50,f'{table}','-'*50)
    print('Count of Records:',pd.read_sql(f"select count(*) as count from {table}",conn)['count'].values[0])
    display(pd.read_sql(f"select * from {table}",conn))
purchases.groupby(['Brand','PurchasePrice'])[['Quantity','Dollars']].sum()
sales = pd.read_sql("SELECT * FROM sales", conn)
sales.groupby('Brand')[['SalesDollars','SalesPrice','SalesQuantity']].sum()
freight_summary=pd.read_sql_query("""select VendorNumber,SUM(Freight) as FreightCost From vendor_invoice Group By VendorNumber""",conn)
pd.read_sql_query("""select p.VendorNumber,p.VendorName,p.Brand,p.PurchasePrice,SUM(p.Quantity) as totalquantity,SUM(p.Dollars) as totalpurchasedollars,pp.Volume,pp.Price as ActualPrice from purchases p join  purchase_prices pp on p.brand=pp.brand where p.PurchasePrice>0 group by p.VendorNumber,p.VendorName,p.Brand order by totalpurchasedollars """,conn)