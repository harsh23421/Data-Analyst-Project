import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)
engine=create_engine('sqlite:///inventory.db')
def ingest_db(df,table_name,engine):
       """Ingest a DataFrame into the specified SQLite table."""
      df.to_sql(table_name,con=engine,if_exists='replace',index=False)
      path=os.getenv("path", os.path.join(os.path.dirname(__file__), "data"))


def load_raw_data():
    """Load all CSV files from DATA_PATH and ingest into the database."""
    if not os.path.exists(path):
        logging.error(f"Data directory not found: {DATA_PATH}")
        raise FileNotFoundError(
            f"Data directory '{path}' does not exist. "
            "Set the DATA_PATH environment variable or place CSVs in a 'data/' folder."
        )
    start = time.time()
    files_ingested = 0
    for file in os.listdir(DATA_PATH):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(DATA_PATH, file))
            logging.info(f"Ingesting '{file}' as table '{file[:-4]}'")
            ingest_db(df, file[:-4], engine)
            files_ingested += 1
    total_time = (time.time() - start) / 60
    logging.info(f"Ingestion complete. {files_ingested} file(s) loaded in {total_time:.2f} minutes.")
    print(f"Done. {files_ingested} file(s) ingested in {total_time:.2f} minutes.")
   
if __name__=="__main__":
    load_raw_data()
