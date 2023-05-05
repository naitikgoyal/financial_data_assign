import os
import configparser
from datetime import datetime
import pandas_datareader.data as web
import sqlite3

# Load config file
config = configparser.ConfigParser()
config.read('config1.ini')
companies = config['Companies'].keys()


start_date = datetime(2020, 1, 1)
end_date = datetime(2022, 1, 1)

# Create a conn
db_path = os.path.join(os.getcwd(), 'finance_data.db')
conn = sqlite3.connect(db_path)

# Loop
for company in companies:

    try:
        df = web.DataReader(company, 'yahoo', start_date, end_date)
    except:
        print(f"Error downloading data for {company}")
        continue


    df['company'] = company
    df.to_sql('finance_data', conn, if_exists='append', index=True)

# Close the conn
conn.close()
