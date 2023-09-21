# %%
import requests
import pandas as pd
import os
import zipfile
import shutil
import time

# %%
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
def  binance_ohlc_downloader(start, end, tf, ticker):

    date = pd.date_range(start, end , freq = '1D')
    day = []
    for i in range(len(date)):
        day.append(str(date[i].date()))

    data =  pd.DataFrame()
    url = "https://data.binance.vision/data/futures/um/daily/klines/"+str(ticker)+"/"+str(tf)+"/"

    # try:
    for i in day:
        
        filename = f"{ticker}-{tf}-{i}.zip"
        folder =  f"{ticker}-{tf}-{i}"
        print(url + filename)
        response = requests.get(url + filename)

        with open(filename, "wb") as f:
            f.write(response.content)

        try :

            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(f"{ticker}-{tf}-{i}")

            sh_filename = f"\{ticker}-{tf}-{i}.csv"
            folder = f"{ticker}-{tf}-{i}"
            df = pd.read_csv( str(folder)+ str(sh_filename) , usecols = [0,1,2,3,4,5,6,7,8,9,10] )
            
            if df.columns[1] != 'open':
                df = df.reset_index().T.reset_index().T
                df = df.drop(columns=df.columns[0], axis=1).reset_index(drop = True)

            df.columns = ['datetime','open','high','low','close','volume','close_time','quote_volume', 'count' , 'taker_buy_volume' , 'taker_buy_quote_volume' ]
            print(i)
            df['datetime'] = pd.to_datetime(df['datetime'], unit =  'ms')
            # df['datetime'] = pd.to_datetime(df['datetime'])

            data = pd.concat([data,df]).sort_values('datetime').drop_duplicates('datetime')

            os.remove(filename)
            shutil.rmtree(str(folder))

            data = data.sort_values('datetime').drop_duplicates('datetime')
            
            for i in data.columns:
                if i != 'datetime':
                    data[str(i)] = pd.to_numeric(data[str(i)], errors='coerce')

        except  :
            print(' File Not Found Skipped ')
            os.remove(filename)
            # break
            pass

            
            
    # except:
    #     pass
    
    return data

# %%
import psycopg2
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{ip}:{port}/binance_data')

def create_table( table_name ):

    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
        
        datetime TIMESTAMPTZ PRIMARY KEY,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume FLOAT,
        close_time INT,
        quote_volume FLOAT ,
        count INT,
        taker_buy_volume FLOAT ,
        taker_buy_quote_volume FLOAT
            
        );
    '''
    with engine.connect() as conn:
        conn.execute(create_table_query)

# %%
def download_create_table_then_store(  start_1 , end_1 , tf_1 , ticker  ):

    df = binance_ohlc_downloader( start =  start_1  , end = end_1 , tf = tf_1 , ticker = ticker )
    tb_name = f'{ticker}_{tf_1}'
    create_table( tb_name )
    df.set_index('datetime').to_sql( tb_name , engine , if_exists='replace' , index = True  )

# %%
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import time
import re

# %%
def clean_date_list(  ticker ):


    op = webdriver.ChromeOptions()
    op.add_argument('headless')
    driver = webdriver.Chrome( options=op )

    driver.get(f'https://data.binance.vision/?prefix=data/futures/um/daily/klines/{ticker}/5m/')
    time.sleep(5)
    date_range = driver.find_elements_by_xpath( "//*[@id='listing']" )

    date_range = [ name.text for name in date_range ]
    
    # Define a regular expression pattern to match date strings in the format "YYYY-MM-DD"
    date_pattern = r'\d{4}-\d{2}-\d{2}'

    # Find all date matches in the input text
    date_matches = re.findall(date_pattern,  date_range[0]  )

    date_matches = sorted(list(set(date_matches)))

    return date_matches

# %%
def get_symbols_list():

   op = webdriver.ChromeOptions()
   op.add_argument('headless')
   driver = webdriver.Chrome( options=op )

   driver.get('https://data.binance.vision/?prefix=data/futures/um/daily/klines/')
   time.sleep(5)
   clist = driver.find_elements_by_xpath( "//*[@id='listing']" )
   clist = [name.text for name in clist ] 
   clist = [  item.replace('/', '') for item in clist ]
   clist = [  item.replace('\n', ',') for item in clist ]
   clist = clist[0].split( ',' )[1:]
   clist = [ item for item in clist if not any(char.isdigit() for char in item ) ]
   clist =  [item for item in clist if "BUSD" not in item]

   return clist
clist =  get_symbols_list()

# %%
from datetime import datetime

# start_1 =  '2019-12-31' 
end_1 = str( datetime.now().date() )
tf_list = ['5m']

for symb in clist  :
    try:
        start_1 = clean_date_list( symb )[0]
        for tif in tf_list:
            print(symb , tif )
            download_create_table_then_store( start_1 , end_1 , tif , symb  )
    except:
        pass

# %%
from datetime import datetime

# start_1 =  '2019-12-31' 
end_1 = str( datetime.now().date() )
tf_list = ['5m']

for symb in clist  :
    try:
        start_1 = clean_date_list( symb )[0]
        for tif in tf_list:
            print(symb , tif )
            download_create_table_then_store( start_1 , end_1 , tif , symb  )
    except:
        pass

# %%
from datetime import datetime

# start_1 =  '2019-12-31' 
end_1 = str( datetime.now().date() )
tf_list = ['5m']

for symb in clist  :
    try:
        start_1 = clean_date_list( symb )[0]
        for tif in tf_list:
            print(symb , tif )
            download_create_table_then_store( start_1 , end_1 , tif , symb  )
    except:
        pass

# %%
from datetime import datetime

# start_1 =  '2019-12-31' 
end_1 = str( datetime.now().date() )
tf_list = ['5m']

for symb in clist  :
    try:
        start_1 = clean_date_list( symb )[0]
        for tif in tf_list:
            print(symb , tif )
            download_create_table_then_store( start_1 , end_1 , tif , symb  )
    except:
        pass


