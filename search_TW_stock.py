##### Description #####

# Goal: Get stock info 
#       twstock-> MySQL

# Step:


##### Import #####
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
pd.set_option("display.max_rows",1000000000)
pd.set_option("display.max_columns",1000000000)
import twstock
from datetime import timedelta, datetime, date
import numpy as np
import matplotlib.pyplot as plt
import talib

from pandas_datareader import data as web
import yfinance as yfin
yfin.pdr_override()
## 'Yahoo!_Issue #952'
## pip uninstall yfinance
## pip uninstall pandas-datareader
## pip install yfinance --upgrade --no-cache-dir
## pip install pandas-datareader
## Restart kernal

#pip install mysql-connector
#pip install sqlalchemy
#pip install pandas-datareader
#pip install twstock
#pip install matplotlib

##### Define class and function #####

## Basic Tool (print) ##
def print_red(string_input):
    string_cal = '\033[1;31m' + string_input  + '\033[0m'
    print(string_cal)
    
def print_blue(string_input):    
    string_cal = '\033[1;34m' + string_input  + '\033[0m'
    print(string_cal)

def data_frame_normalize(df):
    df_norm = (df - df.min()) / (df.max() - df.min())
    return df_norm


class create_table_per_stock_code():
    
    # Get stock_code from MySQL we stored to search its stock price.
    def get_stock_code(target_class = '標的'):
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        engine = create_engine('mysql+pymysql://root:root@localhost:3306/ptt_stock')
        sql_unit = 'select * from ptt_stock.unit'
        data_framework_from_sql_unit = pd.read_sql_query( sql_unit, engine)
        
        fliter1 = data_framework_from_sql_unit['stock_code'] != ' '  
        fliter2 = data_framework_from_sql_unit['class']      == target_class
        df_filter = data_framework_from_sql_unit[fliter1 & fliter2]
        df_sorted = df_filter.groupby('stock_code').size().sort_values(ascending = False)
        return df_sorted
    
    # Create table of every stock_code in mySQL.
    def create_by_stock_data(code_name, data_framework_stock):  
        myconn = mysql.connector.connect(host= "localhost",user = "root", password = "root")
        mycursor = myconn.cursor()    
        mycursor.execute('USE ptt_stock;')
        mycursor.execute('DROP TABLE IF EXISTS stock_'+ code_name)
        
        engine = create_engine('mysql+pymysql://root:root@localhost:3306/ptt_stock')    
        data_framework_stock.to_sql( 'stock_' + code_name, engine, index= True)
        
        print_blue( code_name + ' table 創建成功\n')
        mycursor.close()
        myconn.close()

# Get stock price by TW_stock    
class analyze_stock_data():
    
    def __init__(self, target_date):
        self.flag_get_market_price = False
        if ((target_date + timedelta(days=120 *7 /5)) < datetime.today()) :
            self.flag_get_market_price = True
            self.start_date = target_date - timedelta(days= 60 *7 /5)
            self.end_date   = target_date + timedelta(days=120 *7 /5)
            print("Date read successed")
        
    def get_market_price(self):
        if (self.flag_get_market_price == False):
            print_red("Target Date is less than 120")
            return False
        
        ## Get data from start_date(prepare 5 days for sma) to end_date (After 120 days)
        symbol = "^TWII"     
        market_df = web.get_data_yahoo(tickers = symbol, start = self.start_date, end = self.end_date)
        market_df_NAN = talib.SMA(market_df["Close"], 60).round(2)
        market_close_df_sma = market_df_NAN.iloc[60 -1:]
        market_close_df_normalize = data_frame_normalize(market_close_df_sma)
        plt.plot(market_close_df_normalize,label="market") 

        return market_close_df_normalize
    
    
    def get_target_price(self, target_code):
        #target_code must be type string 
        print("Test_start")
        if (self.flag_get_market_price == False):
            print_red("Target Date is less than 120")
            return False
        print("Test_second")
        if (target_code in twstock.twse):
            target_df = web.get_data_yahoo(tickers = str(target_code)+".TW", start = self.start_date, end = self.end_date)
            target_df_NAN = talib.SMA(target_df["Close"], 60).round(2)
            target_close_df_sma = target_df_NAN.iloc[60 -1:]
            target_close_df_normalize = data_frame_normalize(target_close_df_sma)
            plt.plot(target_close_df_normalize,label="target") 
            
            return target_close_df_normalize
        else:
            return False
        
            plt.legend() 
            plt.show()

        
test_datetime = datetime.strptime("2021-1-2", "%Y-%m-%d")   
#test_datetime = datetime(2021, 1, 2)
#print(type(test_datetime))
Test = analyze_stock_data(test_datetime)
Test.get_market_price () 
Test.get_target_price("2330") 
#print(data)