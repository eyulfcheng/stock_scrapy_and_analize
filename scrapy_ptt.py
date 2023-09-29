##### Description #####

# Goal: Scrapy article title from PTT
#       PTT title -> MySQL

# Format:
    #`id`        VARCHAR(50) PRIMARY KEY,
    #`Re`        VARCHAR(4) ,
    #`class`     VARCHAR(255) ,
    #`title`     VARCHAR(255) ,
    #`month`     VARCHAR(4) ,
    #`day`       VARCHAR(4) ,
    #`like`      int(255) ,
    #`html`      VARCHAR(255) ,
    #`stock_code`VARCHAR(4)
# TO DO

##### Import #####
import re
import mysql.connector
import requests
from bs4      import BeautifulSoup
from datetime import datetime


# to do

##### Define class and function #####

## Basic Tool (print) ##
def print_red(string_input):
    string_cal = '\033[1;31m' + string_input  + '\033[0m'
    print(string_cal)
    
def print_blue(string_input):    
    string_cal = '\033[1;34m' + string_input  + '\033[0m'
    print(string_cal)

# Find target from string or return 'NULL'
def find(inp_to_find, string):
    re_find = re.findall(inp_to_find, string)
    if re_find:
        return re_find
    else:
        return ['NULL'] 


#def trans_date_into_time_format(date_string): # 

### Create mySQL DB ###
class mySQL_PTT_title_info:

    # Create DB in mySQL
    def create():  
        myconnect = mysql.connector.connect(host= "localhost", user = "root", password = "root")
        mycursor = myconnect.cursor()
        mycursor.execute('DROP DATABASE IF EXISTS ptt_stock')
        mycursor.execute('CREATE DATABASE ptt_stock')
        mycursor.close() 
        print_blue('succesed connect mysql and create database')
        mycursor = myconnect.cursor()
        list_sql_com = [
            'USE ptt_stock;',
            'DROP TABLE IF EXISTS unit',
            '''CREATE TABLE unit  (
            `id`        INT(40) primary key ,
            `Reply`     VARCHAR(4) ,
            `class`     VARCHAR(255) ,
            `title`     VARCHAR(255) ,
            `date`      DATE ,
            `like`      INT(255) ,
            `url`       VARCHAR(255) ,
            `stock_code`VARCHAR(6)
            );''']
            
        for unit_sql_com in list_sql_com:
            mycursor.execute(unit_sql_com)
        print_blue('succesed create table')
        # mycursor.commit()
        mycursor.close()
        myconnect.close()

    # 
    def insert(array_input):
        myconnect = mysql.connector.connect(host= "localhost", user = "root", password = "root")
        mycursor = myconnect.cursor()
        mycursor.execute("USE ptt_stock;")
        
        for list_input in array_input:
            str_input = '"'
            for index, value in enumerate(list_input): 
                str_input += value 
                if index == len(list_input)-1:
                    str_input += '"'
                else:
                    str_input += '","'
            print(str_input)
            mycursor.execute("INSERT INTO unit VALUES(" + str_input +")")
            
        myconnect.commit()
        mycursor.close()



### Scrapy from PTT ###

class PTT_Scrapy:
    def __init__(self, url, start_date = ' 3/01'):
        self.url        = url
        self.start_date = start_date

    # Enter a PTT page from url
    def get_requests_from_PTT(self):
        HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) \
                   AppleWebKit/537.36 (KHTML, like Gecko) \
                   Chrome/88.0.4324.182 Safari/537.36'}
        self.webpage = requests.get( self.url, headers = HEADERS, cookies={'over18':'1'})
        
        if (str(self.webpage)!="<Response [200]>"):
            print(self.webpage) 
            # response table https://ithelp.ithome.com.tw/m/articles/10269928
            return self.webpage
        else:
            print("Response from request is OK.")
            return False
    # scrapy info about title and make a array   
    def scrapy_title(self):
        soup = BeautifulSoup(self.webpage.text,'html.parser')        
        bs4_name  = soup.find_all('div', class_='r-ent')
        unit_array = []
        unit_id = 0
        unit_year = "2023" # 5817->2022
        for unit_div in bs4_name :
            
            # The div for a article, all info about this article is put in this div. 
            unit       = find('<a href=(.+?)</a>\n</div>',     str(unit_div))[0]
            if unit         == "None" : continue
            
            # Classification about PTT stock layout rule [OO]
            unit_class  = find('\[(.*)\]',unit)[0]
            if   unit_class == "NULL" : continue
            elif unit_class == "公告" : continue
            
            # The day article post
            unit_date  = find('<div class="date">(.*?)</div>', str(unit_div))[0]
            if unit_date == self.start_date:
                print('不抓'+ str(self.start_date) +'(含)之前的文')
                # To Do
                break
            unit_month = find('(\d{1,2})\/\d{2}', unit_date)[0]
            unit_day   = find('\d{1,2}\/(\d{2})', unit_date)[0]
            if   len(unit_month) == 1:
                unit_date_format = unit_year +'-0' + unit_month +'-' + unit_day
            elif len(unit_month) == 2:
                unit_date_format = unit_year +'-'  + unit_month +'-' + unit_day
            #datetime.strptime(unit_date_format,"%Y-%M-%D")
            
            # Title of article
            unit_title = find('(?<=]).+',unit)[0].replace('"',' ').replace('\'',' ')
            
            # The count of like of article
            unit_like  = find('class=.*">(.*?)<', str(unit_div))[0]
            if   unit_like == '爆': unit_like = "100"
            elif str(unit_like)   : unit_like = str(unit_like)
            else:                   unit_like = "0" 
            
            # The code of stock, need to avoid year and day
            unit_code   = find('\d{4,6}',unit_title)[0]
            if unit_class == "閒聊" : continue
            
            # The other index about article 
            unit_url    = 'https://www.ptt.cc' + find('"(/bbs/Stock/M.*?)"',unit)[0]
            unit_id     = unit_id + 1
            unit_reply  = find('[A-Z][a-z]:\s',unit)[0]
            
            # Make a array to output
            unit_list   = [str(unit_id), unit_reply, unit_class, unit_title, unit_date_format, unit_like, unit_url, unit_code ]
            unit_array.append(unit_list)
            
        return unit_array
                       
first_scrapy         = PTT_Scrapy('https://www.ptt.cc/bbs/Stock/index.html', ' 3/01')
first_webpage        = first_scrapy.get_requests_from_PTT()         
article_array        = first_scrapy.scrapy_title()
PTT_title_info_table = mySQL_PTT_title_info
mySQL_PTT_title_info.create()
mySQL_PTT_title_info.insert(article_array)