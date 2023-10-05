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
import random
import time
import mysql.connector
import requests
from bs4      import BeautifulSoup
from datetime import timedelta, datetime, date

# to do

##### Define class and function #####

## Basic Tool (print) ##
def print_red(string_input):
    string_cal = '\033[1;31m' + string_input  + '\033[0m'
    print(string_cal)
    
def print_blue(string_input):    
    string_cal = '\033[1;34m' + string_input  + '\033[0m'
    print(string_cal)
    
## Sleep random seconds to avoid DDos
def sleep_random_second():
    random_second = round(random.random() * 5 + 5, 2)
    print('Let me sleep for...',random_second,'seconds')
    time.sleep(random_second)

## Find target from string or return ' '
def find(inp_to_find, string):
    re_find = re.findall(inp_to_find, string)
    if re_find:
        return re_find
    else:
        return [" "]


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
            `stock_code`VARCHAR(7) ,
            `author`    VARCHAR(12)
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
            #print(str_input)
            mycursor.execute("INSERT INTO unit VALUES(" + str_input +")")
            
        myconnect.commit()
        mycursor.close()



### Scrapy from PTT ###
class PTT_scrapy():
    def __init__(self, url, start_date, unit_id=0):
        self.url        = url
        self.start_date = start_date
        self.flag_keep_scrapy = True
        self.unit_id = unit_id
        self.flag_debug = True
    
    ## To find year by this page's URL.     
    def find_year(self, div_input ,month):
        List_new_year_page_ID = [5817,4526,2413,955,215] 
        # When url_id == 5818 self.unit_year = "2023" .When url_id == 5816 self.unit_year = "2022"
        # new_year_page_ID of 2023 : 5817 ...
        # These article account are too less:
        # [118,[2015,2016]],[80,[2014,2015],[52,[2012,2013]],[26,[2008,2009]],[3,[2007,2008]]
        today = date.today()
        unit_url_id = find('https://www.ptt.cc/bbs/Stock/index(.+?).html', self.url)[0]
        if       unit_url_id    == " " : unit_year = str(today.year)
        elif int(unit_url_id)   <  List_new_year_page_ID[-1]: return "2000"
        else :
            for index,threshold in enumerate(List_new_year_page_ID):
                if   int(unit_url_id) >  threshold : unit_year = str(int(today.year) - index - 1) ; break
                elif int(unit_url_id) == threshold :
                    if   int(month)   == 1      : unit_year = str(int(today.year) - index - 1)
                    elif int(month)   == 12     : unit_year = str(int(today.year) - index - 2)
                else: continue
        return unit_year
    
    ## Transfer date on PTT (9/26) to standrad format (2023-09-26).
    def find_date(self, div_input):
        unit_date   = find('<div class="date">(.*?)</div>', str(div_input))[0]
        self.unit_month  = find('(\d{1,2})\/\d{2}', unit_date)[0]
        self.unit_day    = find('\d{1,2}\/(\d{2})', unit_date)[0]
        self.unit_year   = self.find_year(div_input, self.unit_month)
        
        if   len(self.unit_month) == 1: #(9/26)->(2023-09-26)
            unit_date_format = self.unit_year +'-0' + self.unit_month +'-' + self.unit_day
        elif len(self.unit_month) == 2: #(10/02)->(2023-10-02)
            unit_date_format = self.unit_year +'-'  + self.unit_month +'-' + self.unit_day
        else :
            return False
        
        ## Stop catch data after start_date.
        timedate_1_day = timedelta(days=1)
        without_start_date = datetime.strftime(datetime.strptime( self.start_date,"%Y-%m-%d") - timedate_1_day, "%Y-%m-%d")
        
        #print(unit_date_format +',' +str(without_start_date) )
        if unit_date_format == str(without_start_date):
            print('不抓'+ str(self.start_date) +'之前的文')
            self.flag_keep_scrapy = False
            return False
        
        return unit_date_format
   
    # Transfer any type of like to int.
    def find_like(self, div_input):
        unit_like  = find('class="nrec">.*">(.*?)<', str(div_input))[0]
        if   unit_like == '爆'   : unit_like = "100"
        elif unit_like == ' '    : unit_like = "0"
        elif find('X', str(unit_like))[0] == "X" :
            unit_like = "0"
        elif int(unit_like)      : unit_like = str(unit_like)
        else                     : unit_like = "0" 
        return unit_like
    
    # To find stock_code from title and avoid to catch date string
    def find_code(self,title_input):
        unit_code = find('(\d{4,6}[A-Z]{0,1})',title_input)[0]
        if self.flag_debug == True:
            print("find_code: " + unit_code)
        if unit_code == self.unit_year:
            return " "
        #if   unit_code == 
        return unit_code
        
    # Enter a PTT page from url
    def get_requests_from_PTT(self, url):
        HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) \
                   AppleWebKit/537.36 (KHTML, like Gecko) \
                   Chrome/88.0.4324.182 Safari/537.36'}
        self.webpage = requests.get( url, headers = HEADERS, cookies={'over18':'1'})
        
        if (str(self.webpage)!="<Response [200]>"):
            print(self.webpage) 
            # response table https://ithelp.ithome.com.tw/m/articles/10269928
            return self.webpage
        else:
            print("Response from request is OK.")
            return True
        
    ## Scrapy info about title and make a array   
    def scrapy_title(self):
        self.soup = BeautifulSoup(self.webpage.text,'html.parser')        
        bs4_name  = self.soup.find_all('div', class_='r-ent')
        unit_array = []
         
        for unit_div in bs4_name :
            flag_find_code = True
            
            # The div for a article, all info about this article is put in this div. 
            unit        = find('<a href=(.+?)</a>\n</div>',     str(unit_div))[0]
            if   unit       == "None" : continue
            
            # Classification about PTT stock layout rule [OO]
            unit_class  = find('\[(.*)\]',unit)[0]
            if   unit_class == " "    : continue
            elif unit_class == "公告" : continue
            elif unit_class == "閒聊" : flag_find_code = False
            
            # The day article post
            unit_date   = self.find_date(unit_div)
            if   unit_date  == False  : break
            
            # Title of article
            #unit_title  = find('(?<=]).+',unit)[0].replace('"',' ').replace('\'',' ')
            unit_title  = find('\[.*\](.+)',unit)[0].replace('"',' ').replace('\\',' ')
            if self.flag_debug == True:
                print("find_title: " + unit_title)
            # The code of stock, need to avoid year and day
            if   flag_find_code == True:
                unit_code   = self.find_code(unit_title)
            else:
                unit_code   = " "
            
            # The count of like of article
            unit_like   = self.find_like(unit_div)
            
            # The other index about article about connection URL
            unit_url    = 'https://www.ptt.cc' + find('"(/bbs/Stock/M.*?)"',unit)[0]
            
            # Is article replyed from other article 
            unit_reply  = find('[A-Z][a-z]:\s',unit)[0]
            
            # Find Author
            unit_author = find('<div class="author">(.*?)</div>',     str(unit_div))[0]
            
            # The id will be recorded in mySQL 
            self.unit_id     = self.unit_id + 1
            
            # Make a array to output
            unit_list   = [str(self.unit_id), unit_reply, unit_class, unit_title, unit_date, unit_like, unit_url, unit_code, unit_author ]
            if self.flag_debug == True:
                print(unit_list)
            unit_array.append(unit_list)
            
        return unit_array
    
    ### Main function: Scrapy every pages until start_date.
    def scrapy_total_PTT_stock_page(self):
        
        ## Create DB on mySQL
        PTT_title_info_table = mySQL_PTT_title_info
        mySQL_PTT_title_info.create()
        page_url = self.url
        
        ## Scrapy every pages
        while self.flag_keep_scrapy == True :
            every_webpage = self.get_requests_from_PTT(page_url)
            article_array = self.scrapy_title()
        
            ## Save data in mySQL
            mySQL_PTT_title_info.insert(article_array)
            if self.flag_debug == True:
                print("soup: " + str(self.soup))
            if (len(self.soup.find_all('a', class_='btn wide')) != 0):
                div_last_page = str(self.soup.find_all('a', class_='btn wide')[1]) # [0]:The oldest page
            else:
                continue
            url_last_page ='https://www.ptt.cc' + find('<a class="btn wide" href="(.*?)">‹ 上頁', div_last_page)[0]
            print_blue(url_last_page)
            page_url      = url_last_page
            sleep_random_second()
        # End of while
        
        print("Happy Endding")
        return True
    
    def increase_PTT_stock_page(self):
        
        ## Create DB on mySQL
        PTT_title_info_table = mySQL_PTT_title_info
        #mySQL_PTT_title_info.create()
        page_url = self.url
        
        ## Scrapy every pages
        while self.flag_keep_scrapy == True :
            every_webpage = self.get_requests_from_PTT(page_url)
            article_array = self.scrapy_title()
        
            ## Save data in mySQL
            mySQL_PTT_title_info.insert(article_array)
            if self.flag_debug == True:
                print("soup: " + str(self.soup))
            if (len(self.soup.find_all('a', class_='btn wide')) != 0):
                div_last_page = str(self.soup.find_all('a', class_='btn wide')[1]) # [0]:The oldest page
            else:
                continue
            url_last_page ='https://www.ptt.cc' + find('<a class="btn wide" href="(.*?)">‹ 上頁', div_last_page)[0]
            print_blue(url_last_page)
            page_url      = url_last_page
            sleep_random_second()
        # End of while
        
        print("Happy Endding")
        return True

main_function = PTT_scrapy('https://www.ptt.cc/bbs/Stock/index.html','2018-01-01')
main_function.scrapy_total_PTT_stock_page()
#main_function.increase_PTT_stock_page()