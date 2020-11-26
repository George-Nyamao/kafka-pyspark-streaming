import pandas as pd
from bs4 import BeautifulSoup
import time
from datetime import datetime
import re
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import chromedriver_binary
import string

pd.options.display.float_format = '{:.0f}'.format
is_link = 'https://finance.yahoo.com/quote/AAPL/financials?p=AAPL'
driver = webdriver.Chrome(ChromeDriverManager().install())
driver.get(is_link)
html = driver.execute_script('return document.body.innerHTML;')
soup = BeautifulSoup(html,'lxml')

#close_price = [entry.text for entry in soup.find_all('span', {'class':'Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)'})]

if __name__=="__main__":
    while True:
        driver.get(is_link)
        html = driver.execute_script('return document.body.innerHTML;')
        soup = BeautifulSoup(html, 'lxml')
        event_datetime = datetime.now()
        close_price = [entry.text for entry in soup.find_all('span', {'class': 'Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)'})]
        print(close_price[0])
        time.sleep(1)
