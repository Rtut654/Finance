# scrape data from web pages
import requests
from bs4 import BeautifulSoup
from selenium import webdriver

# operate and save data
import pandas as pd
import csv
import json

# download stock market data
from alpha_vantage.timeseries import TimeSeries

# send requests each X seconds
import time

#
import os

#
import warnings
warnings.filterwarnings('ignore')

key = '7WKARC4DTBTJVW54'

def remove_spaces(text_list):
    func = lambda x: x != ''
    return list(filter(func, text_list))

def get_sp500_data():
    # get general information about the components of S&P 500 index
    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    sp500_requested = requests.get(sp500_url).text
    
    sp500_soup = BeautifulSoup(sp500_requested, 'lxml')
    sp500_table = sp500_soup.find('table', {'class':'wikitable sortable'})
    sp500_tr = sp500_table.findAll('tr')
    
    # create csv file with following header
    columns = remove_spaces(sp500_tr[0].text.split('\n'))
    
    with open('data/sp500.csv', 'a', encoding='utf-8') as fp:
        # header of csv file
        writer = csv.writer(fp)
        writer.writerow(columns)  
        for row in sp500_tr[1:]:
            text_list = row.text.split('\n')
            info = remove_spaces(text_list)
            writer.writerow(info)
    print('Done!')
    
def get_earnings_calendar(tickers): 
    # check whether parameter passed is of right data type
    if not isinstance(tickers, (list, pd.Series)):
        tickers = pd.Series(tickers)
    
    for ticker in tickers:
        url = 'https://finance.yahoo.com/calendar/earnings?symbol='
        request = requests.get(url + ticker).text
        soup = BeautifulSoup(request, 'lxml')
        table = soup.find('tbody')
        if table == None:
            print(ticker + ' was not recognized')
            continue
        table_tr = table.findAll('tr')
        
        for row in table_tr:
            cols = row.find_all('td')
            cols = [x.text.strip() for x in cols]
            with open('data/historical_earnings_calendar/{}.csv'.format(ticker),
                      'a', encoding='utf-8') as fp:
                writer = csv.writer(fp)
                writer.writerow(cols)
        print(ticker + ' earnings calendar were downloaded!')
        
def get_daily_quotes(key, tickers, start = None, end = None, save = True):
    if not isinstance(tickers, (list, pd.Series)):
        tickers = pd.Series(tickers)
    
    for ticker in tickers:
        try:
            ts = TimeSeries(key, output_format='pandas')
            core, meta = ts.get_daily(symbol=ticker, outputsize='full')
            if start != None:
                if end != None:
                    core = core[start:end]
                else:
                    core = core[start:]         
            core.to_csv('data/historical_daily_quotes/{}.csv'.format(ticker))
            print(ticker + ' historical quotes were downloaded!')
        except:
            print(ticker + '  not found!')
            
def get_many_files(key, tickers, start = None, end = None, save = True):
    n = 5 
    chunks_of_tickers = [tickers[i:i+n] for i in range(0, len(tickers), n)]
    
    counter = 1
    for chunk in chunks_of_tickers:
        get_daily_quotes(key, chunk, start, end, save)
        print('Chunk ', counter, ' was downloaded!')
        counter += 1
        time.sleep(65)
        
def get_jsonparsed_data(url):
    """
    Receive the content of ``url``, parse it as JSON and return the object.

    Parameters
    ----------
    url : str

    Returns
    -------
    dict
    """
    response = requests.get(url).text
    return json.loads(response)

def get_fundamentals_quarterly(tickers, fundamentals):
    if not isinstance(tickers, (list, pd.Series)):
        tickers = pd.Series(tickers)
    
    if not isinstance(fundamentals, dict):
        return
    
    for metrics, address in fundamentals.items():
        try:
            os.makedirs("data/fundamentals/" + metrics)
        except FileExistsError:
            # directory already exists
            pass
    
        for ticker in tickers:
            print(ticker)
            try:
                url_base = 'https://financialmodelingprep.com/api/v3/'
                url_var = url_base + '{0}/{1}?period=quarter'.format(address, ticker)
                data_js = get_jsonparsed_data(url_var)
                if not data_js:
                    continue
                _, col = data_js.keys()
                data = pd.DataFrame(data_js[col])
                data.to_csv('data/fundamentals/{0}/{1}.csv'.format(metrics, ticker))
            except:
                print('Error on ' + ticker)
                continue
        print(metrics + ' data has been downloaded successfully!')
        
def find_urls():
    """
    
    """
    url = 'https://www.investing.com/indices/investing.com-us-500-components'
    #
    headers={"User-Agent": "Mozilla/5.0"}
    request = requests.get(url, headers=headers).text
    soup = BeautifulSoup(request, 'lxml')
    table = soup.find('tbody')
    
    urls = []
    for a in table.find_all('a', href=True):
        urls.append(a['href'])
    
    return urls
    
def get_estimates(urls):
    """
    
    """
    try:
        os.makedirs("data/estimates/")
    except FileExistsError:
        # directory already exists
        pass
    
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    driver = webdriver.Chrome("chromedriver.exe", 
                              chrome_options=options)
    button_xpath = '//*[@id="showMoreEarningsHistory"]/a'
    for url in urls:
        driver.get('https://www.investing.com{}-earnings'.format(url))
        button = driver.find_element_by_xpath(button_xpath)
        
        driver.execute_script("arguments[0].click();", button)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", button)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", button)
        time.sleep(1)
        
        soup = BeautifulSoup(driver.page_source)
        title = soup.find('h1', {'class':'float_lang_base_1 relativeAttr'}).text
        ticker = title[title.find("(")+1:title.find(")")]
         
        # there are multiple tables on a page, but we are interested in 1st
        class_name = 'genTbl openTbl ecoCalTbl earnings earningsPageTbl'
        table = soup.findAll('table', {'class': class_name})
        table_rows = table[0].findAll('tr')
        
        result = pd.DataFrame(columns=['Release Date', 'Period End',
                                       'EPS', 'Forecast', 'Revenue', 
                                       'Forecast'])
        for row in table_rows[1:]:
            text_list = row.text.split('\n')
            substr = '/\xa0\xa0'
            chars = len(substr)
            new_row = []
            for el in text_list:
                if el == '':
                    continue
                if substr in el:
                    el = el[chars:]
                new_row.append(el)
            result.loc[len(result),:] = new_row
        
        result.to_csv('data/estimates/{}.csv'.format(ticker))
        print(url)
        print(ticker + ' data has been downloaded!')
        
