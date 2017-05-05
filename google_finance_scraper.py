# -*- coding: utf-8 -*-
"""
Created on Fri May  5 16:30:10 2017

@author: richie
"""

#Google Finance Scraper

import re
import requests
import pandas as pd
import pandas.io.data as web
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

url = "https://www.google.com/finance?q=NASDAQ%3A{symbol}&fstype=ii"

QUARTERLY = "quarterly"
ANNUAL = "annual"

def convert_row(row):
    numerical = []
    for i in range(0, len(row)):
        if row[i] == '-':
            numerical.append(0.0)
        else:
            numerical.append(float(row[i].replace(",","")))
    return numerical
    
def translate_index(period_type, index):

    translated_index = []
    
    for ix in index:
        parts = ix.split("-")
        year = int(parts[0])
        month = int(parts[1])
        if period_type == ANNUAL:
            if month > 6:
                translated_index.append(str(year))
            else:
                translated_index.append(str((year-1)))
        else:
            quarter = ""
            if month <= 3:
                quarter = "Q1"
            elif month <= 6:
                quarter = "Q2"
            elif month <= 9:
                quarter = "Q3"
            else:
                quarter = "Q4"
            quarter = "{year}{quarter}".format(year=str(year), quarter=quarter)
            translated_index.append(quarter)
    
    return translated_index
    
def read_financial_table(table):
    
    rows = table.findAll("tr")
    date_pattern = "[0-9]{4}-[0-9]{2}-[0-9]{2}"    
    indexes = []    
    
    if len(rows) > 1:
        headers = [header.getText() for header in rows[0].findAll("th")]
        indexes = [re.search(date_pattern, header).group(0) for header in headers[1:]]
        
    columns = []
    data = []
        
    for i in range(1, len(rows)):
        row_data = [col.getText() for col in rows[i].findAll("td")]
        
        if len(row_data) > 1:
            columns.append(row_data[0].replace("\n",""))
            field_data = convert_row(row_data[1:])
            data.append(field_data)
    
    df = pd.DataFrame(data, index=columns, columns=indexes)
    
    #swap rows and columns and sort chronologically
    rearranged_df = df.transpose().iloc[::-1]
    rearranged_df["Reporting Period"] = rearranged_df.index
    
    return rearranged_df

def read_financial_data(symbol, page):
    print("Reading " + symbol)    
    
    tables = page.findAll("table")   
    
    if len(tables) < 3:
        print("No financial data for " + symbol)
        return (None, None)
    
    quarterly_data = read_financial_table(tables[1])
    annual_data = read_financial_table(tables[2])    
    
    quarterly_data.index = translate_index(QUARTERLY, quarterly_data.index)
    annual_data.index = translate_index(ANNUAL, annual_data.index)    
    
    return (quarterly_data, annual_data)    


def load_page(symbol):
    formatted_url = url.format(symbol=symbol)
    page = requests.get(formatted_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup
    
def scrape_data():

    sp500 = pd.read_csv("sp500_symbols.csv", delimiter=",")    
    
    symbols = list(sp500["Symbol"])
            
    quarterly_reports = pd.DataFrame()
    annual_reports = pd.DataFrame()
    
    for symbol in symbols:
        page = load_page(symbol)
        (quarterly_data, annual_data) = read_financial_data(symbol, page)
        
        if quarterly_data is not None:
            quarterly_data["Symbol"] = symbol
            annual_data["Symbol"] = symbol
            
            quarterly_reports = pd.concat([quarterly_data, quarterly_reports])            
            annual_reports = pd.concat([annual_data, annual_reports])
               
    quarterly_reports.to_csv("GoogleData/QuarterlyReports.csv", delimiter=",")
    annual_reports.to_csv("GoogleData/AnnualReports.csv", delimiter=",")
