import pandas as pd  # file operations
from bs4 import BeautifulSoup as soups  #Scrapping tool
from urllib.request import urlopen as ureq # For requesting data from link
from urllib.error import HTTPError
from urllib.error import URLError
import numpy as np
import json
import os
import re
import pymongo
from dotenv import load_dotenv
load_dotenv()

import asyncio
import concurrent.futures

PRETEXT_URL = 'http://howstat.com/cricket/Statistics/Players/'
PLAYERLIST = 'PlayerList.asp?Group={}'

ODI_URL = 'PlayerOverview_ODI.asp'
T20_URL = 'PlayerOverview_T20.asp'
TEST_URL = 'PlayerOverview.asp'
IGNORE_VALUES = ['Name', 'Born', 'Detailed Profiles & Statistics', 'Country (Current)', 'Tests', 'T20s', 'ODIs']

def mapData(dataDist, index, value):
    if(index == 0):
        dataDist['Name'] = value
    elif(index == 1):
        dataDist['Date Of Birth'] = value
    elif(index == 2):
        dataDist['Country'] = value
    elif(index == 3):
        dataDist['Test'] = value
    elif(index == 4):
        dataDist['ODI'] = value
    elif(index == 5):
        dataDist['T20'] = value
    return dataDist

def textIsKey(dataDist, key, value):
    dataDist[key.replace(':', '')] = value
    return dataDist

def extractTdAndMapValues(dataDist, heading, tds):
    header = ''
    i = 0
    data = {}
    while(i < len(tds)):
        if(tds[i].text.strip() in heading):
            if(header != ''):
                dataDist[header] = data
                data = {}
            header = tds[i].text.strip()
            i+=1
        elif(tds[i].text.strip() != ''):
            textIsKey(data, tds[i].text.strip(), tds[i+1].text.strip())
            i+=2
        else:
            i+=1
    dataDist[header] = data
    return dataDist


def scrap(x):
    x = chr(x) #to change integer into character
    url = PRETEXT_URL+PLAYERLIST.format(x)
    try:
        print("\n")
        print(url)
        pagehtml = ureq(url)
    except HTTPError as e:
        print(e)
    except URLError as e:
        print("Website Can't be reached")
    else:
        soup = soups(pagehtml,"html.parser") #parse the html
        table = soup.find("table", { "class" : "TableLined" })
        if table is not None:
            #for x in table:
            rows = table.find_all('tr', attrs={"bgcolor" : ["#FFFFFF", "#E3FBE9"]}) #find all tr tag(rows)
            localCricketData = []
            for tr in rows:
                data=[]
                allLinks = tr.find_all('a', { "class" : "LinkNormal" })
                if(len(allLinks)):
                    allLinks = allLinks[1:]

                cols = tr.find_all('td') #find all td tags(columns)
                i = 0
                dataDist = {}
                for td in cols:
                    if(td.text.strip() not in IGNORE_VALUES and td.text.strip().find('No. of Records') == -1):
                        textValue = td.text.strip()
                        if(textValue == ''):
                            textValue = 0
                        dataDist= mapData(dataDist, i, textValue)
                        data.append(td.text.strip())
                        i+=1

                for link in allLinks:
                    dataTitle = ''
                    subDataDist = {}
                    subUrl = PRETEXT_URL+link.get('href')
                    if re.search(ODI_URL, subUrl):
                        dataTitle = 'ODI Records'
                    elif re.search(T20_URL, subUrl):
                        dataTitle = 'T20 Records'
                    elif re.search(TEST_URL, subUrl):
                        dataTitle = 'Test Records'
                        
                    try:
                        #print("\n")
                        #print(subUrl)
                        subPagehtml = ureq(subUrl)
                    except HTTPError as e:
                        print(e)
                    except URLError as e:
                        print("Website Can't be reached")
                    else:
                        subSoup = soups(subPagehtml,"lxml") #parse the html
                        subTables = subSoup.find('table', attrs={"width" : ["270"]})
                        content = subTables.find_all('td')
                        mainKey = []
                        headers = subTables.find_all('td', attrs={"colspan" : "2"})
                        for heading in headers:
                            if(heading.text.strip() != ''):
                                mainKey.append(heading.text.strip())

                        dataDist[dataTitle] = extractTdAndMapValues(subDataDist, mainKey, content)
                        #print(dataDist)

                if(bool(dataDist)):
                    localCricketData.append(dataDist)
            return localCricketData
    
async def addData():
    cricketData = []
    cosmosDB = os.getenv("CRICKETDATA_STRING")
    client = pymongo.MongoClient(cosmosDB)
    db = client["playerdb"]
    col = db["playerinformation"]
    col.drop()
    db = client["playerdb"]
    col = db["playerinformation"]

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=26)
    loop = asyncio.get_event_loop()
    inputs = list(range(65, 91, 1))
    futures = [loop.run_in_executor(executor, scrap, i) for i in inputs]
    results = await asyncio.gather(*futures)
    for (i, result) in zip(inputs, results):
        if result:
            #cricketData.append(result)
            col.insert_many(result)
    #with open("cricketData.json", "w") as outfile:  
        #json.dump(cricketData, outfile)