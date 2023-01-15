import requests 
from bs4 import BeautifulSoup as bs
import pandas as pd
import csv
import urllib.request
import urllib.parse
from urllib.request import urlopen, Request
import re
import time
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


# 크롬창(웹드라이버) 열기
service = Service("C:\workspaces\workspace_crawling\drivers\chromedriver")
driver = webdriver.Chrome(service=service)

# 관광명소 namelist 가져오기
df = pd.read_csv(r'C:\workspaces\seoul_attractions_NameList.csv')
nameList = df['Name']
#print(nameList[0:10])

name = []
point = []

# 구글 맵 검색
for index in nameList:
    driver.get(f"https://www.google.co.kr/maps/search/{index}/data=!3m1!4b1?hl=ko")
    time.sleep(0.3)

    name.append(index)

    check = input('일치하는 가게를 클릭 후 y를 눌러주세요(이외 멈춤) : ')
    if check.upper() == 'Y':
        now_url = driver.current_url
    else:
        break
    time.sleep(1)

    try:
        google_point = driver.find_element(by=By.CSS_SELECTOR, value='div.F7nice.mmu3tf').text
    except:
        google_point = ''
    point.append(google_point)

data = {
    'Name': name,
    'Point': point
}

df = pd.DataFrame(data)
print(df)

df.to_csv(r'C:\workspaces\seoul_attractions_NameList_Point.csv', encoding='utf-8-sig')
