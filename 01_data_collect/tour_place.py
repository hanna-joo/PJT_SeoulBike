import requests 
from bs4 import BeautifulSoup as bs
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import csv
import urllib.request
import urllib.parse
from urllib.request import urlopen, Request
import re
import time
import os

name = []
point = []
review = []

for i in range(1, 36):
    url = 'https://korean.visitseoul.net/attractions?curPage=' + str(i)
    # 홈페이지 주소로 내용 가져오기
    req = Request(url, headers={'User-Agent': 'Chrome'})
    time.sleep(1)
    html = urlopen(req)
    attractions = bs(html, 'html.parser')

    #print(attractions.prettify())

    # 내용 추리기
    list_soup = attractions.find('div', {'class': 'article-list-slide'})
    list_soup_name = list_soup.find_all('span', {'class': 'title'})
    list_soup_point = list_soup.select('img')
    list_soup_review = list_soup.find_all('span', {'class': 'trip-text'})

    #print(list_soup_point)

    # 가게이름 추출
    for item_name in list_soup_name:
        name.append(item_name.text.strip())
    '''
    # 평점 추출
    for item_point in list_soup_point:
        row_point = item_point['alt'].split('평점:')
        point.append(row_point[1])

    # 리뷰 추출
    for item_review in list_soup_review:
        row_review = item_review.text.split('reviews')
        review.append(row_review[0])
    '''
    time.sleep(0.3)

#print(name)
#print(point)
#print(review)

# csv 저장
data = {
    'Name': name,
    #'Point': point,
    #'Review': review}
}
df = pd.DataFrame(data)

df.to_csv(r'C:\workspaces\seoul_attractions_NameList.csv', encoding='utf-8-sig')
