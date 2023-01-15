# place_station.py

# pip install sqlalchemy
# pip install pymysql
from pymongo import MongoClient
from sqlalchemy import create_engine
import pymysql
import numpy as np
import pandas as pd
import time

# mongo db
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client.pjt2

# mysql db
user_nm = 'root'
user_pw = '1234'
host_nm = 'localhost'
host_address = '3306'
db_nm = 'pjt2'
db_connection_path = f"mysql+pymysql://{user_nm}:{user_pw}@{host_nm}:{host_address}/{db_nm}"
db_connection = create_engine(db_connection_path, encoding='utf-8')
conn = db_connection.connect()

def mongo_load(entity_nm):
    entity = mongo_db[entity_nm]
    return entity

def mysql_load(table_nm):
    table = pd.read_sql_table(table_nm, con=conn)
    return table

def km_to_mile(km):
    mile = km*0.621371
    return float(mile)

# 장소별 근처 자전거대여소
def place_station(collection, kilometer) :
    mongo_bike_station = mongo_load('BIKE_STATION')
    mongo_place = mongo_db[collection]
    place_list = list(mongo_place.find())
    dist = km_to_mile(kilometer) / 3963.2
    # 담을 df 생성
    df = pd.DataFrame(columns = ['place_nm', 'station_id'])
    row_cnt = 0
    for i in range(len(place_list)):
        place = place_list[i]
        place_nm = place['place_nm']
        place_coord = place['location']['coordinates']
        stations = list(mongo_bike_station.find({'location': {'$geoWithin': {'$centerSphere': [place_coord, dist]}}}))
        for j in range(len(stations)):
            df.loc[row_cnt] = [place_nm, stations[j]['bike_station_id']]
            row_cnt += 1
    return df


if __name__ == "__main__":
    # 행사장소별 근처 자전거대여소
    event_place = mysql_load('EVENT_PLACE')
    event_station = place_station('EVENT_PLACE', 0.5)
    event_station = event_place.merge(event_station, on='place_nm').drop_duplicates(subset=['place_nm', 'station_id'])
    event_station.to_sql(name='EVENT_STATION', con=db_connection, if_exists='replace')

    # 관광명소별 근처 자전거대여소
    tour_place = mysql_load('TOUR_PLACE').drop('place_star', axis=1)
    tour_station = place_station('TOUR_PLACE', 0.5)
    tour_station = tour_place.merge(tour_station, on='place_nm').drop_duplicates(subset=['place_nm', 'station_id'])
    tour_station.to_sql(name='TOUR_STATION', con=db_connection, if_exists='replace')
    
    print(event_place)
    print(tour_station)
