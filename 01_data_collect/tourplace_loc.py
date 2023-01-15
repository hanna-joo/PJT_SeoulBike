import pandas as pd
import requests
import json

df = pd.read_csv(r'seoul_attractions_NameList_Point.csv')
nameList = df['Name']


list_x = []
list_y = []

# 데이터 개수만큼 반복하기 - dict에 붙이기
for i in range(0, len(nameList)) :
    location = nameList[i]
    url = f"https://dapi.kakao.com/v2/local/search/keyword.json?query={location}"
    kakao_key = "eeb4d25bd0990160503da341e8678475"
    result = requests.get(url, headers={"Authorization": f"KakaoAK {kakao_key}"})
    json_obj = result.json()
    # x : 경도 / y : 위도
    try :
        x = json_obj['documents'][0]['x']
        y = json_obj['documents'][0]['y']
    except :
        x = 0
        y = 0
    # print(x, y)
    list_x.append(y)
    list_y.append(x)
    # df['위도'] = y
    # df['경도'] = x
df['위도'] = list_x
df['경도'] = list_y

df.to_csv('tourplace_loc.csv', header=False, index=False, encoding='utf-8-sig')



