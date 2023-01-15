import requests
import json
import re

key = '6c70526b4f67773537355357454f4b'

with open('festival.json', 'r', encoding='UTF8') as f:
    data = json.load(f)

culture = data['culture']
# place = list(map(lambda x: re.split('\W+', x['place']), data['culture']))

place = list(map(lambda x: re.split('/ |, | → | ~ ', x['place'].replace('및', ',')), data['culture']))
print(place)

for i in range(0, len(place)) :
    coord = []
    for j in range(0, len(place[i])):

        location = place[i][j]
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
        coord.append([y, x]) # 위도, 경도
    data['culture'][i]['place'] = place[i]
    data['culture'][i]['coord'] = coord

print(data['culture'])

with open('festival_loc.json', 'w') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
