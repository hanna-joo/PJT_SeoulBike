import requests
import json

key = '6c70526b4f67773537355357454f4b'

# 서울시 문화공간 정보 api
url_munwha = f'http://openapi.seoul.go.kr:8088/{key}/json/culturalSpaceInfo/1/5/'
# 전체 데이터 개수 확인
resp01 = requests.get(url_munwha)
r_dict01 = json.loads(resp01.text)
# print(r_dict01)
numOfRows_munwha = r_dict01['culturalSpaceInfo']['list_total_count']

# 전체 데이터 가져오기
url02 = f'http://openapi.seoul.go.kr:8088/{key}/json/culturalSpaceInfo/1/{numOfRows_munwha}/'
resp02 = requests.get(url02)
r_dict02 = json.loads(resp02.text)
# 잘 가져왔는지 확인
# print(r_dict02)
numResult = len(r_dict02['culturalSpaceInfo']['row'])
# print(numResult)
# print(r_dict02['culturalSpaceInfo']['row'][100])

# json으로 변환하여 파일로 저장
with open('univ_json.json', 'w') as f:
    json.dump(r_dict02, f, ensure_ascii=False, indent=4)