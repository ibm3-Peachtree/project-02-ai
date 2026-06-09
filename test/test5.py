from dotenv import load_dotenv
import os
import requests
from pprint import pprint

load_dotenv()
keyword = "한국은행 앞 교차로"

url = "https://dapi.kakao.com/v2/local/search/keyword.json"
headers = {
    "Authorization" : f"KakaoAK {os.getenv("KAKAO_RESTAPI")}"
}
params = {
    "query" : keyword,
}

try : 
    response = requests.get(url, headers=headers, params=params)
    
    response.raise_for_status()
    
    data = response.json()
    pprint(data)
    
    output = None
    for doro in data['documents'] :
        if '교통,수송' in doro['category_name'] :
            output = doro
            break
    pprint(output)
    
except requests.exceptions.HTTPError as e:
    print(f"HTTP 에러 발생: {e}")
except Exception as e:
    print(f"기타 에러 발생: {e}")