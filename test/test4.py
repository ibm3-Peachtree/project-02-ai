import json
import re

from openai import AsyncOpenAI
import asyncio

KANANA_MODEL_01_URL = "http://127.0.0.1:8001/v1"
KANANA_MODEL_02_URL = "http://127.0.0.1:8002/v1"
# kanana_client = AsyncOpenAI(base_url=KANANA_MODEL_01_URL, api_key="fake-key")
kanana_client = AsyncOpenAI(base_url=KANANA_MODEL_02_URL, api_key="fake-key")

def extract_json(raw_text):
    match = re.search(r'\{.*\}', raw_text, re.DOTALL)
    if match:
        json_string = match.group(0)
        json_string = json_string.replace(r"\'", "'")
        # 모델이 None을 뱉든 null을 뱉든 안전하게 null로 통일하여 json.loads 에러 방지
        json_string = re.sub(r':\s*None', ': null', json_string)
        json_string = re.sub(r':\s*null', ': null', json_string)
        json_string = re.sub(r':\s*True', ': true', json_string)
        json_string = re.sub(r':\s*False', ': false', json_string)
        return json_string
    return None

async def ask_kanana():

    # 2. Kanana 모델에게 질문 던지기
    # vLLM 서버에 로드된 모델명을 정확히 입력해야 합니다.
    user_xy_routes = [
    {
        "stationName": None,
        "x": None,
        "y": None,
        "arsID": None,
        "type": "walk",
        "no": "walk"
    },
    {
        "stationName": "성대시장",
        "x": 126.931594,
        "y": 37.500176,
        "arsID": "20148",
        "type": "bus",
        "no": "bus:6515"
    },
    {
        "stationName": "상도초등학교입구",
        "x": 126.936729,
        "y": 37.503321,
        "arsID": "20149",
        "type": "bus",
        "no": "bus:6515"
    },
    {
        "stationName": "청화병원",
        "x": 126.940078,
        "y": 37.507244,
        "arsID": "20193",
        "type": "bus",
        "no": "bus:6515"
    },
    {
        "stationName": "동작구청.노량진초등학교앞",
        "x": 126.940327,
        "y": 37.510418,
        "arsID": "20191",
        "type": "bus",
        "no": "bus:6515"
    },
    {
        "stationName": "노량진수산시장.CTS기독교TV",
        "x": 126.938949,
        "y": 37.513505,
        "arsID": "20010",
        "type": "bus",
        "no": "bus:6515"
    },
    {
        "stationName": None,
        "x": None,
        "y": None,
        "arsID": None,
        "type": "walk",
        "no": "walk"
    },
    {
        "stationName": "노량진",
        "x": 126.941659,
        "y": 37.514206,
        "arsID": None,
        "type": "subway",
        "no": "subway:수도권 1호선"
    },
    {
        "stationName": "용산",
        "x": 126.964428,
        "y": 37.529679,
        "arsID": None,
        "type": "subway",
        "no": "subway:수도권 1호선"
    },
    {
        "stationName": "남영",
        "x": 126.971316,
        "y": 37.540606,
        "arsID": None,
        "type": "subway",
        "no": "subway:수도권 1호선"
    },
    {
        "stationName": "서울역",
        "x": 126.972317,
        "y": 37.555946,
        "arsID": None,
        "type": "subway",
        "no": "subway:수도권 1호선"
    },
    {
        "stationName": "시청",
        "x": 126.97714,
        "y": 37.565366,
        "arsID": None,
        "type": "subway",
        "no": "subway:수도권 1호선"
    },
    {
        "stationName": "종각",
        "x": 126.983197,
        "y": 37.570176,
        "arsID": None,
        "type": "subway",
        "no": "subway:수도권 1호선"
    },
    {
        "stationName": None,
        "x": None,
        "y": None,
        "arsID": None,
        "type": "walk",
        "no": "walk"
    }
    ]

    current_gps = [
        {"accuracy": 7.39300012588501, "latitude": 37.5007444, "longitude": 126.9324223, "speed": 7.5040364265441895, "type": None},
        {"accuracy": 8.375, "latitude": 37.5009829, "longitude": 126.9328071, "speed": 7.187917232513428, "type": None},
        {"accuracy": 3.7899999618530273, "latitude": 37.5012678, "longitude": 126.9332268, "speed": 8.522705078125, "type": None},
        {"accuracy": 6.125, "latitude": 37.5018415, "longitude": 126.9340983, "speed": 11.299799919128418, "type": None},
        {"accuracy": 5.560999870300293, "latitude": 37.5021236, "longitude": 126.9345973, "speed": 10.332758903503418, "type": None},
        {"accuracy": 5.689000129699707, "latitude": 37.50245, "longitude": 126.935048, "speed": 10.457072257995605, "type": None},
        {"accuracy": 5.699999809265137, "latitude": 37.5027439, "longitude": 126.9355105, "speed": 9.857364654541016, "type": None},
        {"accuracy": 5.585999965667725, "latitude": 37.5029933, "longitude": 126.9359129, "speed": 8.618464469909668, "type": None},
        {"accuracy": 5.546000003814697, "latitude": 37.503135, "longitude": 126.9361436, "speed": 0.6104251742362976, "type": None},
        {"accuracy": 4.304999828338623, "latitude": 37.5032333, "longitude": 126.9362348, "speed": 3.4776737689971924, "type": None},
        {"accuracy": 18.545000076293945, "latitude": 37.5033015, "longitude": 126.9363533, "speed": 3.334347724914551, "type": None},
        {"accuracy": 4.789999961853027, "latitude": 37.5035442, "longitude": 126.9369053, "speed": 6.589717388153076, "type": None},
        {"accuracy": 7.747000217437744, "latitude": 37.5036655, "longitude": 126.9370653, "speed": 0.35632944107055664, "type": None},
        {"accuracy": 20.989999771118164, "latitude": 37.5037135, "longitude": 126.937019, "speed": 0.38732489943504333, "type": None},
        {"accuracy": 6.574999809265137, "latitude": 37.5037037, "longitude": 126.9370886, "speed": 1.1258610486984253, "type": None},
        {"accuracy": 13.38700008392334, "latitude": 37.5037963, "longitude": 126.9372548, "speed": 4.6411919593811035, "type": None},
        {"accuracy": 4.465000152587891, "latitude": 37.5039741, "longitude": 126.9375517, "speed": 7.9334235191345215, "type": None},
        {"accuracy": 4.625, "latitude": 37.5041834, "longitude": 126.9378808, "speed": 6.07911491394043, "type": None},
        {"accuracy": 5.866000175476074, "latitude": 37.5043147, "longitude": 126.9380831, "speed": 2.967027187347412, "type": None},
        {"accuracy": 6.796000003814697, "latitude": 37.5043918, "longitude": 126.9381814, "speed": 1.24944269657135, "type": None},
        {"accuracy": 9.61400032043457, "latitude": 37.5043544, "longitude": 126.938253, "speed": 0.2667718231678009, "type": None},
        {"accuracy": 7.394999980926514, "latitude": 37.5046823, "longitude": 126.9385882, "speed": 6.2579426765441895, "type": None},
        {"accuracy": 7.933000087738037, "latitude": 37.5051533, "longitude": 126.9393675, "speed": 8.581695556640625, "type": None},
        {"accuracy": 10.489999771118164, "latitude": 37.5054146, "longitude": 126.9395003, "speed": 5.701005935668945, "type": None},
        {"accuracy": 11.416999816894531, "latitude": 37.5058701, "longitude": 126.9395611, "speed": 9.569999694824219, "type": None},
        {"accuracy": 8.765999794006348, "latitude": 37.5066094, "longitude": 126.9398025, "speed": 8.40999984741211, "type": None},
        {"accuracy": 5.6539998054504395, "latitude": 37.5068941, "longitude": 126.939888, "speed": 4.809999942779541, "type": None},
        {"accuracy": 4.176000118255615, "latitude": 37.5069793, "longitude": 126.939923, "speed": 0.0, "type": None},
        {"accuracy": 16.909000396728516, "latitude": 37.5070702, "longitude": 126.9399438, "speed": 0.0, "type": None},
        {"accuracy": 5.591000080108643, "latitude": 37.5072144, "longitude": 126.9399648, "speed": 0.0, "type": None},
        {"accuracy": 4.415999889373779, "latitude": 37.5073428, "longitude": 126.9399645, "speed": 3.880000114440918, "type": None},
        {"accuracy": 5.482999801635742, "latitude": 37.5075121, "longitude": 126.9399762, "speed": 3.799999952316284, "type": None},
        {"accuracy": 7.9710001945495605, "latitude": 37.5076323, "longitude": 126.9399758, "speed": 1.7999999523162842, "type": None},
        {"accuracy": 7.34499979019165, "latitude": 37.5076772, "longitude": 126.9399671, "speed": 0.3799999952316284, "type": None},
        {"accuracy": 6.551000118255615, "latitude": 37.5078908, "longitude": 126.9399392, "speed": 5.690000057220459, "type": None},
        {"accuracy": 14.92300033569336, "latitude": 37.5082713, "longitude": 126.9401043, "speed": 9.15999984741211, "type": None},
        {"accuracy": 5.71999979019165, "latitude": 37.5087805, "longitude": 126.9402096, "speed": 11.3100004196167, "type": None},
        {"accuracy": 5.686999797821045, "latitude": 37.5092435, "longitude": 126.940216, "speed": 10.630000114440918, "type": None},
        {"accuracy": 6.420000076293945, "latitude": 37.5097096, "longitude": 126.9402512, "speed": 6.78000020980835, "type": None},
        {"accuracy": 5.880000114440918, "latitude": 37.5098923, "longitude": 126.9402221, "speed": 1.4700000286102295, "type": None}
        ]
    incident = {"si":"서울특별시","gu":"동작구","info":"지하철 1호선 용산역 화재로 인한 1호선 전면 통제","start":"2026-06-05 09:18:00","end":"2026-06-05 20:30:00","lat":37.529679,"lng":126.964428,"created_at":"2026-06-05 03:20:06"}

    kanana_client = AsyncOpenAI(base_url=KANANA_MODEL_02_URL, api_key="fake-key")
    
    system_instruction = """
        당신은 유저의 실시간 gps 위치 로그를 분석하여, 돌발 상황 발생 시 유저가 실제로 우회 경로를 받아보고 행동할 수 있는 미래의 '최적 우회 시작 정거장'을 예측하는 지도 전문가입니다.
        
        latitude, lat, y는 위도 / longitude, lng, x는 경도입니다.
        
        [필수 추론 단계]
        step1. 현재 위치 파악 (최신 데이터 기준):
        - current_gps는 과거부터 현재까지의 이동 로그 배열입니다.
        - '배열의 가장 마지막(최전) 요소'가 유저의 현재 실시간 위치입니다. 중간 데이터를 현재 위치로 착각하지 마세요.
        - 현재 유저의 최신 위치(마지막 로그)와 이동 속도(speed)를 확인하고, user_xy_routes에서 이미 지나온 정거장들은 후보에서 제외하세요.
        
        step2. 타임 버퍼 및 미래 위치 예측 (핵심):
        - LLM이 답변을 생성하고, Neo4j GraphDB가 새로운 우회 경로를 계산하여 유저의 스마트폰에 화면이 뜨기까지 최소 5초~10초의 '연산 시간(Time Buffer)'이 소요됩니다.
        - 유저가 현재 버스(speed 약 5~11m/s)를 타고 이동 중이므로, 계산이 끝나는 10초 뒤에 유저는 현재 위치보다 최소 50m~100m 이상 전진해 있습니다.
        - 따라서, 현재 위치와 너무 인접하거나 이미 지나치고 있는 정거장은 절대로 시작점(start_node)이 될 수 없습니다. 유저가 화면을 볼 때쯤엔 이미 지나쳐버리기 때문입니다.
        
        step3. 우회 시작점(start_node) 선정:
        - 현재 최신 GPS 위치보다 앞서 있으면서, 연산 버퍼 시간(10초) 동안 버스가 이동할 거리를 고려했을 때 유저가 '여유롭게 도달할 수 있는 다음 정거장 또는 다다음 정거장'을 user_xy_routes에서 찾아 시작점으로 선택하세요.
        - 추론한 구체적인 연산 시간과 예상 이동 거리를 논리적으로 명시하여 reason에 작성하세요.
        
        arsID가 존재하는 경우 그대로 넣어주고, 없는 경우 null로 표현합니다.
        
        반드시 다른 설명 없는 '순수 JSON' 형식으로만 답하세요. 
        값의 유무를 표현할 때는 파이썬 스타일의 None 대신 반드시 JSON 표준인 소문자 null을 사용하세요.
        
        정확한 JSON 예시 (참고용 논리 구조이며, 실제 정거장 이름은 전혀 다릅니다) : 
        {
            "reason" : "최신 GPS 로그 기준으로 유저의 위도가 이미 가상의 A정거장을 지나 B정거장 직전까지 접근한 상태입니다. 시스템의 우회 경로 연산 소요 시간(약 8초) 및 버스의 현재 주행 속도(초속 9m)를 계산하면, 유저가 스마트폰 화면으로 우회 경로를 확인하는 시점에는 버스가 이미 B정거장을 통과하고 있거나 지나친 상태가 됩니다. 따라서 유저가 인지 장애 없이 안전하게 하차 및 우회 결정을 내릴 수 있도록, 심리적·물리적 시공간 여유가 확보되는 다음 정거장인 'C정거장'을 우회 시작점으로 선정하는 것이 타당합니다.",
            "start_node" : {
                "stationName": "강남역서초현대아파트",
                "x": 127.024512,
                "y": 37.494102,
                "arsID": "22104",
                "type": "bus",
                "no": "bus:740"
            }
        }
    """

    response = await kanana_client.chat.completions.create(
        model="kakaocorp/kanana-1.5-8b-instruct-2505",
        messages=[
            {
                "role" : "system",
                "content" : system_instruction
            },
            {
                "role" : "user",
                "content" : f"current_gps : {current_gps}\nuser_xy_routes : {user_xy_routes}\nincident: {incident} "
            }
        ],
        max_tokens=3000,
        temperature=0.1, # 답변의 일관성을 위해 0.2~0.3 유지 권장
    )

    # 3. 답변 출력
    # print("\n[Kanana 1.5 8B 답변]:")
    outputs = json.loads(extract_json(response.choices[0].message.content))
    is_not_last_node = True
    idx = -1
    while is_not_last_node :
        if user_xy_routes[idx]["x"] and user_xy_routes[idx]["y"] :
            is_not_last_node = True
            outputs["end_node"] = {
                "stationName": user_xy_routes[idx]["stationName"],
                "x": user_xy_routes[idx]["x"],
                "y": user_xy_routes[idx]["y"],
                "arsID": user_xy_routes[idx]["arsID"],
                "type": user_xy_routes[idx]["type"],
                "no": user_xy_routes[idx]["no"]
            }
            is_not_last_node = False
        idx -= 1
    print(outputs)

if __name__ == "__main__":
    asyncio.run(ask_kanana())