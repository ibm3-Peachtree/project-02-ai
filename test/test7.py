import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from openai import AsyncOpenAI
import asyncio
from pprint import pprint

KANANA_MODEL_01_URL = "http://127.0.0.1:8001/v1"
KANANA_MODEL_02_URL = "http://127.0.0.1:8002/v1"
# kanana_client = AsyncOpenAI(base_url=KANANA_MODEL_01_URL, api_key="fake-key")
kanana_client = AsyncOpenAI(base_url=KANANA_MODEL_02_URL, api_key="fake-key")

def extract_json_array(raw_text):
    # [ 로 시작해서 ] 로 끝나는 가장 긴 구간을 찾습니다. (점진적 매칭)
    match = re.search(r'\[\s*\{.*\}\s*\]', raw_text, re.DOTALL)
    
    if match:
        json_string = match.group(0) # 매칭된 [ { ... } ] 부분만 추출
        return json_string
    else:
        return None

async def ask_kanana(data):

    # 2. Kanana 모델에게 질문 던지기
    # vLLM 서버에 로드된 모델명을 정확히 입력해야 합니다.
    raw_data = data

    kanana_client = AsyncOpenAI(base_url=KANANA_MODEL_02_URL, api_key="fake-key")
    current_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d : %H:%M:%S")
    
    # 프롬프트를 개별 분리하도록 명확하게 다듬었습니다.
    system_instruction = f"""
        당신은 대한민국 교통 요금 정산 시스템엔진입니다.
        이전 대화나 이전 데이터의 결과는 완전히 잊으십시오. 
        오직 '현재 새로 입력된 데이터'만 독립적으로 보고 요금을 정산해야 합니다.
        
        [정산 연산 필수 단계]
        입력 데이터 리스트 내부에 존재하는 **모든 경로 객체**들을 순서대로 하나씩 모두 순회하며, 각 객체별로 아래 단계를 예외 없이 독립적으로 수행하십시오. 
        절대 다른 경로 객체의 데이터를 섞거나 앞선 결과를 복사하여 재사용하지 마십시오.
        
        STEP 1. [현재 객체의 대중교통 거리 합산]:
          - 현재 처리 중인 객체 내부의 "path_segments"만 확인하십시오.
          - "type": "walk" 또는 "type": "TRANSFER" 인 세그먼트는 무조건 0m 처리합니다.
          - "type": "TRANSIT" 인 세그먼트의 "total_distance_m" 값만 '현재 객체 안에 명시되어 있는 것만' 모두 찾아 더하여 [최종 누적 거리]를 구하십시오.
          - 절대 다른 객체의 주행 거리나 가상의 데이터를 상상해서 더하지 마십시오. 눈앞에 적힌 숫자만 더해야 합니다.

        STEP 2. [거리 구간별 요금 테이블 매핑]:
          - STEP 1에서 합산된 [최종 누적 거리]가 어떤 구간에 속하는지 보고 요금을 매핑하십시오.
          
        -------------------------------------------------------------
        [최종 누적 거리 범위]                 | [최종 부과 요금]
        -------------------------------------------------------------
        - 0m ~ 10,000m (10km 이하)           | 1,550원
        - 10,001m ~ 15,000m (10km ~ 15km)    | 1,650원
        - 15,001m ~ 20,000m (15km ~ 20km)    | 1,750원
        - 20,001m ~ 25,000m (20km ~ 25km)    | 1,850원
        -------------------------------------------------------------

        [출력 절대 규칙]
        - 다른 설명이나 인사말, 마크다운 기호(```json)를 절대 붙이지 말고 오직 유효한 순수 JSON 리스트(`[...]`) 구조만 출력하십시오.
        
        [JSON 필드 규격]
        - "path_idx" : 현재 분석 중인 경로 객체의 "path_id" 값을 정수형 숫자로 그대로 매핑하십시오.
        - "reason" : 현재 경로 객체에서 STEP 1의 거리 식을 구하고 이를 통해 계산된 실제 합산 거리(m)를 명시하고, 이것이 위 요금 테이블의 어느 구간에 매핑되는지 간결하게 작성하세요.
        - "cost" : "reason"에서 도출된 최종 정산 요금 (정수형 INT)
        
        정확한 출력 예시:
        [
          {{
            "path_idx": 0,
            "reason" : "1. paths_segments의 type이 TRANSIT인 세그먼트 거리 : 2호선(9000.00m) + 4호선(6001.52m) = 15001.52m
              2. TRANSIT 세그먼트들의 거리를 합산한 결과 총 15,001.52m로 15km~20km 구간에 해당하여 1,750원이 책정됩니다.",
            "cost" : 1750
          }},
          {{
            "path_idx": 1,
            "reason" : "1. paths_segments의 type이 TRANSIT인 세그먼트거리 : 5호선 (5000.00m) = 5000.00m
              2. TRANSIT 세그먼트들의 거리를 합산한 결과 총 5,000.00m로 10km 이하 기본요금 구간에 해당하여 1,550원이 책정됩니다.",
            "cost" : 1550
          }}
        ]
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
                "content" : f"사용자 추천 경로 : {raw_data}"
            }
        ],
        max_tokens=3000,
        temperature=0.1, # 답변의 일관성을 위해 0.2~0.3 유지 권장
    )

    # 3. 답변 출력
    print("\n[Kanana 1.5 8B 답변]:")
    # print(response.choices[0].message.content.split(','))
    pprint(response.choices[0].message.content)
    pprint(json.loads(extract_json_array(response.choices[0].message.content)))
        # print(type(response.choices[0].message.content))

    # except Exception as e:
    #     print(f"에러가 발생했습니다: {e}")


# 4. 비동기 함수 실행
if __name__ == "__main__":
    data01 = [
      {
        "path_id": 0,
        "total_duration_min": 44.6,
        "transfer_count": 1,
        "path_segments": [
          {
            "type": "TRANSFER",
            "display_name": [
              "도보"
            ],
            "segment_duration_min": 4.6,
            "total_distance_m": 338,
            "stop_count": 0,
            "stations": [
              {
                "name": "지하철2호선강남역(중)",
                "x": 127.0257167,
                "y": 37.5017,
                "ars_id": "22011"
              },
              {
                "name": "신논현",
                "x": 127.02506,
                "y": 37.504598,
                "ars_id": None
              }
            ]
          },
          {
            "type": "TRANSIT",
            "display_name": [
              "서울 지하철 9호선"
            ],
            "segment_duration_min": 20.5,
            "total_distance_m": 9623.83,
            "stop_count": 10,
            "stations": [
              {
                "name": "신논현",
                "x": 127.02506,
                "y": 37.504598,
                "ars_id": None
              },
              {
                "name": "사평 (9호선)",
                "x": 127.015259,
                "y": 37.504206,
                "ars_id": None
              },
              {
                "name": "고속터미널 (9호선)",
                "x": 127.004403,
                "y": 37.50598,
                "ars_id": None
              },
              {
                "name": "신반포 (9호선)",
                "x": 126.995925,
                "y": 37.503415,
                "ars_id": None
              },
              {
                "name": "구반포 (9호선)",
                "x": 126.987332,
                "y": 37.501364,
                "ars_id": None
              },
              {
                "name": "동작 (9호선)",
                "x": 126.978153,
                "y": 37.502878,
                "ars_id": None
              },
              {
                "name": "흑석 (9호선)",
                "x": 126.963708,
                "y": 37.50877,
                "ars_id": None
              },
              {
                "name": "노들 (9호선)",
                "x": 126.953222,
                "y": 37.512887,
                "ars_id": None
              },
              {
                "name": "노량진 (9호선)",
                "x": 126.941005,
                "y": 37.513534,
                "ars_id": None
              },
              {
                "name": "샛강 (9호선)",
                "x": 126.928422,
                "y": 37.517274,
                "ars_id": None
              },
              {
                "name": "여의도 (9호선)",
                "x": 126.92403,
                "y": 37.52176,
                "ars_id": None
              },
              {
                "name": "여의도",
                "x": 126.92403,
                "y": 37.52176,
                "ars_id": None
              }
            ]
          },
          {
            "type": "TRANSFER",
            "display_name": [
              "도보"
            ],
            "segment_duration_min": 2.5,
            "total_distance_m": 180,
            "stop_count": 0,
            "stations": [
              {
                "name": "여의도",
                "x": 126.92403,
                "y": 37.52176,
                "ars_id": None
              }
            ]
          },
          {
            "type": "TRANSIT",
            "display_name": [
              "수도권 전철 5호선"
            ],
            "segment_duration_min": 17.0,
            "total_distance_m": 7349.6900000000005,
            "stop_count": 7,
            "stations": [
              {
                "name": "여의도",
                "x": 126.924357,
                "y": 37.521747,
                "ars_id": None
              },
              {
                "name": "여의나루 (5호선)",
                "x": 126.932901,
                "y": 37.527098,
                "ars_id": None
              },
              {
                "name": "마포 (5호선)",
                "x": 126.945932,
                "y": 37.539574,
                "ars_id": None
              },
              {
                "name": "공덕 (5호선)",
                "x": 126.951372,
                "y": 37.544431,
                "ars_id": None
              },
              {
                "name": "애오개 (5호선)",
                "x": 126.95682,
                "y": 37.553736,
                "ars_id": None
              },
              {
                "name": "충정로 (5호선)",
                "x": 126.9629,
                "y": 37.560236,
                "ars_id": None
              },
              {
                "name": "서대문 (5호선)",
                "x": 126.966641,
                "y": 37.565773,
                "ars_id": None
              },
              {
                "name": "광화문 (5호선)",
                "x": 126.97717,
                "y": 37.571525,
                "ars_id": None
              },
              {
                "name": "광화문",
                "x": 126.97717,
                "y": 37.571525,
                "ars_id": None
              }
            ]
          }
        ]
      },
      {
        "path_id": 1,
        "total_duration_min": 44.6,
        "transfer_count": 1,
        "path_segments": [
          {
            "type": "TRANSFER",
            "display_name": [
              "도보"
            ],
            "segment_duration_min": 4.6,
            "total_distance_m": 338,
            "stop_count": 0,
            "stations": [
              {
                "name": "지하철2호선강남역(중)",
                "x": 127.0257167,
                "y": 37.5017,
                "ars_id": "22011"
              },
              {
                "name": "신논현",
                "x": 127.02506,
                "y": 37.504598,
                "ars_id": None
              }
            ]
          },
          {
            "type": "TRANSIT",
            "display_name": [
              "서울 지하철 9호선"
            ],
            "segment_duration_min": 20.5,
            "total_distance_m": 9623.83,
            "stop_count": 10,
            "stations": [
              {
                "name": "신논현",
                "x": 127.02506,
                "y": 37.504598,
                "ars_id": None
              },
              {
                "name": "사평 (9호선)",
                "x": 127.015259,
                "y": 37.504206,
                "ars_id": None
              },
              {
                "name": "고속터미널 (9호선)",
                "x": 127.004403,
                "y": 37.50598,
                "ars_id": None
              },
              {
                "name": "신반포 (9호선)",
                "x": 126.995925,
                "y": 37.503415,
                "ars_id": None
              },
              {
                "name": "구반포 (9호선)",
                "x": 126.987332,
                "y": 37.501364,
                "ars_id": None
              },
              {
                "name": "동작 (9호선)",
                "x": 126.978153,
                "y": 37.502878,
                "ars_id": None
              },
              {
                "name": "흑석 (9호선)",
                "x": 126.963708,
                "y": 37.50877,
                "ars_id": None
              },
              {
                "name": "노들 (9호선)",
                "x": 126.953222,
                "y": 37.512887,
                "ars_id": None
              },
              {
                "name": "노량진 (9호선)",
                "x": 126.941005,
                "y": 37.513534,
                "ars_id": None
              },
              {
                "name": "샛강 (9호선)",
                "x": 126.928422,
                "y": 37.517274,
                "ars_id": None
              },
              {
                "name": "여의도 (9호선)",
                "x": 126.92403,
                "y": 37.52176,
                "ars_id": None
              },
              {
                "name": "여의도",
                "x": 126.92403,
                "y": 37.52176,
                "ars_id": None
              }
            ]
          }
          
        ]
      }
    ]
    asyncio.run(ask_kanana(data01))