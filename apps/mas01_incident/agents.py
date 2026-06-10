# apps/mas01_incident/agents.py
from typing import TypedDict, Annotated, Sequence, Dict, List, Any
import json
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

from openai import AsyncOpenAI
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, ToolMessage
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
import operator

import config
from apps.mas01_incident.tools import resolve_address_point, resolve_between_nodes, resolve_linear_reference, publish_to_channel

class AgentState(TypedDict) :
    raw_incident_data : Dict[str, Any]
    extracted_entities : List[Dict[str, Any]]
    affected_nodes : List[Dict[str, Any]]

# LangGraph Node 함수 정의
async def extract_affected_node(state:AgentState) -> Dict[str, Any] :
    """
    교통 공지사항 정보로부터 엔터티(영향 받는 도로, 버스 노선, 버스 정류소, 지하철 노선, 지하철 역)추출
    엔터티 별 경도와 위도를 추출합니다.
    입력 : incident data
    출력 : 영향 받는 도로별 정보 
    """
    raw_data = state['raw_incident_data']
    config.logger.info(f"[MAS01 Agent : extract_affected] inputs : {raw_data}")
    
    raw_lat = raw_data.get("lat")
    raw_lng = raw_data.get("lng")
    
    if raw_lat and raw_lng:
        try:
            val_lat = float(raw_lat)
            val_lng = float(raw_lng)
            
            # 100이 넘는 값(124~132)이 lat(위도)에 들어와 있다면 명백한 오류이므로 자리를 바꿉니다.
            if val_lat > 100.0 and val_lng < 50.0:
                config.logger.warning(f"🔄 [Redis 축 전도 감지] lat과 lng가 뒤바뀌어 들어왔습니다. 강제 교정합니다. (입력 lat: {val_lat}, lng: {val_lng})")
                raw_data["lat"] = val_lng  # 37.52... 을 위도로
                raw_data["lng"] = val_lat  # 127.05... 을 경도로
            else:
                # 데이터가 정상적으로 들어왔을 때의 포맷팅
                raw_data["lat"] = val_lat
                raw_data["lng"] = val_lng
        except ValueError:
            pass # 숫자가 아닐 경우의 예외 방어
            
    config.logger.info(f"[MAS01 Agent : extract_affected] 보정 완료된 레디스 데이터 : {raw_data}")
    
    kanana_client = AsyncOpenAI(base_url=config.KANANA_MODEL_02_URL, api_key="fake-key")
    current_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d : %H:%M:%S")
    
    system_instruction = f"""
        [현재 시스템 기준 일시] : {current_time}
        
        당신은 대한민국 교통 및 지리 전문가입니다.
        사용자가 입력한 교통 공지사항 정보를 분석하여,오직 물리적으로 실질적으로 통제되는 공간 좌표 추출이 가능한 실제 인프라(도로 구간, 건물 지번 주소, 지하철 노선, 5자리 숫자의 버스 정류소 ID)만 '각각 하나의 독립된 항목'으로 완벽히 분리해야 합니다. 
        
        🚨 **[분석 대상 제외 절대 규칙]:**
        - 본문에 등장하는 3자리 또는 4자리 형태의 시내/광역 버스 노선 번호(예: 571, 5012 등) 및 버스 운수 회사 이름은 무시하십시오.
        - 오직 공사, 행사, 사고 등으로 인해 **차량 통행이 물리적으로 완전히 '막히거나 차단된' 실제 통제 구간/지점만** 추출 대상입니다.
        
        🚨 **[통제도로 vs 우회도로 판별 논리 프레임 - 필독]:**
        당신은 장소를 추출하기 전, 본문 문맥에서 해당 장소가 다음 중 어느 쪽에 해당하는지 **반드시 검증**해야 합니다.
        1. 🔴 **통제 대상 (추출 대상 O):**
           - 사고, 공사, 행사로 인해 차량 진입이 '막힘', '통제됨', '차단됨', '금지됨'의 대상이 된 원래의 도로 및 구간 (예: 잠수교)
        
        2. 🔵 **우회 대상 (추출 절대 금지 X - 파기):**
           - 막힌 곳을 '피해 가기 위해', '대신 지나가는', '대체하여 운행하는' 도로 및 정류소 명칭 (예: 반포대교, 반포대교 남단 정류소 등)
           - "~번 버스가 OO도로로 우회하여 XX정류소에 정차한다"는 문장의 장소들은 **실제 통제/사고 지점이 아니라 정상 소통 중인 대체 경로**이므로 최종 JSON 결과 배열(`[]`)에 단 한 건도 포함시켜서는 안 되며 완전히 소멸시켜야 합니다.

        🚨 **조사 '및', '와/과', 또는 쉼표(,)로 나열된 여러 장소나 구간은 절대로 하나의 JSON 객체에 묶어서 작성하지 마십시오. 반드시 각각 독립된 JSON 객체로 찢어서 출력해야 합니다.**

        출력 JSON 구조:
        [
            {{
                "affected" : "인프라 좌표를 생성할 수 있는 구간 명칭(A ↔ B) 또는 순수 지하철 호선명 또는 건물 지번 주소 또는 본문에 명시된 5자리 정류소 ID 숫자 (버스 노선 번호 절대 금지)",
                "location_type" : "BETWEEN_NODES, LINEAR_REFERENCE, ADDRESS_POINT, BUS, SUBWAY 중 하나로 분류",
                "details" : {{
                    "road_name" : "해당하는 도로 이름 또는 지하철 호선명. 없다면 null",
                    "start_node" : "BETWEEN_NODES 또는 SUBWAY 유형일 때 시작 지점/시작 역 명칭. 없다면 null",
                    "end_node" : "BETWEEN_NODES 또는 SUBWAY 유형일 때 종료 지점/종료 역 명칭. 없다면 null",
                    "anchor_node" : "LINEAR_REFERENCE 유형일 때 기준이 되는 랜드마크 명칭 (예: 금하지하차도, 행주대교). 없다면 null",
                    "offset_start" : "LINEAR_REFERENCE 유형일 때 시작 거리(정수형, m 단위, 예: 250). 없다면 null",
                    "offset_end" : "LINEAR_REFERENCE 유형일 때 종료 거리(정수형, m 단위, 예: 650). 없다면 null",
                    "address" : "ADDRESS_POINT 유형일 때의 주소 정보. 없다면 null"
                }},
                "lat" : "본문에 직접 명시된 위도 값(float). 명시되어 있지 않다면 반드시 null. 대한민국은 북위 약 33°~38°에 위치",
                "lng" : "본문에 직접 명시된 경도 값(float). 명시되어 있지 않다면 반드시 null. 대한민국은 동경 약 126°~131°에 위치",
                "si" : "본문에 명시되거나 유추 가능한 도시 이름 (예: 서울특별시)",
                "gu" : "본문에 명시되거나 유추 가능한 지역구 이름 (예: 서초구, 중구 등)",
                "startDateTime" : "datetime 형식. %Y-%m-%d %H:%M:%S 형태",
                "endDateTime" : "datetime 형식. %Y-%m-%d %H:%M:%S 형태",
                "content" : "이 특정 장소와 관련된 통제 내용 요약(우회 정류소/운행, 우회 도로 내용 절대 금지)"
            }}
        ]

        위와 같은 JSON 구조로 만들어야 합니다.

        다음과 같은 step에 따라 JSON 객체를 생성해주세요.
        1. 텍스트 필터링 및 파기 단계: 
           - 본문에서 '우회', '우회 운행', '임시 우회', '이격 정류소' 문맥에 걸려 있는 장소 및 도로명은 머릿속에서 아예 지워버리세요(Drop). 
           - 오직 통행이 완전히 차단되거나 금지된 실제 '원인 제공 통제 구간'만 남기세요.
        2. 위치 및 인프라 유형(location_type) 분류하기: 
           - 남은 실제 통제 구간에 대해서만 유형을 분류하고, 버스 노선 번호는 폐기 처리하세요.
        3. location_type별로 JSON 객체 완성하기: 분류한 인프라 객체들을 기재 규칙에 따라 각각 완성해주세요.

        [위치 및 인프라 유형(location_type) 분류 규칙]
        - "ADDRESS_POINT": 
            1. 교차로나 상하행 구간 분리 없이, 특정 교량/랜드마크 명칭(예: "잠수교", "삼각지교차로")이 단독으로 명시되었거나 특정 건물 지번 주소가 명시된 경우에만 해당합니다.
            2. 🚨 **[통제 상태 검증 필수 원칙]**: 명시된 장소가 **'공사, 사고, 행사 등으로 인해 차량이나 보행자의 통행이 실제로 금지되거나 차단된 곳'일 때만 이 유형으로 추출**할 수 있습니다. 
            3. 버스 우회 노선 상에 존재하는 정상 가동 정류소 명칭이나, 단순 안내용 랜드마크(예: "우회 도로인 반포대교 남단 이용 바람", "600m 이격된 정류소에서 승하차")는 실제 통제/사고 지점이 아니므로 **절대로 "ADDRESS_POINT"를 포함한 그 어떤 객체로도 생성해서는 안 되며 소멸시켜야 합니다.**
        - "LINEAR_REFERENCE": 
            1. 특정 기준점(나들목/교량/지하차도/IC 등)을 명시하고, 그 뒤에 **'~m 전방/후방', '몇 m 지난 지점', 'OOOm ~ OOOm 구간'처럼 숫자와 m(미터) 단위가 결합된 물리적 거리 범위**가 등장하는 경우 무조건 이 유형으로 분류하십시오.
            2. 🚨 **[BETWEEN_NODES 오매핑 절대 방지 규칙]**: 비록 본문에 '~' 기호나 'A ~ B' 형태로 미터 거리가 나열되어 있더라도, **"기준 랜드마크 + 미터(m) 거리" 형태가 결합되어 있다면 이는 도로 구간이 아니라 기준점 기반 선형 참조이므로 절대로 "BETWEEN_NODES"로 분류해서는 안 되며, 무조건 "LINEAR_REFERENCE" 서랍에 넣어야 합니다.**
            
        - "BETWEEN_NODES": 
            1. 본문에 'A교차로 ↔ B교차로', 'A에서 B 사이', 'A교차로 ~ B교차로'와 같이 **순수한 지명, 교차로명, IC 명칭, 지하철역 명칭인 출발 거점과 종료 거점**이 명확하게 대칭을 이룰 때에만 이 유형을 적용합니다.
            2. `start_node`나 `end_node` 자리에 '행주대교 지난 300m' 같은 **미터(m) 단위의 가상 위치가 들어가야 하는 상황이라면 이 유형을 절대 선택하지 마십시오.**
        - "BUS": 본문 텍스트 내에 **'반드시 연속된 5자리의 숫자로만 구성된 순수 버스 정류소 고유 번호(예 : 01234)'**가 명확히 박혀 있는 경우에만 분류합니다. 3자리나 4자리 숫자는 버스 노선 번호이므로 절대로 이 유형으로 분류할 수 없습니다.
            - 본문 텍스트를 검사하여 **5자리 숫자로 이루어진 정류소 ID**가 실제로 존재하지 않는다면, "BUS" 유형의 JSON 객체는 최종 출력 배열(`[]`)에 단 하나도 생성하거나 포함시켜서는 안 됩니다.
        - "SUBWAY" : 지하철 및 철도 노선(예 : 2호선, 경의중앙선 등)의 통제가 발생하는 경우
        

        [location_type 별 기재 규칙]
        - "BETWEEN_NODES" : 
            - [구간 분류 및 나열형 지명 분리 절대 규칙]:
                1. 본문 텍스트에 'A ↔ B', 'A에서 B 사이', 'A부터 B까지'처럼 방향성이나 시점과 종점이 물리적으로 명확하게 연결된 텍스트 구조일 때에만 이 유형을 적용할 수 있습니다.
                2. **단순 나열형 차단:** 본문에 "통제구간: 한강 잠수교 및 반포한강공원 남단 달빛광장" 또는 "A 및 B", "A, B, C 일대" 처럼 **조사 '및', '와/과', 또는 쉼표(,)로 단사들이 연결되어 있는 경우는 절대로 하나의 구간(BETWEEN_NODES)으로 묶어서는 안 됩니다.** 3. 이처럼 명확한 화살표나 기호 없이 단지 여러 지명이 나열된 경우, 이는 구간이 아니라 **각각 독립된 개별 지점**들입니다. 따라서 반드시 이들을 전부 쪼개어 **각각 "ADDRESS_POINT" 유형의 독립된 JSON 객체로 분리하여 전수 추출**해야 합니다. (절대로 임의로 앞 단어를 start_node에, 뒷 단어를 end_node에 끼워 맞추지 마십시오.)
            - "affected" : '시작지점명 ↔ 종료지점명' 형태로 연관 구간을 명확히 기재하십시오.
            - "details": `start_node`에 시작점 명칭, `end_node`에 종료점 명칭을 깨끗하게 격리 기재하세요.
        - "BUS": 본문 텍스트 내에 **'반드시 연속된 5자리의 숫자로만 구성된 순수 버스 정류소 고유 번호(예 : 01234)'**가 명확히 박혀 있는 경우에만 분류합니다.
            ⭕ **[GOOD 예시 - 올바른 출력]**:
            "affected" : 01234 (O)
            
            - ❌ **[절대 오답 예시]**: "405번 버스 반포대교 남단 정류소 임시 우회 승하차" ➡️ 이는 통제 구역이 아니라 단순 버스 우회 정보이므로 "BUS"든 "ADDRESS_POINT"든 **어떤 객체로도 생성해서는 안 되며 완전히 무시하고 폐기**해야 합니다.
            - 본문 텍스트를 검사하여 **5자리 숫자로 이루어진 정류소 ID**가 실제로 존재하지 않는다면, "BUS" 유형의 JSON 객체는 최종 출력 배열(`[]`)에 단 하나도 생성하거나 포함시켜서는 안 됩니다.
            
        - "SUBWAY" :         
            1. 지하철 및 철도 관련 공지는 "location_type"을 "SUBWAY"로 지정하세요.
            2. 🚨🚨 **[affected 필드 기재 절대 엄격 금지 규칙]** 🚨🚨
               - `affected` 필드에는 **오직, 오직 순수 지하철 호선/노선 이름만 단독으로** 깨끗하게 작성해야 합니다. 뒤에 '역' 이름을 붙이는 행위는 시스템을 붕괴시키는 치명적인 오류입니다.
               - 역 이름(예: 용산역, 자양역)은 **절대로, 무슨 일이 있어도 `affected`에 포함시켜서는 안 되며**, 무조건 `details` 내부의 `start_node`와 `end_node`로만 격리해야 합니다.
               ⭕ **[GOOD 예시 - 올바른 출력]**:
               "affected": "1호선" (O)
               "affected": "7호선" (O)
               "affected": "경의중앙선" (O)
               
            3. 지하철 호선이 여러 개인 경우, 각각 호선을 affected에 각각 따로 기재해야 하며, start_node, end_node도 각각에 맞도록 기재해야 합니다.
            4. 특정 역 하나만 문제가 발생했다면 start_node와 end_node를 같게 적어야 합니다.
        - "ADDRESS_POINT" :
            - 출력 JSON 구조에 따라 기재하되, 실제 통제 구역이 아닌 우회 안내용 정류소나 도로는 절대 이 서랍에 담지 마십시오.
        - "LINEAR_REFERENCE" :
            1. **[필수 텍스트 분리 및 격리 규칙]**: 이 유형은 본문 텍스트(예: "행주대교 지난 300m~600m")를 다음 3가지 요소로 칼같이 격리 기재해야 합니다.
               - `anchor_node`: 기준이 되는 순수 랜드마크/지명 이름만 단독으로 추출하십시오. (예: "행주대교", "금하지하차도", "영동대교")
               - `offset_start`: 기준점으로부터 시작되는 물리적 거리의 숫자(정수형, m 단위, 예: 300 또는 250)
               - `offset_end`: 기준점으로부터 끝나는 물리적 거리의 숫자(정수형, m 단위, 예: 600 또는 650)
            2. 🚨 **[노드 중복 기재 엄격 금지]**: `anchor_node`에 추출한 순수 지명(예: "행주대교")을 제외한 나머지 가상의 위치 문자열 전체(예: "행주대교 지난 300m")를 `start_node`와 `end_node` 필드에 중복해서 채워 넣는 행위를 절대 금지합니다.
            3. 따라서 이 유형일 때 `start_node`와 `end_node`는 원칙적으로 **null**로 비워두어야 하며, 오직 `anchor_node`, `offset_start`, `offset_end` 삼박자만 깨끗하게 채워야 합니다.
            - "affected" : 입력된 원본의 구간 명칭(예: "올림픽대로 행주대교 지난 300m~600m")을 그대로 기재하십시오.    
        
        [DateTime 설정 규칙 ★★★]
        1. 본문에 '매주 일요일', '매주 토·일요일'처럼 주기적 반복 조건이 기술되어 있는 경우:
           - 절대 축제 전체 기간(예: 4월~6월)을 start/end에 통째로 박지 마십시오.
           - 상단에 주어진 **[현재 시스템 기준 일시]**를 기준으로, **'이번 주(현재 일시가 속한 주) 또는 현재 일시 직후에 도달하는 가장 가까운 해당 요일'의 실제 주말 날짜**를 계산하여 대입해야 합니다.
           
           ※ 연산 예시 (현재 기준일이 2026-06-08 월요일이고, 본문이 '매주 일요일'인 경우):
             - 이번 주에 돌아오는 일요일은 '2026-06-14'입니다.
             - 따라서 `startDateTime`은 이번 주 일요일 날짜와 당일 통제 시작 시각을 결합한 '2026-06-14 11:00:00'이 되어야 합니다.
             - `endDateTime` 역시 당일 통제가 종료 및 해제되는 시각을 결합한 '2026-06-14 23:00:00'이 되어야 합니다.

        2. 단발성 행사인 경우:
           - 본문에 특정 날짜와 시간이 명시되어 있다면 그 날짜를 그대로 %Y-%m-%d %H:%M:%S 형식에 맞춰 입력하세요.
           - 연도가 생략되어 있다면 [현재 시스템 기준 일시]의 연도를 따릅니다.
           - 오직 본문에 종료 시점이나 기간에 대한 언급이 '아예 없을 때'에만 endDateTime을 '2099-12-31 23:59:59'로 설정하세요.

        반드시 순수 JSON 형식으로만 응답하세요.        
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
                "content" : f"교통 공지사항 정보 : {raw_data}"
            }
        ],
        max_tokens=3000,
        temperature=0.1, # 답변의 일관성을 위해 0.2~0.3 유지 권장
    )

    result = json.loads(response.choices[0].message.content)
    config.logger.info(f"[MAS01 Agent : extract_affected] outputs : {result}")
    return {"extracted_entities" : result}
        
async def enrich_coordinates_node(state: AgentState) -> Dict[str, Any]:
    """
    LLM이 추출한 extracted_entities 리스트를 루프 돌며,
    location_type 별 최적의 GIS 함수를 실행해 lat, lng 사후 세팅
    """
    entities = state.get('extracted_entities', [])
    config.logger.info(f"[MAS01 Agent : enrich_coordinates_node] Processing {len(entities)} entities...")
    
    final_processed_nodes = []
    
    for entity in entities:
        item = entity.copy() # 원본 훼손 방지
        location_type = item.get("location_type")
        details = item.get("details", {})
        affected_name = item.get("affected")
        
        if item.get("lat") is None or item.get("lng") is None:
            coord_result = None
            
            # 1. 분기 라우팅 처리
            if location_type == "BETWEEN_NODES":
                coord_result = await resolve_between_nodes(
                    road_name=details.get("road_name"),
                    start_node=details.get("start_node"),
                    end_node=details.get("end_node")
                )
            elif location_type == "LINEAR_REFERENCE":
                coord_result = await resolve_linear_reference(
                    road_name=details.get("road_name"),
                    anchor_node=details.get("anchor_node"),
                    offset_start=float(details.get("offset_start", 0)),
                    offset_end=float(details.get("offset_end", 0))
                )
            elif location_type == "ADDRESS_POINT":
                coord_result = await resolve_address_point(address=details.get("address"))
                
            # 2. 좌표 툴 연산 성공 시 결합
            if coord_result:
                item["lat"] = coord_result["lat"]
                item["lng"] = coord_result["lng"]
                
                if not item.get("si"):
                    item["si"] = coord_result.get("si")
                if not item.get("gu"):
                    item["gu"] = coord_result.get("gu")
                config.logger.info(f"[MAS01 Agent : enrich_coordinates_node] 매핑 성공 [{affected_name}] -> {coord_result}")
            else:
                config.logger.warning(f"[MAS01 Agent : enrich_coordinates_node] 매핑 실패 [{affected_name}] - SHP 내 데이터 부재")
                
        final_processed_nodes.append(item)
        config.logger.info(f"[MAS01 Agent : enrich_coordinates_node] 결과 : {final_processed_nodes}")
        
    return {"affected_nodes": final_processed_nodes}

async def apply_to_neo4j_graph_node(state:AgentState) -> Dict[str, Any] :
    nodes = state.get("affected_nodes", [])
    config.logger.info(f"[MAS01 Agent : apply_to_neo4j_graph_node] {len(nodes)}개의 인프라 객체 그래프 DB 반영 시작...")
    
    cypher_query01 = """
        MERGE (i:Incident {id: $incident_id})
        ON CREATE SET 
            i.content = $content,
            i.location_type = $location_type,
            i.start_time = datetime(replace($start_time, " ", "T")),
            i.end_time = datetime(replace($end_time, " ", "T"))
        WITH i
        MATCH (s:Station {is_master: false, type: "BUS"})
        WHERE point.distance(s.location, point({srid: 4326, x: $lng, y: $lat})) <= 200
        MERGE (s)-[r:AFFECTED_BY]->(i)
        RETURN count(r) as connected_count
    """
    
    cypher_query02 = """
        MERGE (i:Incident {id: $incident_id})
        ON CREATE SET 
            i.content = $content,
            i.location_type = $location_type,
            i.start_time = datetime(replace($start_time, " ", "T")),
            i.end_time = datetime(replace($end_time, " ", "T"))
        WITH i
        MATCH (s:Station {type: "BUS", ars_id: $ars_id, is_master: false})
        MERGE (s)-[r:AFFECTED_BY]->(i)
        RETURN count(r) as connected_count
    """
    
    cypher_query03 = """
        MERGE (i:Incident {id: $incident_id})
        ON CREATE SET 
            i.content = $content,
            i.location_type = $location_type,
            i.start_time = datetime(replace($start_time, " ", "T")),
            i.end_time = datetime(replace($end_time, " ", "T"))
        WITH i
        MATCH (start_st:Station {is_master: false})
        WHERE start_st.route_id = $route_id AND start_st.name CONTAINS $start_node
        MATCH (end_st:Station {is_master: false})
        WHERE end_st.route_id = $route_id AND end_st.name CONTAINS $end_node
        MATCH path = shortestPath((start_st)-[:NEXT_STOP*..30]->(end_st))
        WITH i, nodes(path) as affected_platforms
        UNWIND affected_platforms as s
        MERGE (s)-[r:AFFECTED_BY]->(i)
        RETURN count(r) as connected_count
    """
    
    cypher_query04 = """
        MERGE (i:Incident {id: $incident_id})
        ON CREATE SET 
            i.content = $content,
            i.location_type = $location_type,
            i.start_time = datetime(replace($start_time, " ", "T")),
            i.end_time = datetime(replace($end_time, " ", "T"))
        WITH i
        MATCH (s:Station {is_master: false})
        WHERE s.route_id = $route_id AND s.name CONTAINS $start_node
        MERGE (s)-[r:AFFECTED_BY]->(i)
        RETURN count(r) as connected_count
    """
    
    success_nodes = []
    
    # 1단계: 모든 엔터티 루프를 돌며 Neo4j 세션 단독 실행 및 완전 밀봉
    for node in nodes :
        item = node.copy()
        lat = item.get("lat")
        lng = item.get("lng")
        affected = item.get("affected")
        location_type = item.get("location_type")
        
        result = None
        try:
            async with config.neo4j_client.session() as session:
                if location_type == "BUS":
                    incident_id = hashlib.md5(f"{item.get('startDateTime')}_BUS_{affected}".encode('utf-8')).hexdigest()
                    result = await session.run(
                        cypher_query02,
                        incident_id=incident_id,
                        content=item.get("content"),
                        location_type=location_type,
                        start_time=item.get("startDateTime"),
                        end_time=item.get("endDateTime"),
                        ars_id=str(affected).strip()
                    )
                elif location_type == "SUBWAY" :
                    details = item.get("details")
                    start_st = details.get("start_node").replace("역", "")  
                    end_st = details.get("end_node").replace("역", "")
                    incident_id = hashlib.md5(f"{item.get('startDateTime')}_{item.get('affected')}_{start_st}_{end_st}".encode('utf-8')).hexdigest()
                    
                    if start_st == end_st:
                        result = await session.run(
                            cypher_query04, 
                            incident_id=incident_id, 
                            content=item.get("content"), 
                            location_type=location_type, 
                            start_time=item.get("startDateTime"), 
                            end_time=item.get("endDateTime"), 
                            route_id=str(item.get("affected")).strip(), 
                            start_node=str(start_st).strip(), 
                            end_node=str(end_st).strip()
                        )
                    else : 
                        result = await session.run(
                            cypher_query03, 
                            incident_id=incident_id, 
                            content=item.get("content"), 
                            location_type=location_type, 
                            start_time=item.get("startDateTime"), 
                            end_time=item.get("endDateTime"), 
                            route_id=str(item.get("affected")).strip(), 
                            start_node=str(start_st).strip(), 
                            end_node=str(end_st).strip()
                        )
                else :
                    if lat is None or lng is None:
                        config.logger.warning(f"[MAS01 Agent apply_to_neo4j_graph_node] : [{item.get('affected')}] 좌표 정보 부재로 패스.")
                        continue
                    
                    incident_id = hashlib.md5(f"{item.get('startDateTime')}_{lat}_{lng}".encode('utf-8')).hexdigest()
                    result = await session.run(cypher_query01, incident_id=incident_id, content=item.get("content"), location_type=item.get("location_type"), start_time=item.get("startDateTime"), end_time=item.get("endDateTime"), lat=float(lat), lng=float(lng))
                    
                # 분기별 무관하게 result.single()을 소모하여 그래프 트랜잭션 동기화 및 강제 빌드 유도
                record = await result.single()
                connected_count = record["connected_count"] if record else 0
                
                item["incident_id"] = incident_id
                success_nodes.append(item)
                config.logger.info(f"[MAS01 Agent apply_to_neo4j_graph_node][Neo4j 동기화 완료] [{item.get('affected')}] 관계선 {connected_count}개소 융합 완료.")
                
        except Exception as e:
            config.logger.error(f"[MAS01 apply_to_neo4j_graph_node ] [Neo4j 세션 오류] '{item.get('affected')}' 처리 실패: {e}")
            continue
            
    # 루프가 완전히 종료(Neo4j DB 영구커밋 완료)된 안전 구역에서만 Redis 발행 기동!!
    config.logger.info(f"[MAS01 apply_to_neo4j_graph_node] [MAS01 -> Neo4j] 모든 인프라 노드 완벽 저장 성공. 최종 채널 전파를 시작합니다 (총 {len(success_nodes)}건)")
    for s_node in success_nodes:
        gu_name = s_node.get("gu")
        si_name = s_node.get("si")
        if gu_name:
            await publish_to_channel(gu_name, si_name, s_node)
            config.logger.info(f"[MAS01 apply_to_neo4j_graph_node] [MAS01 -> Redis] DB 무결성을 확인한 후 안전하게 [{gu_name}] 스트림 발행 성공!")
            
    return {"affected_nodes": success_nodes}
    

mas01_workflow = StateGraph(AgentState)

mas01_workflow.add_node('extract_affected_node', extract_affected_node)
mas01_workflow.add_node('enrich_coordinates_node', enrich_coordinates_node)
mas01_workflow.add_node('apply_to_neo4j_graph_node', apply_to_neo4j_graph_node)

mas01_workflow.set_entry_point('extract_affected_node')
mas01_workflow.add_edge('extract_affected_node', 'enrich_coordinates_node')
mas01_workflow.add_edge('enrich_coordinates_node', 'apply_to_neo4j_graph_node')
mas01_workflow.add_edge('apply_to_neo4j_graph_node', END)

mas01_agent = mas01_workflow.compile()