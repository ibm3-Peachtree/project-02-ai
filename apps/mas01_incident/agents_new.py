# apps/mas01_incident/agents.py
from typing import TypedDict, Annotated, Sequence, Dict, List, Any
import re
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
from apps.mas01_incident.instructions import between_instruction, address_instruction, linear_instruction, busstop_instruction, subway_instruction

class AgentState(TypedDict) :
    raw_incident_data : Dict[str, Any]
    ner_entities : List[Dict[str, Any]] # 통제, 우회, entity
    preprocessed_entities : List[Dict[str, Any]] # 전처리
    classified_entities : List[Dict[str, Any]] # location_type classfication
    temp_outputs : List[Dict[str, Any]]
    final_outputs : List[Dict[str, Any]]

kanana_client = AsyncOpenAI(base_url=config.KANANA_MODEL_02_URL, api_key="fake-key")

def extract_json_array(raw_text):
    # [ 로 시작해서 ] 로 끝나는 가장 긴 구간을 찾습니다. (점진적 매칭)
    match = re.search(r'\[\s*\{.*\}\s*\]', raw_text, re.DOTALL)
    
    if match:
        json_string = match.group(0) # 매칭된 [ { ... } ] 부분만 추출
        return json_string
    else:
        return "[]"
    
async def node_ner(state: AgentState) -> List[Dict[str, Any]] :
    """
    raw incident data로부터 교통 통제가 되는 entity들을 추출합니다
    또한 통제, 우회, 혼잡을 분류합니다
    """
    ner_instruction = f"""
        당신은 교통 정보 전문 분류가입니다. 
        사용자가 입력한 교통 공지사항 정보에는 물리적으로 사고나 행사, 공사 등의 이유로 도로/정류소/지하철 역 이용이 통제된다는 내용과,
        때때로 통제를 피하기 위한 우회 도로/정류소/지하철 역/버스 노선에 대한 내용이 기재되어 있습니다.
        
        당신의 역할은 교통 공지사항으로부터 물리적인 공간 좌표를 추출할 수 있는 실제 인프라(도로 구간, 건물 지번 주소, 지하철 노선, 5자리 숫자의 버스 정류소 ID)를 각각 엔터티로 추출하고, 각 엔터티가 "통제"를 위해 기재되었는지,
        "우회" 경로를 안내하기 위해 기재되었는지 순수 JSON List 형태로 출력하는 것입니다.
        
        [통제 / 우회 / 혼잡 분류법]
        - 통제 : 특정 도로/정류소/지하철 역 자체가 폐쇄되거나, 이용이 불가하거나, '무정차' 혹은 '무정차 통과' 대상이 된 경우 무조건 '통제'입니다. (예: 무정차 정류장 목록은 예외 없이 전부 '통제'로 분류)
        - 우회 : 통제 구역을 피하기 위해 "대체 구역"으로 새롭게 지정되어 이용하라고 안내된 도로명이나 정류소 명칭인 경우에만 '우회'입니다.
        
        [entity 추출법]
        다음은 entity 후보들에 대한 설명입니다. 각 json 요소 별 entity는 하나씩만 가져야 합니다.
        1. 도로 및 지명
            - 두 장소 간의 연결 구간만 추출합니다. 반드시 하이픈(-)을 중심으로 "출발지 - 도착지" 형태로만 작성하세요.
            - 만약 본문에 여러 경로가 연속으로 나열되어 있다면(예: A → B → C → D), 절대로 한 줄로 묶지 말고 직접 쪼개서 개별 객체로 만드세요.
            - 입력 예시: "강남역 사거리 → 신논현역 → 서초역"
            - 출력 예시 (반드시 분리할 것):
                {{"entity" : "강남역 사거리 - 신논현역", "obj" : "통제", "meta" : "도로"}},
                {{"entity" : "신논현역 - 서초역", "obj" : "통제", "meta" : "도로"}}          
        2. 버스 정류소 
            - 버스 정류소 이름 혹은 ARS ID(순수 5자리 숫자, 예 : 01234) 만 기재합니다
            - 버스 정류소 이름(ARS ID) 형태로 적힌 경우 ARS ID만 기재합니다
            - 예 : 
                - ARS ID인 경우 : 01234
                - 정류소 이름 : 종로1가
        3. 버스
            - 버스 노선 번호(버스 회사(OO운수 등)) 형태일 경우 버스 노선 번호(숫자 only)만 기재합니다.
        4. 지하철 노선, 역
            - 예 :
                - 두 개 이상 역 구간인 경우 : 1호선 (용산역 - 서울역)
                - 하나의 역 지점인 경우 : 1호선 용산역
                
        [obj]
        - 위의 통제 / 우회 / 혼잡 분류 방법에 따른 나온 결과
        
        [meta]
        - 도로, 버스, 버스정류장, 지하철 중 하나로 작성
        
        [출력 규칙]
        - 마크다운(예: ```json), 부연 설명, 인사말 등을 절대 적지 마십시오.
        - 오직 유효한 순수 JSON List만 출력해야 하며, 이를 위반하면 시스템이 다운됩니다.
        순수 JSON List 출력 예시 :
        [
            {{
                "entity" : "남부순환로",
                "obj" : "통제",
                "meta" : "도로"
            }},
            {{
                "entity" : "25010",
                "obj" : "우회",
                "meta" : "버스"
            }}
        ]
    """
    response = await kanana_client.chat.completions.create(
        model="kakaocorp/kanana-1.5-8b-instruct-2505",
        messages=[
            {
                "role" : "system",
                "content" : ner_instruction
            },
            {
                "role" : "user",
                "content" : f"교통 공지사항 정보 : {state['raw_incident_data']}"
            }
        ],
        max_tokens=3000,
        temperature=0.1, # 답변의 일관성을 위해 0.2~0.3 유지 권장
    )
    try :
        res_list = json.loads(extract_json_array(response.choices[0].message.content))
        config.logger.info(f"[MAS01 NODE NER 완료] : {res_list}")
    except Exception as e:
        res_list = []
        config.logger.error(f"[MAS01 NODE NER 실패] : {e}")
    return {
        "ner_entities" : res_list
    }
    
async def node_preprocess(state: AgentState) -> List[Dict[str, Any]] :
    """
    추출된 entity를 정제합니다
    """
    entity_preprocess_instruction = """
    당신은 교통 정보의 텍스트 정제 전문가입니다.
    사용자가 입력한 교통 공지사항 본문과 1차 추출된 entities 리스트를 바탕으로, 지명, 도로, 랜드마크 표현을 시스템 표준 서식에 맞게 정제하는 것이 당신의 임무입니다.

    다음 정제 규칙을 엄격히 준수하세요.

    [텍스트 정제 규칙]
    1. 모든 구간 연결 기호는 하이픈 "-" 기호로 통일합니다. 물결표나 다른 특수문자는 절대 금지합니다.
        - "entity" : 남부순환로 (봉천 - 서울대입구)
        - "entity" : 한강대교 북단 - 한강대교 남단
        - 다중 구간 쪼개기 규칙 (필수): 하이픈("-"), 화살표 기호나 물결표("~")를 중심으로 지명이 3개 이상 길게 나열된 경우(예: A - B - C - D), 절대 한 줄로 두지 말고 순서대로 2개씩 짝을 지어 각각 독립된 객체로 쪼개야 합니다.
            - 입력 예시: "A - B - C - D"
            - 출력 예시:
                - "entity": "A - B"
                - "entity": "B - C",
                - "entity": "C - D",
        

    3. 버스 정류소 고유 번호(ARS ID) 5자리 숫자가 정류소 이름과 함께 적혀 있다면, 이름은 버리고 오직 "5자리 숫자"만 남기세요.
    4. 버스 노선 번호가 "000번, 0000번, 마을00번, N00번"처럼 여러 개 적혀있다면, 각 버스 노선 번호 별로 entity를 각각 쪼개십시오.
    5. 지하철 역 구간이나 노선 정보는 다음과 같이 정리합니다.
        - 노선 자체: "1호선"
        - 특정 역: "1호선 용산역"
        - 역 구간: "1호선 (용산역 - 서울역)"
    
    [obj 규칙]
    - 입력된 entity의 obj를 기준으로 합니다.
    - 반드시 통제, 우회, 혼잡 중 하나여야 합니다.

    [출력 규칙]
        - 마크다운(예: ```json), 부연 설명, 인사말 등을 절대 적지 마십시오.
        - 오직 유효한 순수 JSON List만 출력해야 하며, 이를 위반하면 시스템이 다운됩니다.
    이후 아래 구조의 순수 JSON List 형태로 출력하세요.
    [
        {
            "entity": "정제 완료된 지명 또는 구간 텍스트",
            "obj": "기존 데이터의 obj 값 유지"
        }
    ]
    """
    response = await kanana_client.chat.completions.create(
        model="kakaocorp/kanana-1.5-8b-instruct-2505",
        messages=[
            {
                "role" : "system",
                "content" : entity_preprocess_instruction
            },
            {
                "role" : "user",
                "content" : f"교통 공지사항 정보 : {state['raw_incident_data']}\n entities : {state['ner_entities']}"
            }
        ],
        max_tokens=3000,
        temperature=0.1, # 답변의 일관성을 위해 0.2~0.3 유지 권장
    )
    try :
        entity_pre = json.loads(extract_json_array(response.choices[0].message.content))
        config.logger.info(f"[MAS01 NODE PREPROCESS 완료] : {entity_pre}")
    except Exception as e :
        entity_pre = []
        config.logger.error(f"[MAS01 NODE PREPROCESS 실패] : {e}")
    
    return {
        "preprocessed_entities" : entity_pre
    }
    
async def node_location_type_classify(state: AgentState) -> List[Dict[str, Any]] :
    location_type_instruction = """
    당신은 교통 정보 분류 전문가입니다.
    교통 공지사항 본문, entities 리스트를 사용자가 입력할 때, 각 entity가 어떤 인프라 유형(location_type)에 속하는지 매핑하는 것이 당신의 역할입니다.

    아래의 분류 규칙을 절대적으로 따르세요. 섣부른 추측이나 형태만 보고 판단하는 행위는 엄격히 금지합니다.

    지하철 호선 이름 : [
        "1호선", "2호선", "3호선", "4호선", "5호선", 
        "6호선", "7호선", "8호선", "9호선", 
        "경의중앙선", "경춘선", "수인분당선", "신분당선", 
        "공항철도", "우이신설선", "신림선", "경강선", 
        "서해선", "인천1호선", "인천2호선", "김포골드라인", 
        "용인에버라인", "의정부경전철", "GTX-A"
    ]
        
    [location_type 분류 규칙]
    각 "entity"를 기준으로 판단하며 반드시 하나의 type으로만 분류하세요.
    
    - BETWEEN_NODES:
        [BETWEEN_NODES가 아닌 경우]
        0. '500m' 처럼 숫자 + 미터법이 결합되어 entity에 기재된 경우 LINEAR_REFERENCE로 분류하세요.
        1. 지하철 호선 이름이 명시되어 있거나 역, 역 구간이 명시되어 있다면 무조건 SUBWAY로 분류하세요.
        [BETWEEN_NODES인 경우]
        1. entities의 "entity"가 하이픈 "-" 기호를 중심으로 두 장소가 연결되어 있는 경우
            - '~일대', '~주변' 같은 표현 역시 '공간적 구간/범위'를 뜻하므로 반드시 BETWEEN_NODES로 분류하세요.
        2. 순수하게 지명과 지명 사이의 통제를 의미하는 구간 형태는 모두 BETWEEN_NODES에 해당합니다.
        3. 공간적 범위나 출발~종료 의미를 내포하고 있다면(예: 'A - B 일대') 도로 형태가 아니어도 BETWEEN_NODES로 분류해야 합니다.    
        
    - LINEAR_REFERENCE:
        [LINEAR_REFERENCE인 경우]
        1. 반드시 entities의 "entity"에 '300m', '600m'와 같이 숫자가 결합된 거리 단위(m, 미터) 정보가 "반드시 직접 명시"되어 있어야 합니다.
        [LINEAR_REFERENCE가 아닌 경우]
        1. 미터법(m)이 명시되어 있지 않고 두 지점이 하이픈(-)으로 연결되어 있으면 BETWEEN_NODES로 분류하세요.
        2. 숫자 + 번(000번, 0000번)으로 구성된 경우, 버스 노선 번호 이므로 BUS로 분류하세요.
    
    - ADDRESS_POINT:
        1.  단 하나의 교량, 랜드마크, 단독 교차로명, 혹은 특정 건물 지번 주소만 단독 명시된 경우에 해당합니다.
        2.  지번 주소에 '000-00'과 같이 숫자와 하이픈(-)으로 구성된 문 도로명주소와 함께 있을 수 있습니다.
        3.  지번 주소에 'OO로 00' 처럼 도로명과 함께 숫자가 포함될 수 있습니다.

    - BUSSTOP:
        1. "00000"처럼 연속된 순수 5자리 숫자로만 구성된 버스 정류소 고유 ARS ID인 경우에 해당합니다.
    
    - BUS:
        1. "000번", "0000번" 처럼 뒤에 번이나 버스 노선임이 명시된 버스 운행 노선 번호인 경우에 해당합니다.

    - SUBWAY:
        [SUBWAY가 아닌 경우]
            1. 랜드마크 명과 지하철 역 이름이 중복되는 경우, entity에 '역'이나 '호선' 마커가 없는 단순 지명이거나 지하철 이용에 직접적인 차질이 없다면 BETWEEN_NODES로 분류하세요.
        [SUBWAY인 경우]
        1. 지하철 호선 이름(예: 7호선)이 단독으로 있거나 호선과 역 이름이 결합된 철도 관련 인프라인 경우에 해당합니다.
        2. 반드시 텍스트에 "1호선", "2호선", "경의중앙선" 같은 '정확한 노선 이름'이 명시되어 있거나, "용산역", "시청역" 처럼 뒤에 '역'이라는 글자가 명확히 붙어있어야 합니다.
        3. 지하철 역 이용이 불가능 할 때만 SUBWAY로 분류하세요.
        4. 예시 : 1호선, 1호선 용산역, 1호선 (용산역 - 서울역) 와 같이 지하철 노선 이름과 역 이름으로 구성된 경우에만 분류하세요.
        

    [라벨링 필수 규칙]
    "location_type" 필드에는 아래의 지정된 6개 문자열 중 '정확히 하나'만 대입해야 합니다. 대소문자와 언더바(_)를 엄격히 준수하세요.
    - 허용 리스트: ["LINEAR_REFERENCE", "BETWEEN_NODES", "ADDRESS_POINT", "BUS", "BUSSTOP", "SUBWAY"]
    
    [출력 규칙]
        - 마크다운(예: ```json), 부연 설명, 인사말 등을 절대 적지 마십시오.
        - 오직 유효한 순수 JSON List만 출력해야 하며, 이를 위반하면 시스템이 다운됩니다.
    
    이후 아래의 구조에 맞춰 최종 완성된 순수 JSON List 형태로 답변하세요.
    [
        {
            "reason" : "location_type을 결정하게 된 근거 제시",
            "location_type" : "reason에서 도출된 답을 기재",
            "entity" : "1단계에서 넘어온 entity 텍스트 그대로 유지",
            "obj" : "entity의 obj 값 유지(통제 or 우회 or 혼잡 중 하나)",
        }
    ]
    """
    
    response = await kanana_client.chat.completions.create(
        model="kakaocorp/kanana-1.5-8b-instruct-2505",
        messages=[
            {
                "role" : "system",
                "content" : location_type_instruction
            },
            {
                "role" : "user",
                "content" : f"교통 공지사항 정보 : {state['raw_incident_data']}\n entities : {state['preprocessed_entities']}"
            }
        ],
        max_tokens=3000,
        temperature=0.1, # 답변의 일관성을 위해 0.2~0.3 유지 권장
    )
    
    try :
        location_type = json.loads(extract_json_array(response.choices[0].message.content))
        config.logger.info(f"[MAS01 NODE LOCATION TYPE CLASSIFY 완료] : {location_type}")
    except Exception as e :
        location_type = []
        config.logger.error(f"[MAS01 NODE LOCATION TYPE CLASSIFY 실패] : {e}")
    
    return {
        "classified_entities" : location_type
    }
    
async def run_sequential_generator(state: AgentState, location_type:str, instruction:str) -> List[Dict[str, Any]] :
    current_outputs = state.get("temp_outputs", [])
    
    target_entities = [lt for lt in state['classified_entities'] if lt.get('obj') == '통제' and lt.get('location_type') == location_type]
    if not target_entities:
        return {"temp_outputs": current_outputs}
    
    current_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d : %H:%M:%S")
    specific_instruction = instruction.replace("{current_time}", current_time)
    response = await kanana_client.chat.completions.create(
        model="kakaocorp/kanana-1.5-8b-instruct-2505",
        messages=[
            {"role": "system", "content": specific_instruction},
            {"role": "user", "content": f"교통 공지사항 정보 : {state['raw_incident_data']}\nentities : {target_entities}"}
        ],
        temperature=0.1
    )
    
    try :
        temp = json.loads(extract_json_array(response.choices[0].message.content))
        config.logger.info(f"[MAS01 RUN SEQUENTIAL GENERATOR 완료] : {temp}") 
        current_outputs.extend(temp)
    except Exception as e :
        temp = []
        config.logger.error(f"[MAS01 RUN SEQUENTIAL GENERATOR 실패] : {e}")
    
    for output in current_outputs :
        raw_lat = output['lat']
        raw_lng = output['lng']
        
        if raw_lat and raw_lng:
            try:
                val_lat = float(raw_lat)
                val_lng = float(raw_lng)
                
                # 100이 넘는 값(124~132)이 lat(위도)에 들어와 있다면 명백한 오류이므로 자리를 바꿉니다.
                if val_lat > 100.0 and val_lng < 50.0:
                    config.logger.warning(f"🔄 [Redis 축 전도 감지] lat과 lng가 뒤바뀌어 들어왔습니다. 강제 교정합니다. (입력 lat: {val_lat}, lng: {val_lng})")
                    output["lat"] = val_lng  # 37.52... 을 위도로
                    output["lng"] = val_lat  # 127.05... 을 경도로
                else:
                    # 데이터가 정상적으로 들어왔을 때의 포맷팅
                    output["lat"] = val_lat
                    output["lng"] = val_lng
            except ValueError:
                pass # 숫자가 아닐 경우의 예외 방어
                
        config.logger.info(f"[MAS01 Agent : run sequential generator] 보정 완료된 레디스 데이터 : {output}")
    
    return {
        "temp_outputs" : current_outputs
    }
    
async def node_address_parser(state: AgentState) -> List[Dict[str, Any]]:
    config.logger.info("▶ [MAS01 Seq] Address 파싱 검사 중...")
    return await run_sequential_generator(state, "ADDRESS_POINT", address_instruction)

async def node_linear_parser(state: AgentState) -> List[Dict[str, Any]]:
    config.logger.info("▶ [MAS01 Seq] Linear 파싱 검사 중...")
    return await run_sequential_generator(state, "LINEAR_REFERENCE", linear_instruction)

async def node_between_parser(state: AgentState) -> List[Dict[str, Any]]:
    config.logger.info("▶ [MAS01 Seq] Between 파싱 검사 중...")
    return await run_sequential_generator(state, "BETWEEN_NODES", between_instruction)

async def node_busstop_parser(state: AgentState) -> List[Dict[str, Any]]:
    config.logger.info("▶ [MAS01 Seq] Busstop 파싱 검사 중...")
    return await run_sequential_generator(state, "BUSSTOP", busstop_instruction)

async def node_subway_parser(state: AgentState) -> List[Dict[str, Any]]:
    config.logger.info("▶ [MAS01 Seq] Subway 파싱 검사 중...")
    return await run_sequential_generator(state, "SUBWAY", subway_instruction)

async def node_enrich_coordinates(state: AgentState) -> List[Dict[str, Any]] :
    """
    LLM이 추출한 entities 리스트를 루프 돌며,
    location_type 별 최적의 GIS 함수를 실행해 lat, lng 사후 세팅
    """
    entities = state.get("temp_outputs")
    config.logger.info(f"[MAS01 Agent : node_enrich_coordinates] Processing {len(entities)} entities...")
    
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
                config.logger.info(f"[MAS01 Agent : node_enrich_coordinates] 매핑 성공 [{affected_name}] -> {coord_result}")
            else:
                config.logger.warning(f"[MAS01 Agent : node_enrich_coordinates] 매핑 실패 [{affected_name}] - SHP 내 데이터 부재")
                
        final_processed_nodes.append(item)
        config.logger.info(f"[MAS01 Agent : node_enrich_coordinates] 결과 : {final_processed_nodes}")
        
    return {"final_outputs": final_processed_nodes}

async def node_apply_neo4j(state:AgentState) -> Dict[str, Any] :
    nodes = state.get("final_outputs", [])
    config.logger.info(f"[MAS01 Agent : node_apply_neo4j] {len(nodes)}개의 인프라 객체 그래프 DB 반영 시작...")
    
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
    
    cypher_query05 = """
        MERGE (i:Incident {id: $incident_id})
        ON CREATE SET 
            i.content = $content,
            i.location_type = $location_type,
            i.start_time = datetime(replace($start_time, " ", "T")),
            i.end_time = datetime(replace($end_time, " ", "T"))
        WITH i
        MATCH (s:Station {is_master: false})
        WHERE s.route_id = $route_id
        MERGE (s)-[r:AFFECTED_BY]->(i)
        RETURN count(r) as connected_count
    """
    
    success_nodes = []
    
    for node in nodes :
        item = node.copy()
        lat = item.get("lat")
        lng = item.get("lng")
        affected = item.get("affected")
        location_type = item.get("location_type")
        
        result = None
        try:
            async with config.neo4j_client.session() as session:
                if location_type == "BUSSTOP":
                    incident_id = hashlib.md5(f"{item.get('startDateTime')}_BUSSTOP_{affected}".encode('utf-8')).hexdigest()
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
                    
                    if start_st and end_st and start_st == end_st:
                        result = await session.run(
                            cypher_query04, 
                            incident_id=incident_id, 
                            content=item.get("content"), 
                            location_type=location_type, 
                            start_time=item.get("startDateTime"), 
                            end_time=item.get("endDateTime"), 
                            route_id=str(details.get("road_name")).strip(), 
                            start_node=str(start_st).strip(), 
                            end_node=str(end_st).strip()
                        )
                    elif start_st and end_st and start_st != end_st : 
                        result = await session.run(
                            cypher_query03, 
                            incident_id=incident_id, 
                            content=item.get("content"), 
                            location_type=location_type, 
                            start_time=item.get("startDateTime"), 
                            end_time=item.get("endDateTime"), 
                            route_id=str(details.get("road_name")).strip(), 
                            start_node=str(start_st).strip(), 
                            end_node=str(end_st).strip()
                        )
                    elif not start_st and not end_st :
                        result = await session.run(
                            cypher_query05, # 노선 전체 전용 쿼리 실행
                            incident_id=incident_id,
                            content=item.get("content"),
                            location_type=location_type,
                            start_time=item.get("startDateTime"),
                            end_time=item.get("endDateTime"),
                            route_id=str(details.get("road_name")).strip()
                        )
                else :
                    if lat is None or lng is None:
                        config.logger.warning(f"[MAS01 Agent node_apply_neo4j] : [{item.get('affected')}] 좌표 정보 부재로 패스.")
                        continue
                    
                    incident_id = hashlib.md5(f"{item.get('startDateTime')}_{lat}_{lng}".encode('utf-8')).hexdigest()
                    result = await session.run(cypher_query01, incident_id=incident_id, content=item.get("content"), location_type=item.get("location_type"), start_time=item.get("startDateTime"), end_time=item.get("endDateTime"), lat=float(lat), lng=float(lng))
                    
                # 분기별 무관하게 result.single()을 소모하여 그래프 트랜잭션 동기화 및 강제 빌드 유도
                record = await result.single()
                connected_count = record["connected_count"] if record else 0
                
                item["incident_id"] = incident_id
                success_nodes.append(item)
                config.logger.info(f"[MAS01 Agent node_apply_neo4j][Neo4j 동기화 완료] [{item.get('affected')}] 관계선 {connected_count}개소 융합 완료.")
                
        except Exception as e:
            config.logger.error(f"[MAS01 node_apply_neo4j ] [Neo4j 세션 오류] '{item.get('affected')}' 처리 실패: {e}")
            continue
            
    # 루프가 완전히 종료(Neo4j DB 영구커밋 완료)된 안전 구역에서만 Redis 발행
    config.logger.info(f"[MAS01 node_apply_neo4j] [MAS01 -> Neo4j] 모든 인프라 노드 완벽 저장 성공. 최종 채널 전파를 시작합니다 (총 {len(success_nodes)}건)")
    for s_node in success_nodes:
        gu_name = s_node.get("gu")
        si_name = s_node.get("si")
        if gu_name:
            await publish_to_channel(gu_name, si_name, s_node)
            config.logger.info(f"[MAS01 node_apply_neo4j] [MAS01 -> Redis] DB 무결성을 확인한 후 안전하게 [{gu_name}] 스트림 발행 성공!")
            
    return {"final_outputs": success_nodes}
    

mas01_workflow = StateGraph(AgentState)

mas01_workflow.add_node("node_ner", node_ner)
mas01_workflow.add_node("node_preprocess", node_preprocess)
mas01_workflow.add_node("node_location_type_classify", node_location_type_classify)
# mas01_workflow.add_node("run_sequential_generator", run_sequential_generator)
mas01_workflow.add_node("node_linear_parser", node_linear_parser)
mas01_workflow.add_node("node_address_parser", node_address_parser)
mas01_workflow.add_node("node_between_parser", node_between_parser)
mas01_workflow.add_node("node_busstop_parser", node_busstop_parser)
mas01_workflow.add_node("node_subway_parser", node_subway_parser)
mas01_workflow.add_node("node_enrich_coordinates", node_enrich_coordinates)
mas01_workflow.add_node("node_apply_neo4j", node_apply_neo4j)

mas01_workflow.set_entry_point("node_ner")
mas01_workflow.add_edge("node_ner", "node_preprocess")
mas01_workflow.add_edge("node_preprocess", "node_location_type_classify")

mas01_workflow.add_edge("node_location_type_classify", "node_address_parser")
mas01_workflow.add_edge("node_address_parser", "node_linear_parser")
mas01_workflow.add_edge("node_linear_parser", "node_between_parser")
mas01_workflow.add_edge("node_between_parser", "node_busstop_parser")
mas01_workflow.add_edge("node_busstop_parser", "node_subway_parser")
mas01_workflow.add_edge("node_subway_parser", "node_enrich_coordinates")
mas01_workflow.add_edge("node_enrich_coordinates","node_apply_neo4j")
mas01_workflow.add_edge("node_apply_neo4j", END)

mas01_agent = mas01_workflow.compile()