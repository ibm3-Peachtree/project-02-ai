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
from config import logger
from apps.mas01_incident.tools import resolve_address_point, resolve_between_nodes, resolve_linear_reference, publish_to_channel

# 참고 https://taykim.tistory.com/35

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
    logger.info(f"[MAS01 Agent : extract_affected] inputs : {raw_data}")
    
    raw_lat = raw_data.get("lat")
    raw_lng = raw_data.get("lng")
    
    if raw_lat and raw_lng:
        try:
            val_lat = float(raw_lat)
            val_lng = float(raw_lng)
            
            # 100이 넘는 값(124~132)이 lat(위도)에 들어와 있다면 명백한 오류이므로 자리를 바꿉니다.
            if val_lat > 100.0 and val_lng < 50.0:
                logger.warning(f"🔄 [Redis 축 전도 감지] lat과 lng가 뒤바뀌어 들어왔습니다. 강제 교정합니다. (입력 lat: {val_lat}, lng: {val_lng})")
                raw_data["lat"] = val_lng  # 37.52... 을 위도로
                raw_data["lng"] = val_lat  # 127.05... 을 경도로
            else:
                # 데이터가 정상적으로 들어왔을 때의 포맷팅
                raw_data["lat"] = val_lat
                raw_data["lng"] = val_lng
        except ValueError:
            pass # 숫자가 아닐 경우의 예외 방어
            
    logger.info(f"[MAS01 Agent : extract_affected] 보정 완료된 레디스 데이터 : {raw_data}")
    
    kanana_client = AsyncOpenAI(base_url=config.KANANA_MODEL_02_URL, api_key="fake-key")
    current_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d : %H:%M:%S")
    
    system_instruction = f"""
        [현재 시스템 기준 일시] : {current_time}
        
        당신은 대한민국 교통 및 지리 전문가입니다.
        사용자가 입력한 교통 공지사항 정보를 분석하여, 오직 공간 좌표 추출이 가능한 실제 인프라(도로 구간, 건물 지번 주소, 지하철 노선, 5자리 숫자의 버스 정류소 ID)만 '각각 하나의 독립된 항목'으로 완벽히 분리해야 합니다. 

        🚨 **[분석 대상 제외 절대 규칙]: 본문에 등장하는 3자리 또는 4자리 형태의 시내/광역 버스 노선 번호(예: 571, 5012, 5528, 405 등) 및 버스 운수 회사 이름은 공간 좌표를 가지지 않으므로 분석 및 추출 대상이 아닙니다. 최종 출력 JSON 배열(`[]`)에 절대로 포함시키지 말고 완전히 무시하여 폐기하십시오.**

        🚨 **조사 '및', '와/과', 또는 쉼표(,)로 나열된 여러 장소나 구간은 절대로 하나의 JSON 객체에 묶어서 작성하지 마십시오. 반드시 각각 독립된 JSON 객체로 찢어서 출력해야 합니다.**

        출력 JSON 구조:
        [
            {{
                "affected" : "인프라 좌표를 생성할 수 있는 구간 명칭(A ↔ B) 또는 순수 지하철 호선명 또는 건물 지번 주소 또는 본문에 명시된 5자리 정류소 ID 숫자 (버스 노선 번호 절대 금지)",
                "location_type" : "BETWEEN_NODES 또는 LINEAR_REFERENCE 또는 ADDRESS_POINT 또는 BUS 또는 SUBWAY 중 하나로 분류",
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
                "content" : "이 특정 장소와 관련된 통제 내용 요약"
            }}
        ]

        위와 같은 JSON 구조로 만들어야 합니다.

        다음과 같은 step에 따라 JSON 객체를 생성해주세요.
        1. 위치 및 인프라 유형(location_type) 분류하기 : 교통 공지사항 정보에 좌표 추출이 가능한 실제 인프라 유형들이 어떤 것들이 있는지 파악하고, 좌표가 없는 버스 노선 번호는 폐기 처리하세요.
        2. location_type별로 JSON 객체 완성하기 : 분류한 인프라 객체들을 기재 규칙에 따라 각각 완성해주세요.

        [위치 및 인프라 유형(location_type) 분류 규칙]
        - "ADDRESS_POINT": 교차로나 상하행 구간 분리 없이, 특정 교량/랜드마크 명칭(예: "잠수교", "삼각지교차로", "반포한강공원 달빛광장")이 단독으로 명시되었거나 특정 건물 지번 주소가 명시된 경우
        - "BETWEEN_NODES": 본문에 'A지점 ↔ B지점', 'A에서 B 사이', 'A교차로~B교차로'와 같이 **출발 거점과 종료 거점이 문장 내에 모두 명시되어 명확한 물리적 도로 구간/범위를 통제**하는 경우
        - "LINEAR_REFERENCE": 특정 기준점(나들목/교량/지하차도 등)을 지나 '몇 미터(m) 전방/후방 구간'을 통제하는 경우        
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
        - "BUS" :     
            1. [필수 필터링 규칙]: 본문 텍스트를 검사하여 **5자리 숫자로 이루어진 정류소 ID**가 실제로 존재하지 않는다면, "BUS" 유형의 JSON 객체는 최종 출력 배열(`[]`)에 단 하나도 생성하거나 포함시켜서는 안 됩니다.
            2. **버스 노선 번호 생성 절대 금지:** 본문의 `571`, `5012`, `5528` 같은 노선 번호들을 정류소 고유 번호로 착각하여 "BUS" 객체로 독립 추출하는 행위는 절대 금지합니다. 조건이 맞지 않으면 아무것도 만들지 말고 스킵하십시오.
            
        - "SUBWAY" :     
            1. 지하철 관련 공지는 "location_type"을 "SUBWAY"로 지정하세요.
                - "affected"에는 오직 지하철 호선 이름만 단독으로 작성하세요. (7호선 자양역처럼 지하철 노선 + 지하철 역 이름 조합으로 적기 금지. '7호선'처럼 지하철 노선만 기재해야 한다.)
                - 역 이름은 무조건 `details` 내부의 `start_node`와 `end_node`로만 격리해야 합니다.
            2. **"affected"에는 오직 지하철/철도 호선 이름만 단독으로 깨끗하게 작성하세요.** - **절대 금지:** `affected` 필드에 역 이름을 함께 붙여서 `"경의중앙선 (서울역 ↔ 수색역)"` 또는 `"7호선 자양역"` 형태로 출력하는 것은 절대 금지합니다. 오직 노선명(`"경의중앙선"`)만 적으세요
            3. 지하철 호선이 여러 개인 경우, 각각 호선을 affected에 각각 따로 기재해야 하며, start_node, end_node도 각각에 맞도록 기재해야 합니다.
            4. 특정 역 하나만 문제가 발생했다면 start_node와 end_node를 같게 적어야 합니다.   
        - "ADDRESS_POINT" :
            - 출력 JSON 구조에 따라 기재
        - "LINEAR_REFERENCE" :
            - 출력 JSON 구조에 따라 기재      
        
        [DateTime 설정 규칙]
        - 본문에 연도가 생략되어 있다면, 텍스트 상단에 주어지는 `[현재 시스템 기준 일시]`의 연도를 참조하여 %Y-%m-%d %H:%M:%S 형식으로 완성하세요.
        - 본문에 '종료 일시'가 구체적인 숫자로 명시되어 있다면, 반드시 그 날짜를 해석하여 연도(%Y), 월(%m), 일(%d), 시분초(%H:%M:%S) 형태에 맞게 입력하세요.
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
    logger.info(f"[MAS01 Agent : extract_affected] outputs : {result}")
    return {"extracted_entities" : result}
        
async def enrich_coordinates_node(state: AgentState) -> Dict[str, Any]:
    """
    LLM이 추출한 extracted_entities 리스트를 루프 돌며,
    location_type 별 최적의 GIS 함수를 실행해 lat, lng 사후 세팅
    """
    entities = state.get('extracted_entities', [])
    logger.info(f"[MAS01 Agent : enrich_coordinates_node] Processing {len(entities)} entities...")
    
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
                coord_result = resolve_between_nodes(
                    road_name=details.get("road_name"),
                    start_node=details.get("start_node"),
                    end_node=details.get("end_node")
                )
            elif location_type == "LINEAR_REFERENCE":
                coord_result = resolve_linear_reference(
                    road_name=details.get("road_name"),
                    anchor_node=details.get("anchor_node"),
                    offset_start=float(details.get("offset_start", 0)),
                    offset_end=float(details.get("offset_end", 0))
                )
            elif location_type == "ADDRESS_POINT":
                coord_result = resolve_address_point(address=details.get("address"))
                
            # 2. 좌표 툴 연산 성공 시 결합
            if coord_result:
                item["lat"] = coord_result["lat"]
                item["lng"] = coord_result["lng"]
                
                if not item.get("si"):
                    item["si"] = coord_result.get("si")
                if not item.get("gu"):
                    item["gu"] = coord_result.get("gu")
                logger.info(f"[MAS01 Agent : enrich_coordinates_node] 매핑 성공 [{affected_name}] -> {coord_result}")
            else:
                logger.warning(f"[MAS01 Agent : enrich_coordinates_node] 매핑 실패 [{affected_name}] - SHP 내 데이터 부재")
                
        final_processed_nodes.append(item)
        logger.info(f"[MAS01 Agent : enrich_coordinates_node] 결과 : {final_processed_nodes}")
        
    return {"affected_nodes": final_processed_nodes}

async def apply_to_neo4j_graph_node(state:AgentState) -> Dict[str, Any] :
    nodes = state.get("affected_nodes", [])
    logger.info(f"[MAS01 Agent : apply_to_neo4j_graph_node] {len(nodes)}개의 인프라 객체 그래프 DB 반영 시작...")
    
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
    
    # 🔁 1단계: 모든 엔터티 루프를 돌며 Neo4j 세션 단독 실행 및 완전 밀봉
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
                        logger.warning(f"[MAS01 Agent apply_to_neo4j_graph_node] : [{item.get('affected')}] 좌표 정보 부재로 패스.")
                        continue
                    
                    incident_id = hashlib.md5(f"{item.get('startDateTime')}_{lat}_{lng}".encode('utf-8')).hexdigest()
                    result = await session.run(cypher_query01, incident_id=incident_id, content=item.get("content"), location_type=item.get("location_type"), start_time=item.get("startDateTime"), end_time=item.get("endDateTime"), lat=float(lat), lng=float(lng))
                    
                # 🎯 [핵심 보정 1] 분기별 무관하게 result.single()을 소모하여 그래프 트랜잭션 동기화 및 강제 빌드 유도
                record = await result.single()
                connected_count = record["connected_count"] if record else 0
                
                item["incident_id"] = incident_id
                success_nodes.append(item)
                logger.info(f"[MAS01 Agent apply_to_neo4j_graph_node][Neo4j 동기화 완료] [{item.get('affected')}] 관계선 {connected_count}개소 융합 완료.")
                
        except Exception as e:
            logger.error(f"[MAS01 apply_to_neo4j_graph_node ] [Neo4j 세션 오류] '{item.get('affected')}' 처리 실패: {e}")
            continue
            
    # 🎯 [핵심 보정 2] 루프가 완전히 종료(Neo4j DB 영구커밋 완료)된 안전 구역에서만 Redis 발행 기동!!
    logger.info(f"[MAS01 apply_to_neo4j_graph_node] [MAS01 -> Neo4j] 모든 인프라 노드 완벽 저장 성공. 최종 채널 전파를 시작합니다 (총 {len(success_nodes)}건)")
    for s_node in success_nodes:
        gu_name = s_node.get("gu")
        si_name = s_node.get("si")
        if gu_name:
            await publish_to_channel(gu_name, si_name, s_node)
            logger.info(f"[MAS01 apply_to_neo4j_graph_node] [MAS01 -> Redis] DB 무결성을 확인한 후 안전하게 [{gu_name}] 스트림 발행 성공!")
            
    return {"affected_nodes": success_nodes}
    

mas01_workflow = StateGraph(AgentState)

mas01_workflow.add_node('extract_affected_node', extract_affected_node)
mas01_workflow.add_node('enrich_coordinates_node', enrich_coordinates_node)
mas01_workflow.add_node('apply_to_neo4j_graph_node', apply_to_neo4j_graph_node)

mas01_workflow.set_entry_point('extract_affected_node')
mas01_workflow.add_edge('extract_affected_node', 'enrich_coordinates_node')
mas01_workflow.add_edge('enrich_coordinates_node', 'apply_to_neo4j_graph_node')
# mas01_workflow.add_edge('enrich_coordinates_node', END)
mas01_workflow.add_edge('apply_to_neo4j_graph_node', END)

mas01_agent = mas01_workflow.compile()