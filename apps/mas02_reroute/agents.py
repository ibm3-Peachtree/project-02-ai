# apps/mas02_reroute/agents.py

from typing import TypedDict, Dict, List, Any
import json
import re

from openai import AsyncOpenAI
from langgraph.graph import StateGraph, END

import config
from config import logger
from apps.mas02_reroute.rerouting import TransportApp
from apps.mas02_reroute.tools import get_incident_meta_data, calculate_distance

class ReroutingAgentState(TypedDict) :
    incident_id : str # 필요한가?
    user_id : str
    user_live_route_xy : List[Dict[str, Any]]
    
    user_init : Dict[str, Any]
    extracted_nodes : List[Dict[str, Any]]
    resolved_node_ids: Dict[str, Any]
    final_rerouting_paths : List[Dict[str, Any]]
    

async def fetch_user_realtime_context(state: ReroutingAgentState) -> Dict[str, Any]:
    user_id = state["user_id"]
    incident_id = state["incident_id"]
    redis_client = config.redis_client
    
    logger.info(f"[MAS02 agents.py][Step 1] 유저 {user_id}의 GPS-경로 인덱스 정밀 동기화...")
    
    # 1. GPS 로그 파싱
    raw_list = await redis_client.lrange(f"location:user:{user_id}", 0, -1)
    user_current_gps = []
    if raw_list:
        user_current_gps = [json.loads(r.decode('utf-8')) if isinstance(r, bytes) else json.loads(r) for r in raw_list]
        
    user_live_route_xy = state["user_live_route_xy"]
    
    # 유저의 최신 GPS 위치와 가장 가까운 실제 버스 정거장(인덱스)을 찾습니다.
    current_station_idx = 1  # 기본값 (종로2가)
    if user_current_gps and user_live_route_xy:
        latest_gps = user_current_gps[-1]  # 최신 GPS 스냅샷
        u_lat = latest_gps.get("latitude")
        u_lng = latest_gps.get("longitude")
        
        min_dist = float('inf')
        for idx, st in enumerate(user_live_route_xy):
            if st.get("x") is None or st.get("y") is None:
                continue
            dist = calculate_distance(u_lat, u_lng, float(st["y"]), float(st["x"]))
            if dist < min_dist:
                min_dist = dist
                current_station_idx = idx

    # 2. 사고 메타 데이터 확보
    incident_meta = await get_incident_meta_data(incident_id)
    incident_content = incident_meta.get("incident", "서울역 일대 통제") if incident_meta else "통제 발생"
    
    # LLM이 딴눈 팔지 못하도록 현재 인덱스와 유효 범위를 명확히 규격화하여 컨텍스트화합니다.
    user_init = {
        "user_gps": user_current_gps[-3:],  # 최근 3개 스냅샷만 압축해서 전달
        "user_live_route_xy": user_live_route_xy,
        "current_station_index": current_station_idx,  # "유저가 현재 이 인덱스 정거장 근처에 있음"을 명시!
        "incident": incident_content
    }
    
    return {"user_init": user_init}
    
async def extract_routing_station_names(state:ReroutingAgentState) -> List[Dict[str, Any]] :
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
    
    user_init = state.get("user_init", {})
    current_idx = user_init.get("current_station_index", 1)
    
    system_instruction = f"""
        당신은 유저의 실시간 위치와 대중교통 노선 배열을 분석하여 최적의 '우회 시작 정거장'을 선정하는 네비게이션 AI입니다.
        
        [현재 상황 스펙]
        - 유저가 현재 지나고 있거나 가장 인접한 정거장의 인덱스는 [{current_idx}]번 정거장입니다.
        
        [선정 규칙]
        1. 시스템 연산 버퍼 시간(10초) 및 버스의 주행 속도를 고려할 때, 유저는 곧 [{current_idx}]번 정거장을 지나쳐 전진하게 됩니다.
        2. 따라서, `start_node`는 반드시 제공된 `user_live_route_xy` 배열 내에서 현재 인덱스보다 뒤에 위치한 인덱스, 즉 [{current_idx + 1}]번 또는 [{current_idx + 2}]번 정거장 중에서 엄선해야 합니다.
        3. 절대 [{current_idx}]보다 작거나, 한강을 건너가 버리는 터무니없이 먼 인덱스(예: 배열 끝자락의 봉현초등학교 등)를 선택하지 마십시오. 유저가 바로 하차 및 우회할 수 있는 '직후 전방 정거장'이어야 합니다.
        
        [출력 JSON 규격]
        {{
            "reason": "현재 {current_idx}번 정거장 위치 대비 연산 시간 동안 주행할 거리를 감안하여 바로 다음 대안 정거장을 선택한 이유 기술",
            "start_node": {{
                "stationName": "선택한 정거장의 실제 이름 (배열에서 그대로 복사)",
                "x": 선택한 정거장의 x 좌표,
                "y": 선택한 정거장의 y 좌표,
                "arsID": "선택한 정거장의 arsID",
                "type": "bus",
                "no": "bus:501"
            }}
        }}
    """
    kanana_client = AsyncOpenAI(base_url=config.KANANA_MODEL_02_URL, api_key="fake-key")
    response = await kanana_client.chat.completions.create(
        model="kakaocorp/kanana-1.5-8b-instruct-2505",
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": f"route_list: {user_init.get('user_live_route_xy')}\nincident: {user_init.get('incident')}"}
        ],
        max_tokens=2000,
        temperature=0.1,
    )
    
    outputs = json.loads(extract_json(response.choices[0].message.content))
    is_not_last_node = True
    idx = -1
    
    user_xy_routes = user_init.get("user_live_route_xy", [])
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
    logger.info(f"[MAS02 agents.py extract_routing_station_names] 노드 추출 완료 : {outputs}")
    return {
        "extracted_nodes" : outputs
    }
    
async def resolve_neo4j_node_ids(state: ReroutingAgentState) -> Dict[str, Any]:
    node = state.get("extracted_nodes", {})
    start_node = node.get("start_node")
    end_node = node.get("end_node")
    
    logger.info(f"[MAS02 agents.py][Step 3] 제공된 스펙 기반 마스터 node_id 추출 시작...")

    async def get_bus_node_id(target_node: Dict) -> str:
        ars_id = str(target_node.get("arsID")).strip()
        
        query = """
            MATCH (s:Station {is_master: true, type: "BUS", ars_id: $ars_id})
            RETURN s.node_id AS node_id LIMIT 1
        """
        async with config.neo4j_client.session() as session:
            result = await session.run(query, ars_id=ars_id)
            record = await result.single()
            return record["node_id"] if record else None
    
    async def get_subway_node_id(target_node: Dict) -> str:
        name = target_node.get("stationName", "").replace("역", "").strip()
        
        # "subway:수도권 1호선" -> "수도권 1호선" 또는 "1호선" 추출
        raw_no = target_node.get("no", "")
        line_name = raw_no.split(":")[1].strip() if ":" in raw_no else raw_no
        line_name = line_name.split(' ')[1]
        
        # 1. 먼저 호선 정보(route_id)가 명확히 살아있는 하위 노드(is_master: false)를 타겟팅합니다.
        # 2. 하위 노드의 node_id(예: "1907_1호선") 앞부분을 잘라내어 마스터의 station_cd(예: "1907")와 매칭합니다.
        # 3. 이로써 환승역이더라도 LLM이 지정한 '호선'에 속한 정확한 역의 마스터 node_id를 보장합니다.
        query = """
            MATCH (sub:Station {is_master: false, type: "SUBWAY"})
            WHERE (sub.name CONTAINS $name) 
              AND (sub.route_id = $line_name OR sub.name CONTAINS $line_name)
            
            # 하위 노드의 node_id가 "1907_1호선" 형태이므로 split하여 "1907" 추출
            WITH split(sub.node_id, "_")[0] AS target_cd
            
            # 추출한 코드를 가진 실제 마스터 노드를 단 한 건 매칭
            MATCH (m:Station {is_master: true, type: "SUBWAY", station_cd: target_cd})
            RETURN m.node_id AS node_id LIMIT 1
        """
        
        async with config.neo4j_client.session() as session:
            result = await session.run(query, name=name, line_name=line_name)
            record = await result.single()
            
            if record:
                return record["node_id"]
            
            backup_query = """
                MATCH (m:Station {is_master: true, type: "SUBWAY"})
                WHERE m.name = $name OR m.name CONTAINS $name
                RETURN m.node_id AS node_id LIMIT 1
            """
            backup_res = await session.run(backup_query, name=name)
            backup_rec = await backup_res.single()
            return backup_rec["node_id"] if backup_rec else None

    # 문자열 "null"이나 파이썬 None 둘 다 방어하기 위해 조건 세분화
    if start_node.get("arsID") and str(start_node.get("arsID")).lower() != "null":
        start_node_id = await get_bus_node_id(start_node)
    else: 
        start_node_id = await get_subway_node_id(start_node)
    
    # 🎯 종료 노드 (End Node) ID 추출 분기
    if end_node.get("arsID") and str(end_node.get("arsID")).lower() != "null":
        end_node_id = await get_bus_node_id(end_node)
    else: 
        end_node_id = await get_subway_node_id(end_node)
    
    logger.info(f"🎯 [MAS02 agents.py resolve_neo4j_node_ids ID 매핑 완료] 시작 ID: {start_node_id} / 종료 ID: {end_node_id}")
    
    return {
        "resolved_node_ids": {
            "start_node_id": start_node_id,
            "end_node_id": end_node_id
        }
    }
    
async def generate_and_format_routes(state: ReroutingAgentState) -> Dict[str, Any]:
    nodes = state.get("resolved_node_ids")
    start_node = nodes.get("start_node_id")
    end_node = nodes.get("end_node_id")
    
    app = TransportApp()
    
    await app.delete_gds_graph()
    await app.build_gds_graph1()
    await app.build_gds_graph2()
    await app.build_gds_graph3()
    
    combined_records = []
    path_counter = 0
    
    # 🎯 [수정] 경로 탐색 쿼리들도 async def 스펙이므로 await를 적용해 결과를 패칭합니다.
    query_1_result = await app.get_optimal_path1(start_node, end_node)
    for r in query_1_result:
        r['path_idx'] = path_counter; combined_records.append(r); path_counter += 1
        
    query_2_result = await app.get_optimal_path2(start_node, end_node)
    for r in query_2_result:
        r['path_idx'] = path_counter; combined_records.append(r); path_counter += 1
        
    query_3_result = await app.get_optimal_path3(start_node, end_node)
    for r in query_3_result:
        r['path_idx'] = path_counter; combined_records.append(r); path_counter += 1
        
    logger.info(f"📦 독립 3-Query 최적 원본 레코드 취합 완료 (총 후보군: {len(combined_records)})") 
    
    # format_perfect_routing_paths는 순수 연산 함수(동기식)이므로 기존 구조 유지
    final_routes = app.format_perfect_routing_paths(combined_records)
    
    logger.info("\n================= 🗺️ 최종 요구사항 만족 취합 대안 경로 출력 =================")
    final_routes = json.dumps(final_routes, ensure_ascii=False, indent=2)
    
    return {
        "final_rerouting_paths" : final_routes
    }
    
mas02_workflow = StateGraph(ReroutingAgentState)

mas02_workflow.add_node('fetch_user_realtime_context', fetch_user_realtime_context)
mas02_workflow.add_node('extract_routing_station_names', extract_routing_station_names)
mas02_workflow.add_node('resolve_neo4j_node_ids', resolve_neo4j_node_ids)
mas02_workflow.add_node('generate_and_format_routes', generate_and_format_routes)

mas02_workflow.set_entry_point('fetch_user_realtime_context')
mas02_workflow.add_edge('fetch_user_realtime_context', 'extract_routing_station_names')
mas02_workflow.add_edge('extract_routing_station_names', 'resolve_neo4j_node_ids')
mas02_workflow.add_edge('resolve_neo4j_node_ids', 'generate_and_format_routes')
mas02_workflow.add_edge('generate_and_format_routes', END)

mas02_agent = mas02_workflow.compile()