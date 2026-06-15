# apps/mas02_reroute/tools.py
import math
import json

import config

def calculate_distance_meters(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    하베사인(Haversine) 공식을 사용하여 두 위경도 좌표 사이의 실제 거리를 미터(m) 단위로 구합니다.
    """
    # 지구 반지름 (미터 단위)
    R = 6371000.0
    
    # 라디안 변환
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)
    
    # 하베사인 공식 연산
    a = (math.sin(delta_phi / 2.0) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    
    return R * c

async def get_affected_coordinates_from_neo4j(incident_id: str) -> list:
    """
    Neo4j에서 현재 사고 ID로 인해 통제된 가상 플랫폼들과 연결된 
    물리 마스터 노드의 x(경도), y(위도) 좌표 리스트를 반환합니다.
    """
    # 가상 플랫폼과 물리 마스터 정류소 간의 관계(예: MATCH 등)를 기반으로 
    # 마스터 노드의 x, y 좌표를 Distinct하게 받음
    # (만약 스키마 상 가상 노드가 마스터 ID를 접두사로 가지므로 ID 기반 매칭도 가능)
    neo4j_query = """
        MATCH (virtual:Station {is_master: false})-[:AFFECTED_BY]->(i:Incident {id: $incident_id})
        // 🎯 1. ID 문자열을 '_' 기준으로 쪼개서 [0]번째(앞자리)만 가공합니다.
        WITH virtual, split(virtual.node_id, '_')[0] AS master_id

        // 2. 가공된 master_id와 가상 노드의 좌표들을 DISTINCT로 묶어 완벽하게 중복을 제거합니다.
        RETURN DISTINCT master_id, virtual.x as x, virtual.y as y
    """
    try:
        async with config.neo4j_client.session() as session:
            result = await session.run(neo4j_query, incident_id=incident_id)
            records = await result.data()
            
            # 결과 예시: [{'x': 126.941659, 'y': 37.514206}, ...]
            config.logger.info(f"📍 [Neo4j] 통제 구역 내 물리 좌표 {len(records)}개 확보 완료.")
            return records
    except Exception as e:
        config.logger.error(f"❌ [Neo4j Coordinate Tool Error] 통제 좌표 추출 실패: {e}")
        return []

async def get_active_users_by_coordinates(affected_coords: list, incident_id:str) -> list:
    """
    Redis의 XY 경로 키들을 SCAN하여 최신 스냅샷을 추린 뒤,
    사고 통제 좌표와 정확히 일치하는 정류소/역을 밟는 유저 리스트(recoId)를 반환합니다.
    """
    user_latest_keys = {}

    match_pattern = "routine:live:my:xy:user:*"
    keys = await config.redis_client.keys(match_pattern)
    
    if keys:
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            tokens = key.split(":")
            if len(tokens) < 5: continue
            user_id = tokens[5]
            user_latest_keys[user_id] = {"key": key}
                
    config.logger.info(f"[MAS02 get_active_users_by_coordinates] 1. [Redis Scan] 최신 스냅샷 매핑 완료. 활성 유저 후보군: {list(user_latest_keys.keys())}명\n incident_id : {incident_id}")

    affected_user_reco_ids = []
    affected_user_xy = []

    for user_id, info in user_latest_keys.items():
        reroute_history_key = f"user:{user_id}:reroute:history:{incident_id}"
        is_already_rerouted = await config.redis_client.exists(reroute_history_key)
        
        if is_already_rerouted:
            continue 

        raw_data = await config.redis_client.get(info["key"])
        if not raw_data: continue
        
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode('utf-8')
            
        raw_data = raw_data.strip()
        if not raw_data or not raw_data.startswith(('[', '{')):
            config.logger.warning(f"⚠️ 유저 {user_id}의 Redis 데이터가 올바른 JSON 포맷이 아닙니다. 스킵합니다.")
            continue
            
        user_xy_list = json.loads(raw_data)
        if isinstance(user_xy_list, str):
            user_xy_list = json.loads(user_xy_list)
            
        target_nodes = []
        if isinstance(user_xy_list, dict):
            target_nodes = user_xy_list.get("routeXYDtoList", [])
        elif isinstance(user_xy_list, list):
            target_nodes = user_xy_list
            
        is_user_affected = False
        
        for node in target_nodes:
            if not node or node.get("x") is None or node.get("y") is None: 
                continue
                
            u_lng = float(node.get("x"))
            u_lat = float(node.get("y"))
            
            for aff_coord in affected_coords:
                if not aff_coord or aff_coord.get("x") is None or aff_coord.get("y") is None: 
                    continue
                    
                aff_lng = float(aff_coord['x'])
                aff_lat = float(aff_coord['y'])
                
                actual_distance = calculate_distance_meters(u_lat, u_lng, aff_lat, aff_lng)
                
                if actual_distance <= 50.0:
                    is_user_affected = True
                    
                    # [보정] 자료형에 상관없이 안전하게 추출된 노드 리스트(또는 원본 리스트)를 append 합니다.
                    affected_user_xy.append(target_nodes)
                    
                    config.logger.warning(f"[MAS02 tools.py get_active_users_by_coordinates][신규 난입 포착] 유저 {user_id}번이 통제 구역 {actual_distance:.2f}m 거리에 진입!")
                    break 
                    
            if is_user_affected: break 
                
        if is_user_affected:
            affected_user_reco_ids.append(int(user_id))
            # 우회 플래그 캐싱은 실제 영향을 받은 유저(is_user_affected == True)일 때만 마킹되도록 if문 내부로 이동했습니다.
            await config.redis_client.set(name=reroute_history_key, value="DONE", ex=3600*24)
            
    return affected_user_reco_ids, affected_user_xy

async def get_incident_meta_data(incident_id: str ) -> dict:
    """
    Redis로부터 특정 사건의 메타(LLM 요약 및 기간) 데이터를 조회하여 
    파이썬 딕셔너리 객체로 반환합니다.
    """
    meta_key = f"incident:meta:{incident_id}"
    
    config.logger.info(f"🔍 [Redis 읽기] 사건 메타 데이터 캐시 스캔 시작: {meta_key}")
    
    try:
        # 1. Redis에서 데이터 읽어오기
        raw_meta = await config.redis_client.get(meta_key)
        
        # 데이터가 아예 존재하지 않는 경우 (캐시 미스) 빈 딕셔너리 리턴
        if not raw_meta:
            config.logger.warning(f"⚠️ [Redis 캐시 미스] 해당 키의 데이터가 존재하지 않거나 만료되었습니다: {meta_key}")
            return {}
            
        # 2. 바이트 타입일 경우 문자열로 안전하게 디코딩
        if isinstance(raw_meta, bytes):
            raw_meta = raw_meta.decode('utf-8')
            
        # 3. JSON 문자열을 파이썬 딕셔너리로 원상복구
        meta_dict = json.loads(raw_meta)
        
        # 만약 내부적으로 문자열이 이중 패킹(Stringified)되어 들어갔을 경우를 대비한 2차 해제 방어선
        if isinstance(meta_dict, str):
            meta_dict = json.loads(meta_dict)
            
        config.logger.info(f"⚡ [Redis 캐시 적중] 성공적으로 메타 정보를 복원했습니다. (사건 요약: {meta_dict.get('incident', '내용 없음')})")
        return meta_dict

    except Exception as e:
        config.logger.error(f"❌ [Redis 읽기 에러] {meta_key} 데이터 파싱 중 실패: {e}")
        return {}
    
def calculate_distance(lat1, lng1, lat2, lng2):
    """두 좌표 사이의 직선 거리를 미터(m) 단위로 계산"""
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)
    a = (math.sin(delta_phi / 2.0) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2)
    return R * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

async def total_cost(final_routes_path):
    config.logger.info(f"MAS02 tools.py total cost 진입")
    for idx, route in enumerate(final_routes_path):
        total_distance = 0
        max_base_fare = 0
        
        # 현재 경로의 모든 세그먼트를 돌며 거리 합산 및 기본요금 후보 추출
        for path_segment in route["path_segments"]:
            if path_segment["type"] == "TRANSIT":
                total_distance += path_segment["total_distance_m"]
                
                # display_name 분석하여 기본요금 한도 설정
                display_name = "".join(path_segment.get("display_name", []))
                
                if "마을버스" in display_name:
                    current_base = 1200
                elif "광역" in display_name or "직행좌석" in display_name:
                    current_base = 3000
                elif "지하철" in display_name or "전철" in display_name or "호선" in display_name:
                    current_base = 1550  # 지하철이 섞이면 베이스는 1,550원
                else:
                    current_base = 1500  # 일반 시내버스 기본요금
                
                if current_base > max_base_fare:
                    max_base_fare = current_base
        
        if max_base_fare == 0:
            route["cost"] = 0
            continue
        
        final_fare = max_base_fare
        
        if total_distance > 10000:
            # 10km를 초과한 순수 초과 거리 계산
            excess_distance = total_distance - 10000
            
            # 5km(5,000m) 마다 100원씩 올림(math.ceil)하여 추가 요금 계산
            extra_fare = math.ceil(excess_distance / 5000) * 100
            final_fare += extra_fare
            
        route["cost"] = final_fare
    config.logger.info(f"MAS02 tools.py total cost 완료")
        
    return final_routes_path