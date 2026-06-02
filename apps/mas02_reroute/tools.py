# apps/mas02_reroute/tools.py
import math
import json

import config
from config import logger

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
    # 💡 설계서 반영: 가상 플랫폼과 물리 마스터 정류소 간의 관계(예: MATCH 등)를 기반으로 
    # 마스터 노드의 x, y 좌표를 Distinct하게 긁어옵니다.
    # (만약 스키마 상 가상 노드가 마스터 ID를 접두사로 가지므로 ID 기반 매칭도 가능합니다)
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
            logger.info(f"📍 [Neo4j] 통제 구역 내 물리 좌표 {len(records)}개 확보 완료.")
            return records
    except Exception as e:
        logger.error(f"❌ [Neo4j Coordinate Tool Error] 통제 좌표 추출 실패: {e}")
        return []

async def get_active_users_by_coordinates(affected_coords: list) -> list:
    """
    Redis의 XY 경로 키들을 SCAN하여 최신 스냅샷을 추린 뒤,
    사고 통제 좌표와 정확히 일치하는 정류소/역을 밟는 유저 리스트(recoId)를 반환합니다.
    """
    redis_client = config.redis_client
    user_latest_keys = {}

    match_pattern = "routine:route:xy:user:*"
    
    keys = await redis_client.keys(match_pattern)
    
    user_latest_keys = {}
    
    if keys:
        for key in keys:
            tokens = key.split(":")  # ['routine', 'route', 'xy', 'user', '2', '3']
            if len(tokens) < 6: 
                continue
            
            user_id = tokens[4]   # "2"
            seq_num = int(tokens[5]) # 3
            
            # 유저 ID별로 가장 최신(가장 높은) seq_num을 가진 키만 바인딩
            if user_id not in user_latest_keys or seq_num > user_latest_keys[user_id]["seq"]:
                user_latest_keys[user_id] = {"key": key, "seq": seq_num}
                
    logger.info(f"[MAS02 get_active_users_by_coordinates] 1. [Redis Scan] 최신 스냅샷 매핑 완료. 활성 유저 후보군: {list(user_latest_keys.keys())}명")

    # 2. 좌표 비교 연산 수행
    affected_user_reco_ids = []

    for user_id, info in user_latest_keys.items():
        raw_data = await redis_client.get(info["key"])
        if not raw_data: 
            continue
        
        user_xy_list = json.loads(raw_data)
        is_user_affected = False
        
        for node in user_xy_list:
            if node.get("x") is None or node.get("y") is None:
                continue
                
            # 유저 좌표 꺼내기 (X는 경도, Y는 위도)
            u_lng = float(node.get("x"))
            u_lat = float(node.get("y"))
            
            # 🎯 [10m 저격 연산] 이번 사고로 통제된 모든 좌표들을 돌며 10m 이내에 걸리는지 확인
            for aff_coord in affected_coords:
                aff_lng = float(aff_coord['x'])
                aff_lat = float(aff_coord['y'])
                
                # 하베사인 함수로 두 지점의 정확한 m 거리 계산
                actual_distance = calculate_distance_meters(u_lat, u_lng, aff_lat, aff_lng)
                
                # 📏 거리가 10미터 이내라면 저격 성공!
                if actual_distance <= 10.0:
                    is_user_affected = True
                    logger.warning(
                        f"🚨 [10m 반경 저격 성공] 유저 {user_id}번이 통제 구역과 "
                        f"정확히 {actual_distance:.2f}m 거리에 위치한 '{node.get('stationName')}' 구역을 통과 예정입니다!"
                    )
                    break # 내부 사고 좌표 루프 탈출
                    
            if is_user_affected:
                break # 다음 유저 노드 탐색 중단하고 다음 유저로 이동
                
        if is_user_affected:
            affected_user_reco_ids.append(int(user_id))
            
    return affected_user_reco_ids