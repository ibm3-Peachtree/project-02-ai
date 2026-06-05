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
    # 가상 플랫폼과 물리 마스터 정류소 간의 관계(예: MATCH 등)를 기반으로 
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

async def get_active_users_by_coordinates(affected_coords: list, incident_id:str) -> list:
    """
    Redis의 XY 경로 키들을 SCAN하여 최신 스냅샷을 추린 뒤,
    사고 통제 좌표와 정확히 일치하는 정류소/역을 밟는 유저 리스트(recoId)를 반환합니다.
    """
    redis_client = config.redis_client
    user_latest_keys = {}

    match_pattern = "routine:live:xy:user:*"
    keys = await redis_client.keys(match_pattern)
    
    if keys:
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            tokens = key.split(":")
            if len(tokens) < 5: continue
            user_id = tokens[4]
            user_latest_keys[user_id] = {"key": key}
                
    logger.info(f"[MAS02 get_active_users_by_coordinates] 1. [Redis Scan] 최신 스냅샷 매핑 완료. 활성 유저 후보군: {list(user_latest_keys.keys())}명")

    # 🎯 affected_coords는 완벽한 파이썬 list이므로 전처리 불필요! 일체 터치하지 않습니다.
    affected_user_reco_ids = []
    affected_user_xy = []

    for user_id, info in user_latest_keys.items():
        reroute_history_key = f"user:{user_id}:reroute:history:{incident_id}"
        is_already_rerouted = await redis_client.exists(reroute_history_key)
        
        if is_already_rerouted:
            continue 

        raw_data = await redis_client.get(info["key"])
        if not raw_data: continue
        
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode('utf-8')
            
        # [결정적 보정] Redis 내부 값이 비어있거나 JSON 형태( [, { )가 아니면 
        # json.loads를 타지 않고 영리하게 스킵하여 DecodeError를 완벽 차단합니다.
        raw_data = raw_data.strip()
        if not raw_data or not raw_data.startswith(('[', '{')):
            logger.warning(f"⚠️ 유저 {user_id}의 Redis 데이터가 올바른 JSON 포맷이 아닙니다. 스킵합니다.")
            continue
            
        user_xy_list = json.loads(raw_data)
        if isinstance(user_xy_list, str):
            user_xy_list = json.loads(user_xy_list)
            
        is_user_affected = False
        
        for node in user_xy_list["routeXYDtoList"]:
            # if isinstance(node, str):
            #     node = node.strip()
            #     if node.startswith('{'):
            #         node = json.loads(node)
            #     else:
            #         continue
                
            if not node or node.get("x") is None or node.get("y") is None: 
                continue
                
            u_lng = float(node.get("x"))
            u_lat = float(node.get("y"))
            
            for aff_coord in affected_coords:
                # 🎯 affected_coords 내부 요소 역시 순수 dict이므로 변환 없이 직통 매핑!
                if not aff_coord or aff_coord.get("x") is None or aff_coord.get("y") is None: 
                    continue
                    
                aff_lng = float(aff_coord['x'])
                aff_lat = float(aff_coord['y'])
                
                actual_distance = calculate_distance_meters(u_lat, u_lng, aff_lat, aff_lng)
                
                if actual_distance <= 50.0:
                    is_user_affected = True
                    affected_user_xy.append(user_xy_list['routeXYDtoList'])
                    logger.warning(f"[MAS02 tools.py get_active_users_by_coordinates][신규 난입 포착] 유저 {user_id}번이 통제 구역 {actual_distance:.2f}m 거리에 진입!")
                    break 
                    
            if is_user_affected: break 
                
        if is_user_affected:
            affected_user_reco_ids.append(int(user_id))
            await redis_client.set(name=reroute_history_key, value="DONE", ex=3600)
            
    return affected_user_reco_ids, affected_user_xy