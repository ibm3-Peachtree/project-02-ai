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
    
    user_latest_keys = {}
    
    if keys:
        for key in keys:
            tokens = key.split(":")  # ['routine', 'route', 'xy', 'user', '2', '3']
            user_id = tokens[4]   # "2"
                
    logger.info(f"[MAS02 get_active_users_by_coordinates] 1. [Redis Scan] 최신 스냅샷 매핑 완료. 활성 유저 후보군: {list(user_latest_keys.keys())}명")

    # 2. 좌표 비교 연산 수행
    affected_user_reco_ids = []

    for user_id, info in user_latest_keys.items():
        # 🎯 [치트키 방어막 1] 이 유저가 이번 사고(incident_id)로 이미 우회로를 안내받았는지 Redis 이력 확인
        reroute_history_key = f"user:{user_id}:reroute:history:{incident_id}"
        is_already_rerouted = await redis_client.exists(reroute_history_key)
        
        if is_already_rerouted:
            #  SKIP: 유저 A는 이미 10초 전(혹은 과거)에 우회로를 쏴줬으므로 패스합니다!
            continue 

        raw_data = await redis_client.get(info["key"])
        if not raw_data: continue
        
        user_xy_list = json.loads(raw_data)
        is_user_affected = False
        
        for node in user_xy_list:
            if node.get("x") is None or node.get("y") is None: continue
                
            u_lng = float(node.get("x"))
            u_lat = float(node.get("y"))
            
            for aff_coord in affected_coords:
                aff_lng = float(aff_coord['x'])
                aff_lat = float(aff_coord['y'])
                
                actual_distance = calculate_distance_meters(u_lat, u_lng, aff_lat, aff_lng)
                
                if actual_distance <= 10.0:
                    is_user_affected = True
                    logger.warning(f"[MAS02 tools.py get_active_users_by_coordinates][신규 난입 포착] 유저 {user_id}번이 통제 구역 {actual_distance:.2f}m 거리에 진입!")
                    break 
                    
            if is_user_affected: break 
                
        if is_user_affected:
            affected_user_reco_ids.append(int(user_id))
            
            # 그물망에 걸려 가로채기 성공한 유저는 Redis에 이력을 남겨둡니다.
            # TTL(ex)은 1시간(3600초) 정도로 지정하여, 사고 대응 바운더리 안에서 중복 발송을 막습니다.
            await redis_client.set(name=reroute_history_key, value="DONE", ex=3600)
            
    return affected_user_reco_ids