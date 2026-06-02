# apps/mas02_reroute/workers.py

import json
import asyncio

import config
from config import logger
from apps.mas02_reroute.tools import get_affected_coordinates_from_neo4j, get_active_users_by_coordinates

SEOUL_GUS = [
    "강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구",
    "노원구", "도봉구", "동대문구", "동작구", "마포구", "서대문구", "서초구", "성동구",
    "성북구", "송파구", "양천구", "영등포구", "용산구", "은평구", "종로구", "중구", "중랑구"
]

# async def get_all_active_users_from_redis() -> list:
#     """
#     Redis를 SCAN하여 모든 유저의 경로 키를 찾은 뒤,
#     가장 최신 시퀀스 번호(마지막 5분 스냅샷)의 데이터만 추출하여 반환.
#     """
#     redis_client = config.redis_client
#     user_latest_keys = {}
    
#     # 1. 패턴에 맞는 모든 실시간 경로 키 SCAN
#     # 예: routine:route:full:user:1:0, routine:route:full:user:1:1, routine:route:full:user:2:0 ...
#     match_pattern = "routine:route:full:user:*"
#     cursor = 0
    
#     while True:
#         cursor, keys = await redis_client.scan(cursor=cursor, match=match_pattern, count=100)
#         for key_bytes in keys:
#             key_str = key_bytes.decode('utf-8') # routine:route:full:user:1:2
#             tokens = key_str.split(":")
            
#             if len(tokens) < 6:
#                 continue
                
#             user_id = tokens[4]     # "1"
#             seq_num = int(tokens[5]) # 2
            
#             # 딕셔너리에 유저 ID별로 가장 높은 seq_num을 가진 키만 갱신하며 남김
#             if user_id not in user_latest_keys or seq_num > user_latest_keys[user_id]["seq"]:
#                 user_latest_keys[user_id] = {
#                     "key": key_str,
#                     "seq": seq_num
#                 }
                
#         if cursor == 0:
#             break

#     # 2. 추출된 최신 키들에서만 유저 JSON 데이터 바인딩
#     active_users_data = []
#     for user_id, info in user_latest_keys.items():
#         raw_data = await redis_client.get(info["key"])
#         if raw_data:
#             user_json = json.loads(raw_data.decode('utf-8'))
#             active_users_data.append(user_json)
            
#     logger.info(f"[MAS02 workers.py get_all_active_users_from_redis : Redis Scan] 과거 로그 제외, 현재 완벽히 활성화된 유저 {len(active_users_data)}명 확보 완료.")
#     return active_users_data

async def redis_incident_consumer_and_rerouter():
    """
    [MAS02 코어 워커] 
    새로운 사고 스트림이 감지되면 즉시 활성 유저를 땡겨와 우회로를 연산합니다.
    """
    GROUP_NAME = "mas02_reroute_group"
    CONSUMER_NAME = "mas02_worker"
    
    stream_keys = [f"incident:stream:서울특별시:{gu}" for gu in SEOUL_GUS]
    
    redis_client = config.redis_client
    
    logger.info("♻️ [MAS02 Worker] 테스트를 위해 컨슈머 그룹 초기화를 시작합니다...")
    for stream_key in stream_keys:
        # 1. 기존에 남아있던 그룹이 있다면 완전 삭제
        try:
            # 1. 🎯 [추가] 과거에 쌓여있던 스트림 데이터 잔여물(큐)을 완전 삭제
            await redis_client.delete(stream_key)
            logger.info(f"🔥 [MAS02 Worker] 과거 잔여 스트림 데이터 완전 청소 완료: {stream_key}")
            
            # 2. 기존에 남아있던 그룹이 있다면 완전 삭제
            await redis_client.xgroup_destroy(stream_key, GROUP_NAME)
            logger.info(f"🗑️ [MAS02 Worker] 기존 그룹 삭제 완료: {stream_key}")
        except Exception:
            pass # 삭제할 그룹이 없으면 에러가 나므로 패스
    
    for stream_key in stream_keys:
        try:
            # 컨슈머 그룹이 없으면 생성 (mkstream=False는 이미 MAS01이 스트림을 만들었기 때문)
            await redis_client.xgroup_create(stream_key, GROUP_NAME, id="0", mkstream=True)
            logger.info(f"✅ [MAS02 Init] 컨슈머 그룹 생성 완료: {stream_key}")
        except Exception as e:
            # 이미 컨슈머 그룹이 존재하면 에러가 나므로 패스
            if "BUSYGROUP" in str(e):
                continue
            logger.warning(f"[MAS02 Init Warning] {stream_key} 초기화 중 예외 (무시 가능): {e}")
    
    logger.info(f"[MAS02 worekrs.py redis_incident_consumer_and_rerouter] 서울시 25개 구 사고 스트림 동시 관제를 시작합니다... 무전 대기 중.")
    
    while True:
        try:
            # [구별 Subscribe 핵심] 
            # 25개 구의 스트림 키들을 { "key": ">" } 형태의 딕셔너리로 조립합니다.
            # streams_dict = {"incident:stream:서울특별시:강남구": ">", "incident:stream:서울특별시:강동구": ">", ...}
            streams_dict = {stream_key: ">" for stream_key in stream_keys}
            
            # xreadgroup에 25개 채널 정보가 담긴 딕셔너리를 통째로 주입!
            response = await redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams=streams_dict,
                count=1,       # 쏠림 방지를 위해 사고는 무조건 1개씩 순차 처리
                block=10000    # 사고가 전혀 없으면 10초 동안 CPU를 쉬게 하며 대기
            )
            
            if response:
                for stream_key, messages in response:
                    current_gu = stream_key.split(":")[-1]
                    
                    for message_id, payload in messages:
                        # payload = payload.get("payload")
                        # payload = json.loads(payload)
                        incident_id = payload['incident_id']
                        
                        # A. Neo4j에서 이번 사고로 묶인 물리 마스터 노드들의 좌표 배열 획득
                        affected_coords = await get_affected_coordinates_from_neo4j(incident_id)
                        
                        if affected_coords:
                            # B. Redis XY 데이터를 열어 좌표가 일치하는 유저 ID(recoId) 추출
                            affected_user_ids = await get_active_users_by_coordinates(affected_coords)
                            
                            if affected_user_ids:
                                logger.info(f"[MAS02 worekrs.py redis_incident_consumer_and_rerouter] [우회 기동] 통제 좌표 영향권 유저 리스트: {affected_user_ids}")
                                
                                # C. ⚡ 걸러진 유저 ID 목록을 기반으로 비동기 다익스트라 우회로 연산 후 Spring Boot로 발송
                                # (ID들을 던져 개별 다익스트라 처리 후 3단계 send_to_spring_boot 호출)
                                # push_tasks = [
                                #     send_reroute_to_spring_boot(
                                #         await build_reroute_json_by_id(u_id)
                                #     ) for u_id in affected_user_ids
                                # ]
                                # await asyncio.gather(*push_tasks)
                                
                        # ACK 처리 상동
                        await config.redis_client.xack(stream_key, GROUP_NAME, message_id)
                        logger.info(f"[MAS02 workers.py redis_incident_consumer_and_rerouter][ACK] 사건 {incident_id} 우회 전파 프로세스 완료 확정.")

            # CPU 과열 방지용 미세 휴식
            await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info("[MAS02 workers.py redis_incident_consumer_and_rerouter] 서버 정지로 인해 구독 리스너를 종료합니다.")
            break
        except Exception as e:
            logger.error(f"[MAS02 workers.py redis_incident_consumer_and_rerouter] 구독 루프 내 예외 발생: {e}")
            await asyncio.sleep(5)