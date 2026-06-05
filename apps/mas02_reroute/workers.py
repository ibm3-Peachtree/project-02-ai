# apps/mas02_reroute/workers.py

import json
import asyncio

import config
from config import logger
from apps.mas02_reroute.tools import get_affected_coordinates_from_neo4j, get_active_users_by_coordinates
from apps.mas02_reroute.alert import process_and_save_alerts, summarize_notice

SEOUL_GUS = [
    "강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구",
    "노원구", "도봉구", "동대문구", "동작구", "마포구", "서대문구", "서초구", "성동구",
    "성북구", "송파구", "양천구", "영등포구", "용산구", "은평구", "종로구", "중구", "중랑구"
]

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
        try:
            await redis_client.delete(stream_key)
            logger.info(f"🔥 [MAS02 Worker] 과거 잔여 스트림 데이터 완전 청소 완료: {stream_key}")
            await redis_client.xgroup_destroy(stream_key, GROUP_NAME)
            logger.info(f"🗑️ [MAS02 Worker] 기존 그룹 삭제 완료: {stream_key}")
        except Exception:
            pass
    
    for stream_key in stream_keys:
        try:
            await redis_client.xgroup_create(stream_key, GROUP_NAME, id="0", mkstream=True)
            logger.info(f"✅ [MAS02 Init] 컨슈머 그룹 생성 완료: {stream_key}")
        except Exception as e:
            if "BUSYGROUP" in str(e):
                continue
            logger.warning(f"[MAS02 Init Warning] {stream_key} 초기화 중 예외 (무시 가능): {e}")
    
    logger.info(f"[MAS02 worekrs.py redis_incident_consumer_and_rerouter] 서울시 25개 구 사고 스트림 동시 관제를 시작합니다... 무전 대기 중.")
    
    while True:
        try:
            streams_dict = {stream_key: ">" for stream_key in stream_keys}
            
            response = await redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams=streams_dict,
                count=1,       
                block=10000    
            )
            
            if response:
                for stream_key, messages in response:
                    current_gu = stream_key.split(":")[-1]
                    
                    for message_id, payload in messages:
                        incident_id = payload['incident_id']
                        
                        # A. Neo4j에서 이번 사고로 묶인 물리 마스터 노드들의 좌표 배열 획득
                        affected_coords = await get_affected_coordinates_from_neo4j(incident_id)
                        
                        if affected_coords:
                            # B. Redis XY 데이터를 열어 좌표가 일치하는 유저 ID(recoId) 추출
                            affected_user_ids, affected_user_xy = await get_active_users_by_coordinates(affected_coords, incident_id)
                            
                            if affected_user_ids:
                                logger.info(f"[MAS02 worekrs.py redis_incident_consumer_and_rerouter] [우회 기동] 통제 좌표 영향권 유저 리스트: {affected_user_ids}")
                                
                                result = await summarize_notice(payload)
                                await process_and_save_alerts(result, affected_user_ids)
                                
                                # 여기에 우회경로
                                
                                for user_id in affected_user_ids:
                                    reroute_history_key = f"user:{user_id}:reroute:history:{incident_id}"
                                    await redis_client.set(name=reroute_history_key, value="DONE", ex=3600*24)
                                    logger.info(f"[MAS02 workers][History Mark] 유저 {user_id} 우회 플래그 캐싱 완료.")
                                
                        await config.redis_client.xack(stream_key, GROUP_NAME, message_id)
                        logger.info(f"[MAS02 workers.py redis_incident_consumer_and_rerouter][ACK] 사건 {incident_id} 우회 전파 프로세스 완료 확정.")
            else :
                # Redis Stream 키들 안에 여전히 지워지지 않고 살아있는 과거 사고 메시지들을 전수 조사
                for stream_key in stream_keys:
                    active_messages = await redis_client.xrange(stream_key, min="-", max="+")
                    
                    if not active_messages:
                        continue
                        
                    for message_id, payload in active_messages:
                        incident_id = payload.get("incident_id")
                        
                        if incident_id:
                            affected_coords = await get_affected_coordinates_from_neo4j(incident_id)
                            if affected_coords:
                                affected_user_ids, affected_user_xy = await get_active_users_by_coordinates(affected_coords, incident_id)
                                
                                if affected_user_ids:
                                    logger.warning(f"[MAS02 workers.py redis_incident_consumer_and_rerouter][순찰 저격 성공] 통제 도중 중간 난입한 유저 포착: {affected_user_ids}")
                                    
                                    meta_key = f"incident:meta:{incident_id}"
                                    cached_meta = await redis_client.get(meta_key)
                                    
                                    result = None
                                    if cached_meta:
                                        try:
                                            if isinstance(cached_meta, bytes):
                                                cached_meta = cached_meta.decode('utf-8')
                                            
                                            meta_dict = json.loads(cached_meta)
                                            
                                            result = meta_dict.get("incident")
                                            logger.info(f"⚡ [MAS02 캐시 적중] LLM 요약본을 Redis 캐시에서 초고속으로 복사해왔습니다. (Key: {meta_key})")
                                        except Exception as e:
                                            logger.error(f"⚠️ 캐시 파싱 에러: {e}")
                                            result = None

                                    if not result:
                                        logger.warning(f"🔍 [캐시 미스] 메타 정보가 없어 LLM 요약을 백업 호출합니다.")
                                        result = await summarize_notice(payload)
                                    
                                    # 공통 알림 발송 기동!
                                    await process_and_save_alerts(result, affected_user_ids)
                                 
                                    for user_id in affected_user_ids:
                                        reroute_history_key = f"user:{user_id}:reroute:history:{incident_id}"
                                        await redis_client.set(name=reroute_history_key, value="DONE", ex=3600*24)
                                        logger.info(f"[MAS02 workers][History Mark] 유저 {user_id} 우회 플래그 캐싱 완료.")
                                
            await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info("[MAS02 workers.py redis_incident_consumer_and_rerouter] 서버 정지로 인해 구독 리스너를 종료합니다.")
            break
        except Exception as e:
            logger.error(f"[MAS02 workers.py redis_incident_consumer_and_rerouter] 구독 루프 내 예외 발생: {e}")
            await asyncio.sleep(5)