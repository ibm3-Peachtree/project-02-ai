# apps/mas02_reroute/workers.py

import json
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import config
from apps.mas02_reroute.tools import get_affected_coordinates_from_neo4j, get_active_users_by_coordinates
from apps.mas02_reroute.alert import process_and_save_alerts, summarize_notice
from apps.mas02_reroute.agents import mas02_agent

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
    
    config.logger.info("[MAS02 Worker] 테스트를 위해 컨슈머 그룹 초기화를 시작합니다...")
    for stream_key in stream_keys:
            try:
                # ❌ 원본 데이터를 파괴하던 delete 구문만 주석 처리하여 사살합니다.
                # await redis_client.delete(stream_key)
                
                # 컨슈머 그룹만 폭파했다가 아래에서 id="0"으로 재생성하므로 
                # 큐에 쌓여있던 기존 데이터를 처음부터 다시 읽어오게 됩니다.
                await redis_client.xgroup_destroy(stream_key, GROUP_NAME)
                config.logger.info(f"[MAS02 Worker] 기존 그룹 삭제 완료: {stream_key}")
            except Exception:
                pass
    
    for stream_key in stream_keys:
        try:
            await redis_client.xgroup_create(stream_key, GROUP_NAME, id="0", mkstream=True)
            config.logger.info(f"[MAS02 Init] 컨슈머 그룹 생성 완료: {stream_key}")
        except Exception as e:
            if "BUSYGROUP" in str(e):
                continue
            config.logger.warning(f"[MAS02 Init Warning] {stream_key} 초기화 중 예외 (무시 가능): {e}")
    
    config.logger.info("[MAS02 worekrs.py redis_incident_consumer_and_rerouter] 서울시 25개 구 사고 스트림 동시 관제를 시작합니다... 무전 대기 중.")
    
    asyncio.create_task(patrol_active_incidents_loop(stream_keys, redis_client, GROUP_NAME))
    
    while True:
        try:
            streams_dict = {stream_key: ">" for stream_key in stream_keys}
            
            # 새로운 메시지 유입(XREADGROUP) 이벤트 리스닝에만 집중합니다.
            response = await redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams=streams_dict,
                count=1,       
                block=10000    
            )
            
            if response:
                for stream_key, messages in response:
                    for message_id, payload in messages:
                        incident_id = payload['incident_id']
                        
                        # A. Neo4j에서 이번 사고로 묶인 물리 마스터 노드들의 좌표 배열 획득
                        affected_coords = await get_affected_coordinates_from_neo4j(incident_id)
                        
                        if affected_coords:
                            # B. Redis XY 데이터를 열어 좌표가 일치하는 유저 ID(recoId) 추출
                            affected_user_ids, affected_user_xy = await get_active_users_by_coordinates(affected_coords, incident_id)
                            
                            if affected_user_ids:
                                config.logger.info(f"[MAS02 worekrs.py redis_incident_consumer_and_rerouter] [우회 기동] 통제 좌표 영향권 유저 리스트: {affected_user_ids}")
                                
                                result = await summarize_notice(payload)
                                await process_and_save_alerts(result, affected_user_ids)
                                
                                for idx, user_id in enumerate(affected_user_ids):
                                    try :
                                        initial_state = {
                                            "incident_id": incident_id,
                                            "user_id" : user_id,
                                            "user_live_route_xy" : affected_user_xy[idx]
                                        }
                                        final_state = await mas02_agent.ainvoke(initial_state)
                                        
                                        processed_nodes = final_state.get("final_rerouting_paths", [])
                                        key = f"routine:live:incident:full:{user_id}"
                                        
                                        if processed_nodes:
                                            try:    
                                                # Redis 인메모리 Key-Value 스냅샷 SET 기동
                                                await redis_client.set(name=key, value=processed_nodes)
                                                
                                                # TTL 계산 (endTime - 현재 시간)
                                                end_date_str = payload.get("endDateTime")
                                                end_time = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("Asia/Seoul"))
                                                now_time = datetime.now(ZoneInfo("Asia/Seoul"))
                                                remaining_time = end_time - now_time
                                                remaining_seconds = int(remaining_time.total_seconds())
                                                if remaining_seconds <= 0:
                                                    remaining_seconds = 1
                                                    
                                                await redis_client.expire(key, remaining_seconds)
                                                config.logger.info(f"[MAS02 workers.py Redis Sync] 유저 {user_id}의 실시간 복합 사고 노드 {len(processed_nodes)}건 적재 완료.")
                                                
                                            except Exception as redis_err:
                                                config.logger.error(f"[MAS02 workers.py Redis Error] 유저 {user_id} 사고 데이터 캐싱 중 실패: {redis_err}")
                                        else:
                                            config.logger.warning(f"[MAS02 workers.py] 유저 {user_id}에게 매핑된 최종 사고 노드가 없어 Redis 저장을 생략합니다.")
                                        
                                    except Exception as e :
                                        config.logger.error(f"[MAS02 workers.py redis_incident_consumer_and_rerouter] {e}")
                                        continue
                                    
                                    reroute_history_key = f"user:{user_id}:reroute:history:{incident_id}"
                                    await redis_client.set(name=reroute_history_key, value="DONE", ex=3600*24)
                                    config.logger.info(f"[MAS02 workers][History Mark] 유저 {user_id} 우회 플래그 캐싱 완료.")
                                
                        await config.redis_client.xack(stream_key, GROUP_NAME, message_id)
                        config.logger.info(f"[MAS02 workers.py redis_incident_consumer_and_rerouter][ACK] 사건 {incident_id} 우회 전파 프로세스 완료 확정.")
            
            # 대기열이 비어있을 때는 불필요한 연산을 하지 않고 0.1초 대기 후 다음 이벤트를 수신합니다.
            await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            config.logger.info("[MAS02 workers.py redis_incident_consumer_and_rerouter] 서버 정지로 인해 구독 리스너를 종료합니다.")
            break
        except Exception as e:
            config.logger.error(f"[MAS02 workers.py redis_incident_consumer_and_rerouter] 구독 루프 내 예외 발생: {e}")
            await asyncio.sleep(5)


async def patrol_active_incidents_loop(stream_keys, redis_client, group_name):
    """
    [MAS02 순찰 스케줄러 태스크]
    지워지지 않고 스트림에 남은 과거 통제 건에 대해 중간에 진입한 유저를 15초 주기로 추적합니다.
    """
    config.logger.info("[MAS02 순찰대] 중간 난입 유저 포착 스케줄러가 백그라운드에서 기동되었습니다.")
    
    while True:
        try:
            # 과도한 무한 루프 조회를 방지하기 위해 15초 간격으로 스캔 주기를 제한합니다.
            await asyncio.sleep(15)
            
            for stream_key in stream_keys:
                active_messages = await redis_client.xrange(stream_key, min="-", max="+")
                
                if not active_messages:
                    continue
                    
                for message_id, payload in active_messages:
                    incident_id = payload.get("incident_id")
                    
                    if incident_id:
                        affected_coords = await get_affected_coordinates_from_neo4j(incident_id)
                        if affected_coords and isinstance(affected_coords, dict):
                            affected_coords = [affected_coords]
                        if affected_coords:
                            affected_user_ids, affected_user_xy = await get_active_users_by_coordinates(affected_coords, incident_id)
                            
                            if affected_user_ids:
                                config.logger.warning(f"[MAS02 workers.py redis_incident_consumer_and_rerouter][순찰 저격 성공] 통제 도중 중간 난입한 유저 포착: {affected_user_ids}")
                                
                                meta_key = f"incident:meta:{incident_id}"
                                cached_meta = await redis_client.get(meta_key)
                                
                                result = None
                                if cached_meta:
                                    try:
                                        if isinstance(cached_meta, bytes):
                                            cached_meta = cached_meta.decode('utf-8')
                                        
                                        meta_dict = json.loads(cached_meta)
                                        result = meta_dict.get("incident")
                                        config.logger.info(f"[MAS02 캐시 적중] LLM 요약본을 Redis 캐시에서 초고속으로 복사해왔습니다. (Key: {meta_key})")
                                    except Exception as e:
                                        config.logger.error(f"캐시 파싱 에러: {e}")
                                        result = None

                                if not result:
                                    config.logger.warning(f"[캐시 미스] 메타 정보가 없어 LLM 요약을 백업 호출합니다.")
                                    result = await summarize_notice(payload)
                                
                                # 공통 알림 발송 기동
                                await process_and_save_alerts(result, affected_user_ids)
                             
                                for idx, user_id in enumerate(affected_user_ids):
                                    try :
                                        initial_state = {
                                            "incident_id": incident_id,
                                            "user_id" : user_id,
                                            "user_live_route_xy" : affected_user_xy[idx]
                                        }
                                        final_state = await mas02_agent.ainvoke(initial_state)
                                        
                                        processed_nodes = final_state.get("final_rerouting_paths", [])
                                        key = f"routine:live:incident:full:{user_id}"
                                        
                                        if processed_nodes:
                                            try:    
                                                # Redis 인메모리 Key-Value 스냅샷 SET 기동
                                                await redis_client.set(name=key, value=processed_nodes)
                                                
                                                # TTL 계산 (endTime - 현재 시간)
                                                end_date_str = payload.get("endDateTime")
                                                end_time = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("Asia/Seoul"))
                                                now_time = datetime.now(ZoneInfo("Asia/Seoul"))
                                                remaining_time = end_time - now_time
                                                remaining_seconds = int(remaining_time.total_seconds())
                                                if remaining_seconds <= 0:
                                                    remaining_seconds = 1
                                                    
                                                await redis_client.expire(key, remaining_seconds)
                                                config.logger.info(f"[MAS02 workers.py Redis Sync] 유저 {user_id}의 실시간 복합 사고 노드 {len(processed_nodes)}건 적재 완료.")
                                                
                                            except Exception as redis_err:
                                                config.logger.error(f"[MAS02 workers.py Redis Error] 유저 {user_id} 사고 데이터 캐싱 중 실패: {redis_err}")
                                        else:
                                            config.logger.warning(f"[MAS02 workers.py] 유저 {user_id}에게 매핑된 최종 사고 노드가 없어 Redis 저장을 생략합니다.")
                                        
                                    except Exception as e :
                                        config.logger.error(f"[MAS02 workers.py redis_incident_consumer_and_rerouter] {e}")
                                        continue
                                
                                    reroute_history_key = f"user:{user_id}:reroute:history:{incident_id}"
                                    await redis_client.set(name=reroute_history_key, value="DONE", ex=3600*24)
                                    config.logger.info(f"[MAS02 workers][History Mark] 유저 {user_id} 우회 플래그 캐싱 완료.")
                                    
        except asyncio.CancelledError:
            config.logger.info("[MAS02 순찰대] 백그라운드 태스크가 종료됩니다.")
            break
        except Exception as e:
            config.logger.error(f"[MAS02 순찰 루프 예외] 에러 발생 후 대기 처리합니다: {e}")
            await asyncio.sleep(5)