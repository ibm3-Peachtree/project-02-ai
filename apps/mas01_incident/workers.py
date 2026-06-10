# apps/mas01_incident/workers.py

import asyncio
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import aiomysql

import config
from apps.mas01_incident.tools import check_duplicate
from apps.mas01_incident.agents import mas01_agent

SEOUL_GUS = [
    "강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구",
    "노원구", "도봉구", "동대문구", "동작구", "마포구", "서대문구", "서초구", "성동구",
    "성북구", "송파구", "양천구", "영등포구", "용산구", "은평구", "종로구", "중구", "중랑구"
]

async def redis_topis_listener():
    """
    [Worker 1] 실시간 좌표 기반 Redis 스트림을 감시하는 백그라운드 태스크
    """
    GROUP_NAME = "mas01_consumer_group"
    CONSUMER_NAME = "mas01_worker_stream"
    
    redis_client = config.redis_client
    
    config.logger.info("[MAS01 Worker 1] [최초 1회 인프라 셋업] 컨슈머 그룹 초기화를 시작합니다...")
    
    kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
    today_str = kst_now.strftime("%Y%m%d")
    
    stream_keys = [f"incident:서울특별시:{gu}:{today_str}.stream" for gu in SEOUL_GUS]
    
    for stream_key in stream_keys:
        try:
            await redis_client.xgroup_destroy(stream_key, GROUP_NAME)
            config.logger.info(f"기존 그룹 삭제 완료: {stream_key}")
        except Exception:
            pass
            
    for stream_key in stream_keys:
        try:
            await redis_client.xgroup_create(stream_key, GROUP_NAME, id="0", mkstream=True)
            config.logger.info(f"컨슈머 그룹 생성 완료: {stream_key}")
        except Exception:
            pass
    
    while True:
        try:
            streams_dict = {stream_key: ">" for stream_key in stream_keys}
            
            response = await redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams=streams_dict,
                count=5,
                block=10000
            )
            
            if response:
                for stream_key, messages in response:
                    for message_id, payload in messages:
                        incident_data = payload
                        
                        if not incident_data.get("info"):
                            config.logger.warning(f"[Stream Worker] {stream_key}에 유효한 돌발 정보가 없습니다.")
                            continue
                        
                        is_new = await check_duplicate(incident_data, "redis")
                        
                        if is_new:
                            config.logger.info("[Stream Worker : Redis] 에이전트 분석 시작")
                            
                            initial_state = {"raw_incident_data": incident_data}
                            final_state = await mas01_agent.ainvoke(initial_state)
                            
                            processed_nodes = final_state.get("affected_nodes", [])
                            config.logger.info("[Stream Worker : Redis] : Node 처리 완료")
                            
                            for node in processed_nodes: 
                                config.logger.info(f"   장소: {node['affected']} | 좌표: ({node['lat']}, {node['lng']}) | 기간: {node['startDateTime']} ~ {node['endDateTime']}")
                            
                            await redis_client.xack(stream_key, GROUP_NAME, message_id)
                        
                        else:
                            config.logger.info(f"[Stream Worker] 중복된 돌발 상황건으로 판명되어 스킵 및 ACK 처리합니다.")
                            await redis_client.xack(stream_key, GROUP_NAME, message_id)

            await asyncio.sleep(1)

        except asyncio.CancelledError:
            config.logger.info("[MAS01 Worker 1] 서버 정지로 인해 스트림 리스너를 종료합니다.")
            break
        except Exception as e:
            config.logger.error(f"[MAS01 Worker 1 Error] 에러 발생: {e}")
            await asyncio.sleep(5)
    
async def mysql_topis_listener():
    """
    [Worker 2] MySQL의 비정형 공지사항 테이블을 주기적으로 감시하는 백그라운드 태스크
    """
    mysql_pool = config.mysql_pool
    
    while True:
        try:
            # 1. 현재 시스템 KST 시각 확보 (%Y-%m-%d %H:%M:%S 형식)
            kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
            current_time = kst_now.strftime("%Y-%m-%d %H:%M:%S")
            
            # 2. 시작 시점은 현재 이하(<=)이고, 종료 시점은 현재 초과(>)인 실시간 진행형 쿼리 정의
            sql = """
                SELECT * FROM topis_notice 
                WHERE start_datetime <= %s 
                  AND end_datetime > %s
            """
            
            async with mysql_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    # 쿼리의 %s 자리에 동일한 현재 시각 스트링을 각각 매핑합니다.
                    await cur.execute(sql, (current_time, current_time))
                    results = await cur.fetchall()
                    
            # 3. 획득한 현재 진행형 공지사항 리스트 분석 처리
            for result in results:
                is_new = await check_duplicate(result, "mysql")
                
                if is_new:
                    config.logger.info("[Stream Worker : MySQL] 에이전트 분석 시작")
                    
                    initial_state = {"raw_incident_data": result}
                    final_state = await mas01_agent.ainvoke(initial_state)
                    
                    processed_nodes = final_state.get("affected_nodes", [])
                    config.logger.info("[Stream Worker : MySQL] : Node 처리 완료")
                    
                    for node in processed_nodes: 
                        config.logger.info(f"장소: {node['affected']} | 좌표: ({node['lat']}, {node['lng']}) | 기간: {node['startDateTime']} ~ {node['endDateTime']}")
            
            # 다음 수집 턴까지 1분 대기
            await asyncio.sleep(60)
        
        except asyncio.CancelledError:
            config.logger.info("[MAS01 Worker 2] 서버 정지로 인해 스트림 리스너를 종료합니다.")
            break
        except Exception as e:
            config.logger.error(f"[MAS01 Worker 2 Error] 에러 발생: {e}")
            await asyncio.sleep(5)
            
async def redis_stream_end_time_cleaner():
    """
    [Background DB Cleaner]
    5분마다 돌면서 endDateTime이 지난 만료된 대중교통 통제 데이터를 추적
    """
    redis_client = config.redis_client
    
    neo4j_purge_cypher = """
        MATCH (i:Incident {id: $incident_id})
        DETACH DELETE i
    """
    
    while True:
        try:
            await asyncio.sleep(300)
            
            kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
            current_ts_ms = int(kst_now.timestamp() * 1000)
            
            for gu in SEOUL_GUS:
                stream_key = f"incident:stream:서울특별시:{gu}"
                expired_messages = await redis_client.xrange(stream_key, min="-", max=str(current_ts_ms))
                
                if not expired_messages:
                    continue
                    
                for message_id, payload in expired_messages:
                    raw_json = payload.get(b"payload").decode('utf-8')
                    data = json.loads(raw_json)
                    
                    end_str = data.get("endDateTime", "").replace("T", " ")
                    end_dt = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
                    
                    if end_dt < datetime.now(ZoneInfo("Asia/Seoul")):
                        target_incident_id = data.get("incident_id")
                        affected_place = data.get("affected", "알 수 없는 장소")
                        
                        if target_incident_id:
                            try:
                                async with config.neo4j_client.session() as session:
                                    result = await session.run(neo4j_purge_cypher, incident_id=target_incident_id)
                                    summary = await result.consume()
                                    
                                    config.logger.info(
                                        f"[MAS01 Worker3 Neo4j Auto-Purge] 시간 만료로 인한 그래프 청소 완료 "
                                        f"(장소: {affected_place} | 삭제된 노드: {summary.counters.nodes_deleted}개)"
                                    )
                            except Exception as ne:
                                config.logger.error(f"[MAS01 Worker3 Neo4j Auto-Purge Error] '{affected_place}' 노드 제거 실패: {ne}")
                                continue 
                        
                        await redis_client.xdel(stream_key, message_id)
                        config.logger.info(f"[MAS01 Worker3 Redis Stream Cleaner] 스트림 메시지 XDEL 완료 (MsgID: {message_id})")
                        
        except asyncio.CancelledError:
            config.logger.info("[MAS01 Worker3 Background DB Cleaner] 서버 종료로 인해 청소 태스크를 종료합니다.")
            break
        except Exception as e:
            config.logger.error(f"[MAS01 Worker3 Background DB Cleaner Total Error] 스케줄러 실행 중 예외 발생: {e}")
            await asyncio.sleep(10)