# apps/mas01_incident/workers.py

import asyncio
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import aiomysql

import config
from config import logger
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
    
    # 컨슈머 그룹 초기화 및 최초 생성은 루프 '외부'에서 단 한 번만 실행
    logger.info("♻️ [MAS01 Worker 1] [최초 1회 인프라 셋업] 컨슈머 그룹 초기화를 시작합니다...")
    
    kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
    today_str = kst_now.strftime("%Y%m%d")
    
    # 현재 테스트 타겟 스트림 (서빙 전환 시 SEOUL_GUS 확장 가능)
    # stream_keys = [f"incident:서울특별시:중구:{today_str}:stream"]
    stream_keys = [f"incident:서울특별시:{gu}:{today_str}.stream" for gu in SEOUL_GUS]
    
    for stream_key in stream_keys:
        try:
            await redis_client.xgroup_destroy(stream_key, GROUP_NAME)
            logger.info(f"🗑️ 기존 그룹 삭제 완료: {stream_key}")
        except Exception:
            pass
            
    for stream_key in stream_keys:
        try:
            # mkstream=True로 스트림 뼈대와 컨슈머 그룹을 공고히 다집니다.
            await redis_client.xgroup_create(stream_key, GROUP_NAME, id="0", mkstream=True)
            logger.info(f"✅ 컨슈머 그룹 생성 완료: {stream_key}")
        except Exception:
            pass
    
    while True:
        try:
            streams_dict = {stream_key: ">" for stream_key in stream_keys}
            
            # 한 번에 여러 개가 들어와도 유실 없이 처리하기 위해 count를 5~10 정도로 넉넉히 주는 것을 추천합니다.
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
                            logger.warning(f"[Stream Worker] {stream_key}에 유효한 돌발 정보가 없습니다.")
                            continue
                        
                        # 중복 검증
                        is_new = await check_duplicate(incident_data, "redis")
                        
                        if is_new:
                            logger.info("[Stream Worker : Redis] 에이전트 분석 시작")
                            
                            initial_state = {"raw_incident_data": incident_data}
                            final_state = await mas01_agent.ainvoke(initial_state)
                            
                            processed_nodes = final_state.get("affected_nodes", [])
                            logger.info("[Stream Worker : Redis] : Node 처리 완료")
                            
                            for node in processed_nodes: 
                                logger.info(f"   📍 장소: {node['affected']} | 좌표: ({node['lat']}, {node['lng']}) | 기간: {node['startDateTime']} ~ {node['endDateTime']}")
                            
                            await redis_client.xack(stream_key, GROUP_NAME, message_id)
                        
                        else:
                            logger.info(f"[Stream Worker] 중복된 돌발 상황건으로 판명되어 스킵 및 ACK 처리합니다.")
                            await redis_client.xack(stream_key, GROUP_NAME, message_id)

            await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info("[MAS01 Worker 1] 서버 정지로 인해 스트림 리스너를 종료합니다.")
            break
        except Exception as e:
            logger.error(f"[MAS01 Worker 1 Error] 에러 발생: {e}")
            await asyncio.sleep(5)
    
async def mysql_topis_listener() :
    """
    [Worker 2] MySQL의 비정형 공지사항 테이블을 주기적으로 감시하는 백그라운드 태스크
    """
    
    mysql_pool = config.mysql_pool
    
    while True :
        try :
            kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
            current_time = kst_now.strftime("%Y-%m-%d %H:%M:%S")
            date_format = "%Y-%m-%d %H:%M:%S"
            
            sql = """
                SELECT * FROM topis_notice 
                WHERE end_datetime >= %s
            """
            
            async with mysql_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:  # 딕셔너리 형태로 결과 반환받기
                    await cur.execute(sql, (current_time,))
                    results = await cur.fetchall()
                    
            for result in results :
                is_new = await check_duplicate(result, "mysql")
                
                if is_new:
                    logger.info("[Stream Worker : MySQL] 에이전트 분석 시작")
                    
                    initial_state = {"raw_incident_data": result}
                    final_state = await mas01_agent.ainvoke(initial_state)
                    
                    processed_nodes = final_state.get("affected_nodes", [])
                    logger.info("[Stream Worker : MySQL] : Node 처리 완료")
                    
                    for node in processed_nodes : 
                        logger.info(f"장소: {node['affected']} | 좌표: ({node['lat']}, {node['lng']}) | 기간: {node['startDateTime']} ~ {node['endDateTime']}")
                        # 다음 행동 등 하기

                await asyncio.sleep(60)
        
        except asyncio.CancelledError:
            logger.info("[MAS01 Worker 2] 서버 정지로 인해 스트림 리스너를 종료합니다.")
            break
        except Exception as e:
            logger.error(f"[MAS01 Worker 2 Error] 에러 발생: {e}")
            await asyncio.sleep(5)  # 에러 발생 시 일시적인 부하 분산을 위해 대기 후 리트라이
            
async def redis_stream_end_time_cleaner():
    """
    [Background DB Cleaner]
    5분마다 돌면서 endDateTime이 지난 만료된 대중교통 통제 데이터를 추적
    만료 데이터 발견 시:
      1. Neo4j 그래프 DB에서 해당 Incident 노드 및 AFFECTED_BY 간선 일괄 삭제 (DETACH DELETE)
      2. Redis Stream에서 해당 메시지 영구 삭제 (XDEL)
    """
    redis_client = config.redis_client
    
    # 특정 ID DETACH DELETE를 수행하면 물려있던 가상 플랫폼간의 :AFFECTED_BY 관계 자동 삭제
    neo4j_purge_cypher = """
        MATCH (i:Incident {id: $incident_id})
        DETACH DELETE i
    """
    
    while True:
        try:
            # 5분마다 관제 청소기 가동
            await asyncio.sleep(300)
            
            # 현재 시간 타임스탬프 밀리초 구하기
            kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
            current_ts_ms = int(kst_now.timestamp() * 1000)
            
            for gu in SEOUL_GUS:
                stream_key = f"incident:stream:서울특별시:{gu}"
                
                # 현재 시점 이전에 쌓인 과거 스트림 메시지만 효율적으로 범위 검색 (XRANGE)
                expired_messages = await redis_client.xrange(stream_key, min="-", max=str(current_ts_ms))
                
                if not expired_messages:
                    continue
                    
                for message_id, payload in expired_messages:
                    raw_json = payload.get(b"payload").decode('utf-8')
                    data = json.loads(raw_json)
                    
                    # 내부 endDateTime 문자열 정밀 파싱
                    end_str = data.get("endDateTime", "").replace("T", " ")
                    end_dt = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
                    
                    # [만료 판정선 규칙] 현재 시간보다 종료 예정 시간이 과거인 경우
                    if end_dt < datetime.now(ZoneInfo("Asia/Seoul")):
                        target_incident_id = data.get("incident_id")
                        affected_place = data.get("affected", "알 수 없는 장소")
                        
                        # Neo4j 그래프 데이터베이스 동기화 청소
                        if target_incident_id:
                            try:
                                async with config.neo4j_client.session() as session:
                                    result = await session.run(neo4j_purge_cypher, incident_id=target_incident_id)
                                    summary = await result.consume()
                                    
                                    logger.info(
                                        f"[MAS01 Worker3 Neo4j Auto-Purge] 시간 만료로 인한 그래프 청소 완료 "
                                        f"(장소: {affected_place} | 삭제된 노드: {summary.counters.nodes_deleted}개)"
                                    )
                            except Exception as ne:
                                logger.error(f"[MAS01 Worker3 Neo4j Auto-Purge Error] '{affected_place}' 노드 제거 실패: {ne}")
                                # Neo4j 삭제 실패 시 데이터 무결성을 위해 Redis 삭제를 건너뛰고 다음 턴에 재시도
                                continue 
                        
                        # Redis Stream 큐 메모리 관리 청소
                        await redis_client.xdel(stream_key, message_id)
                        logger.info(f"[MAS01 Worker3 Redis Stream Cleaner] 스트림 메시지 XDEL 완료 (MsgID: {message_id})")
                        
        except asyncio.CancelledError:
            logger.info("[MAS01 Worker3 Background DB Cleaner] 서버 종료로 인해 청소 태스크를 종료합니다.")
            break
        except Exception as e:
            logger.error(f"[MAS01 Worker3 Background DB Cleaner Total Error] 스케줄러 실행 중 예외 발생: {e}")
            await asyncio.sleep(10) # 에러 폭사 방지용 휴식            
    