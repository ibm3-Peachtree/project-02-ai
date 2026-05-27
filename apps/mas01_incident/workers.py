# apps/mas01_incident/workers.py

import asyncio
import json
import logging
from datetime import datetime

import config
from config import logger
from apps.mas01_incident.tools import check_duplicate

SEOUL_GUS = [
    "강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구",
    "노원구", "도봉구", "동대문구", "동작구", "마포구", "서대문구", "서초구", "성동구",
    "성북구", "송파구", "양천구", "영등포구", "용산구", "은평구", "종로구", "중구", "중랑구"
]

async def redis_topis_listener() :
    """
    [Worker 1] 실시간 좌표 기반 Redis 스트림을 감시하는 백그라운드 태스크
    """
    GROUP_NAME = "mas01_consumer_group"
    CONSUMER_NAME = "mas01_worker_stream"
    
    rag_agent = None
    router_agent = None
    
    redis_client = config.redis_client
    while redis_client is None:
        logger.info("[MAS01 Worker 1] Redis 클라이언트가 준비되기를 기다리는 중...")
        await asyncio.sleep(1)
    
    # logger.info("[MAS01 Worker 1] 구별/날짜별 실시간 스트림 감시 루프 가동 시작...")
    
    while True :
        try :
            # 1. 현재 날짜를 기반으로 오늘 생성되어야 할 구별 스트림 키 목록 정의
            # 형식 예시: incident:서울특별시:강남구:20260526:stream
            # today_str = datetime.now().strftime("%Y%m%d")
            today_str = "20260520"
            stream_keys = [f"incident:서울특별시:{gu}:{today_str}:stream" for gu in SEOUL_GUS]
            
            # 2. 모든 구의 오늘자 스트림에 대해 컨슈머 그룹이 없다면 생성 (mkstream=True로 스트림 자동 생성 방지 보완)
            for stream_key in stream_keys:
                try:
                    await redis_client.xgroup_create(stream_key, GROUP_NAME, id="0", mkstream=True)
                except Exception:
                    # 이미 그룹이 존재할 때 발생하는 예외는 정상적이므로 패스합니다.
                    pass
                
            # 3. 25개 구의 모든 스트림을 동시에 감시하기 위한 딕셔너리 빌드
            # '>' 의미: 이 컨슈머 그룹 기준, 아직 아무도 읽어가지 않은 새 데이터만 가져오겠다
            streams_dict = {stream_key: ">" for stream_key in stream_keys}
            
            # 4. 여러 구의 스트림을 동시에 Blocking으로 한 번에 읽기 (최대 2초 블로킹 대기)
            # 25개 구 중 어느 한 곳이라도 데이터가 들어오면 즉시 반응합니다.
            response = await redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams=streams_dict,
                count=1,
                block=2000
            )
            
            if response:
                for stream_key, messages in response:
                    # decode_responses=True 덕분에 stream_key는 이미 'str' 객체입니다! (.decode() 제거)
                    
                    for message_id, payload in messages:
                        # 키도 바이너리(b'data')가 아닌 일반 문자열('data')로 접근합니다.
                        # 값 역시 이미 문자열로 디코딩되어 있으므로 .decode('utf-8')을 지웁니다.
                        incident_data = payload
                        
                        if not incident_data.get("info"):
                            logger.warning(f"[Stream Worker] {stream_key}에 유효한 돌발 정보가 없습니다.")
                            continue
                            
                        logger.info(f"[Stream Worker] '{stream_key}'에서 새 돌발 상황 포착!")
                        logger.info(f"사고 내용 요약: {incident_data.get('info')[:20]}...")
                        
                        # 1단계: 중복 검증
                        is_new = await check_duplicate(incident_data)
                        
                        if is_new:
                            # 2단계: GraphRAG를 이용하여 실시간 좌표 주변의 피해 반경 [역/노선] 분석 (Agent 2)
                            # 좌표 기반 데이터이므로 분석 에이전트 내에서 Neo4j Spatial Query를 활용하게 됩니다.
                            impact_data = await rag_agent.analyze_coordinate_incident(incident_data)
                            
                            impact_elements = {
                                "stations": impact_data["affected_stations"], 
                                "routes": impact_data["affected_routes"]
                            }
                            
                            # 3단계: Redis User Path 스캔, LLM 문맥 사유, 우회 연산 및 Spring 전파 전체 실행 (Agent 3 + 4 + 5)
                            await router_agent.process_user_rerouting(impact_data, impact_elements)
                        
                        else:
                            print(f"[Stream Worker] 중복된 돌발 상황건으로 판명되어 스킵합니다.")

                        # 처리가 성공적으로 끝나면 해당 구 스트림에 ACK 확정 신호를 보냅니다.
                        await redis_client.xack(stream_key, GROUP_NAME, message_id)

            # 무한 루프로 인한 CPU 과부하 방지용 미세 휴식
            await asyncio.sleep(5)

        except asyncio.CancelledError:
            print("[MAS01 Worker 1] 서버 정지로 인해 스트림 리스너를 종료합니다.")
            break
        except Exception as e:
            print(f"[MAS01 Worker 1 Error] 에러 발생: {e}")
            await asyncio.sleep(5)  # 에러 발생 시 일시적인 부하 분산을 위해 대기 후 리트라이
    
async def mysql_topis_listener() :
    """
    [Worker 2] MySQL의 비정형 공지사항 테이블을 주기적으로 감시하는 백그라운드 태스크
    """
    logger.info("MYSQL Listener")
    