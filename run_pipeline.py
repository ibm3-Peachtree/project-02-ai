# run_pipeline.py
import asyncio
import signal
import sys
import config

from apps.mas01_incident.workers import redis_topis_listener, mysql_topis_listener, redis_stream_end_time_cleaner
from apps.mas02_reroute.workers import redis_incident_consumer_and_rerouter
from config import init_db_connections, close_db_connections, init_gdf

async def shutdown(loop, signal=None):
    if signal:
        config.logger.info(f"=== [System] 종료 시그널 수신: {signal.name} ===")
    config.logger.info("=== [System] DB 연결 해제 및 워커 종료 프로세스 가동 ===")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await close_db_connections()
    config.logger.info("=== [System] 모든 인프라 셧다운 완료. 안전하게 종료합니다 ===")
    loop.stop()

async def main():
    config.logger.info("=== [System] 독립형 데이터 파이프라인 데몬 기동 ===")
    
    # 1. 인프라 터널과 커넥션 풀 개통 (이제 1초 만에 통과합니다)
    await init_db_connections()
    
    # 2. 270만 건 GIS 데이터 로드 완료
    await init_gdf() 
    
    # 3. 리눅스 시그널 핸들러 등록
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(loop, s)))

    config.logger.info("=== [System] 뼈대 인프라 셋업 완료. Neo4j 대용량 가상 그래프 빌드 개시 ===")

    # 4. 모든 베이스가 안전하게 확보된 시점에서 비즈니스 알고리즘 앱 로드
    from apps.mas02_reroute.rerouting import TransportApp
    transport_app = TransportApp()
    
    config.logger.info("🏗️ [GDS 프로젝트] 155만 건 교통망 메모리 프로젝션 연산 중... 잠시 대기")
    await transport_app.delete_gds_graph()
    await transport_app.build_gds_graph1()
    await transport_app.build_gds_graph2()
    await transport_app.build_gds_graph3()
    config.logger.info("✅ [GDS 프로젝트] 가상 그래프 3개소 초고속 메모리 빌드 완료!")

    config.logger.info("=== [System] 4대 메인 워커 관제를 시작합니다. 무전 대기 ===")

    try:
        await asyncio.gather(
            redis_topis_listener(),
            mysql_topis_listener(),
            redis_incident_consumer_and_rerouter(),
            redis_stream_end_time_cleaner()
        )
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        config.logger.info("=== [System] 프로세스가 키보드 입력에 의해 강제 종료되었습니다 ===")
        sys.exit(0)