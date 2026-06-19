# run_pipeline.py (최종 종착지 보정본)

import asyncio
import signal
import sys

import config
from config import init_db_connections, close_db_connections, init_gdf
from apps.mas02_reroute.rerouting import TransportApp

async def shutdown(loop, signal=None):
    if signal:
        config.logger.info(f"=== [System] 종료 시그널 수신: {signal.name} ===")
    config.logger.info("=== [System] DB 연결 해제 및 워커 종료 프로세스 가동 ===")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await config.close_db_connections()
    config.logger.info("=== [System] 모든 인프라 셧다운 완료. 안전하게 종료합니다 ===")
    loop.stop()

async def main():
    config.logger.info("=== [System] 독립형 데이터 파이프라인 데몬 기동 ===")
    
    # 1. 인프라 터널과 커넥션 풀 완전 개통 (이 시점에 글로벌 redis_client 주소가 완벽히 주입됨!)
    await init_db_connections()
    
    # 2. 270만 건 GIS 데이터 로드 완료
    await init_gdf() 
    
    # 3. 리눅스 시그널 핸들러 등록
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(loop, s)))

    config.logger.info("=== [System] 뼈대 인프라 셋업 완료. Neo4j 대용량 가상 그래프 빌드 개시 ===")

    # 💡 [핵심 교정 1] 인프라가 100% 완벽하게 개통된 후, Neo4j 메모리 프로젝션을 위해 먼저 동적 임포트
    from apps.mas02_reroute.rerouting import TransportApp
    config.app = TransportApp()
    
    config.logger.info("[GDS 프로젝트] 155만 건 교통망 메모리 프로젝션 연산 중... (스레드 풀 격리)")
    await config.app.delete_gds_graph()
    await config.app.build_gds_graph1()
    await config.app.build_gds_graph2()
    await config.app.build_gds_graph3()
    config.logger.info("[GDS 프로젝트] 가상 그래프 3개소 초고속 메모리 빌드 완료!")


    print(f"redis_client : {config.redis_client}")
    config.logger.info("=== [System] 4대 메인 워커 관제를 시작합니다. 무전 대기 ===")

    # 에이전트와 워커 파일들이 인프라 성공 주소를 강제로 물고 태어나도록, 
    # 모든 셋업이 완벽히 끝난 바로 이 라인에서 워커들을 동적으로 수입(Import)합니다.
    from apps.mas01_incident.workers import redis_topis_listener, mysql_topis_listener, redis_stream_end_time_cleaner
    from apps.mas02_reroute.workers import redis_incident_consumer_and_rerouter

    # 격리된 태스크 스케줄링 가동
    task_topis1 = asyncio.create_task(redis_topis_listener())
    task_topis2 = asyncio.create_task(mysql_topis_listener())
    task_reroute = asyncio.create_task(redis_incident_consumer_and_rerouter())
    task_cleaner = asyncio.create_task(redis_stream_end_time_cleaner())

    await asyncio.sleep(0.5)

    try:
        await asyncio.gather(task_topis1, task_topis2, task_reroute, task_cleaner)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        config.logger.info("=== [System] 프로세스가 키보드 입력에 의해 강제 종료되었습니다 ===")
        sys.exit(0)