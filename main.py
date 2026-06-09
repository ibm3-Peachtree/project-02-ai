import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import init_db_connections, close_db_connections, init_gdf, logger
import config
from routers.router import router
from apps.mas01_incident.workers import redis_topis_listener, mysql_topis_listener, redis_stream_end_time_cleaner
from apps.mas02_reroute.workers import redis_incident_consumer_and_rerouter

def handle_worker_result(task: asyncio.Task):
    """백그라운드 태스크가 도중에 종료되었을 때 예외를 캐치하는 콜백 함수"""
    try:
        task.result()
    except asyncio.CancelledError:
        pass  # 정상적인 종료(Cancel)는 무시
    except Exception as e:
        logger.error(f"=== [Critical] 백그라운드 워커 내부 에러 발생: {e} ===", exc_info=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 서버가 켜질 때 백그라운드에서 MAS 백그라운드 워커 구동
    logger.info("=== [System] FastAPI 시작 및 DB 연결, 데이터 로드 ===")
    await init_db_connections()
    await init_gdf()
    
    logger.info("🧹 [System] 테스트용 임시 Incident 노드 및 AFFECTED_BY 관계 일괄 청소 시작...")
    try:
        cleanup_cypher = """
        MATCH (i:Incident)
        DETACH DELETE i
        """
        async with config.neo4j_client.session() as session:
            result = await session.run(cleanup_cypher)

            summary = await result.consume()
            nodes_deleted = summary.counters.nodes_deleted
            relationships_deleted = summary.counters.relationships_deleted
            
            logger.info(f"실제 서비스에서 삭제하기[Neo4j Cleanup] 청소 완료 (삭제된 노드: {nodes_deleted}개, 삭제된 관계: {relationships_deleted}개)")
    except Exception as e:
        logger.error(f"실제 서비스에서 삭제하기 [Neo4j Cleanup Error] 셧다운 청소 중 예외 발생: {e}")
        
    
    logger.info("=== [System] FastAPI 시작 및 MAS 워커 가동 ===")
    # 워커 가동 
    mas01_task1 = asyncio.create_task(redis_topis_listener())
    mas01_task2 = asyncio.create_task(mysql_topis_listener())
    mas02_reroute_task = asyncio.create_task(redis_incident_consumer_and_rerouter())
    cleaner_task = asyncio.create_task(redis_stream_end_time_cleaner())
    
    mas01_task1.add_done_callback(handle_worker_result)
    mas01_task2.add_done_callback(handle_worker_result)
    mas02_reroute_task.add_done_callback(handle_worker_result)
    
    yield
    
    logger.info("=== [System] FastAPI 종료 및 DB 연결 해제 ===")
    await close_db_connections()
    
    # 서버가 꺼질 때 백그라운드 워커 안전하게 안전하게 종료
    logger.info("=== [System] FastAPI 종료 및 MAS 워커 정지 ===")
    
    mas01_task1.cancel()
    mas01_task2.cancel()
    mas02_reroute_task.cancel()
    cleaner_task.cancel()
    await asyncio.gather(
        mas01_task1, 
        mas01_task2, 
        cleaner_task, 
        mas02_reroute_task,
        return_exceptions=True
    )
    
    try:
        await mas01_task1
        await mas01_task2
        await mas02_reroute_task
        
    except asyncio.CancelledError:
        pass

app = FastAPI(
    title="AI Service API",
    description="AI 생성을 위한 API 서버",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS 설정 추가
app.add_middleware(
    CORSMiddleware,
    # 허용할 도메인 (프론트엔드 주소)
    allow_origins=["*"], # http://192.168.0.79
    # 쿠키나 인증 정보를 포함할지 여부
    allow_credentials=True, 
    # 허용할 HTTP 메서드
    allow_methods=["*"], 
    # 허용할 HTTP 헤더
    allow_headers=["*"],
)

app.include_router(router, prefix=config.API_PREFIX)

@app.get("/health")
def health_check():
    return {"status": "healthy", "active_systems": ["MAS1", "MAS2_Rerouter"]}

if __name__ == "__main__" :
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)