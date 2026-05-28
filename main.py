import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import init_db_connections, close_db_connections, init_gdf, logger
import config
from routers.mas01_router import mas01_router
from apps.mas01_incident.workers import redis_topis_listener, mysql_topis_listener
# from apps.mas2_router.workers import router_stream_listener # 추후 확장 시 주석 해제

def handle_worker_result(task: asyncio.Task):
    """백그라운드 태스크가 도중에 종료되었을 때 예외를 캐치하는 콜백 함수"""
    try:
        task.result()
    except asyncio.CancelledError:
        pass  # 정상적인 종료(Cancel)는 무시
    except Exception as e:
        # 여기서 에러 로그를 남기거나 서버를 재시작하는 등의 조치를 취할 수 있습니다.
        logger.error(f"=== [Critical] 백그라운드 워커 내부 에러 발생: {e} ===", exc_info=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # [Startup] 서버가 켜질 때 백그라운드에서 MAS 백그라운드 워커 구동
    logger.info("=== [System] FastAPI 시작 및 DB 연결, 데이터 로드 ===")
    await init_db_connections()
    # await init_gdf()
    
    logger.info("=== [System] FastAPI 시작 및 MAS 워커 가동 ===")
    # MAS1 워커 가동 (Redis Stream 구독 시작)
    mas01_task1 = asyncio.create_task(redis_topis_listener())
    mas01_task2 = asyncio.create_task(mysql_topis_listener())
    
    mas01_task1.add_done_callback(handle_worker_result)
    mas01_task2.add_done_callback(handle_worker_result)
    
    # 만약 MAS2가 추가된다면 아래처럼 타스크만 추가해주면 레이어가 분리됩니다.
    # mas2_task = asyncio.create_task(router_stream_listener())
    
    yield
    
    logger.info("=== [System] FastAPI 종료 및 DB 연결 해제 ===")
    await close_db_connections()
    
    # [Shutdown] 서버가 꺼질 때 백그라운드 워커 안전하게 안전하게 종료
    logger.info("=== [System] FastAPI 종료 및 MAS 워커 정지 ===")
    mas01_task1.cancel()
    mas01_task2.cancel()
    await asyncio.gather(mas01_task1, mas01_task2, return_exceptions=True)
    
    try:
        await mas01_task1
        await mas01_task2
        
    except asyncio.CancelledError:
        pass

# app = FastAPI(lifespan=lifespan)

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

app.include_router(mas01_router, prefix=config.API_PREFIX)

@app.get("/health")
def health_check():
    return {"status": "healthy", "active_systems": ["MAS1"]}

if __name__ == "__main__" :
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)