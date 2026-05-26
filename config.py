# config.py

import os
import logging
from dotenv import load_dotenv
import asyncio
from sshtunnel import SSHTunnelForwarder
import redis.asyncio as aioredis              # 💡 지난 코드에서 빠진 import도 추가하세요!
from neo4j import AsyncGraphDatabase

load_dotenv()

logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)

API_PREFIX = "/api/v0/ai"

BASTION_HOST=os.getenv("BASTION_HOST")
BASTION_USER=os.getenv("BASTION_USER")
SSH_KEY_PATH=os.getenv("SSH_KEY_PATH")

REDIS_HOST=os.getenv("REDIS_HOST")
REDIS_PORT=os.getenv("REDIS_PORT")
REDIS_PASSWORD=os.getenv("REDIS_PASSWORD")
REDIS_LOCAL_BIND_PORT=os.getenv("REDIS_LOCAL_BIND_PORT")

NEO4J_HOST=os.getenv("NEO4J_HOST")
NEO4J_USER=os.getenv("NEO4J_USER")
NEO4J_PORT=os.getenv("NEO4J_PORT")
NEO4J_PASSWORD=os.getenv("NEO4J_PASSWORD")
NEO4J_LOCAL_BIND_PORT=os.getenv("NEO4J_LOCAL_BIND_PORT")

ssh_tunnel_redis = None
ssh_tunnel_neo4j = None

redis_client = None
neo4j_client = None  # Neo4j Driver 객체


async def init_db_connections():
    global ssh_tunnel_redis, ssh_tunnel_neo4j, redis_client, neo4j_client
    
    # 루프 이벤트를 가져옵니다 (sshtunnel 백그라운드 스레드와 연동 안정성 확보)
    loop = asyncio.get_running_loop()

    # 1️⃣ Redis SSH 터널 활성화 (로컬 임의 포트 -> 원격 Redis 포트)
    ssh_tunnel_redis = SSHTunnelForwarder(
        (BASTION_HOST, 22),
        ssh_username=BASTION_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(REDIS_HOST, int(REDIS_PORT)),
        local_bind_address=('127.0.0.1', int(REDIS_LOCAL_BIND_PORT))
    )
    
    # 동기 함수인 .start()를 비동기 루프에서 실행
    await loop.run_in_executor(None, ssh_tunnel_redis.start)
    
    logger.info("1️⃣ Redis SSH 터널 활성화 (로컬 임의 포트 -> 원격 Redis 포트)")
    
    # 2️⃣ Neo4j SSH 터널 활성화 (로컬 임의 포트 -> 원격 Neo4j Bolt 포트)
    ssh_tunnel_neo4j = SSHTunnelForwarder(
        (BASTION_HOST, 22),
        ssh_username=BASTION_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(NEO4J_HOST, int(NEO4J_PORT)),
        local_bind_address=('127.0.0.1', int(NEO4J_LOCAL_BIND_PORT))
    )
    
    await loop.run_in_executor(None, ssh_tunnel_neo4j.start)
    
    logger.info("2️⃣ Neo4j SSH 터널 활성화 (로컬 임의 포트 -> 원격 Neo4j Bolt 포트)")

    # 3️⃣ 터널이 열어준 로컬 포트로 클라이언트 연결
    # Redis 연결
    local_redis_port = ssh_tunnel_redis.local_bind_port
    redis_client = aioredis.from_url(
        f"redis://127.0.0.1:{local_redis_port}", 
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    
    logger.info("3️⃣ 터널이 열어준 로컬 포트로 클라이언트 연결 : Redis")
    
    # Neo4j 연결 (Bolt 프로토콜 이용)
    local_neo4j_port = ssh_tunnel_neo4j.local_bind_port
    neo4j_client = AsyncGraphDatabase.driver(
        f"bolt://127.0.0.1:{local_neo4j_port}", 
        auth=("neo4j", NEO4J_PASSWORD)
    )
    
    logger.info("3️⃣ 터널이 열어준 로컬 포트로 클라이언트 연결 : Neo4j")
    logger.info(f"✅ SSH 터널 및 DB 클라이언트 연결 성공!")
    logger.info(f"👉 Redis 포트 맵핑: {local_redis_port} -> {REDIS_PORT}")
    logger.info(f"👉 Neo4j 포트 맵핑: {local_neo4j_port} -> {NEO4J_PORT}")


async def close_db_connections():
    global ssh_tunnel_redis, ssh_tunnel_neo4j, redis_client, neo4j_client
    
    # 클라이언트 및 터널 종료
    if redis_client:
        await redis_client.close()
    if neo4j_client:
        await neo4j_client.close()
        
    loop = asyncio.get_running_loop()
    if ssh_tunnel_redis:
        await loop.run_in_executor(None, ssh_tunnel_redis.stop)
    if ssh_tunnel_neo4j:
        await loop.run_in_executor(None, ssh_tunnel_neo4j.stop)
        
    logger.info("🔒 SSH 터널 및 DB 클라이언트 안전하게 종료됨.")