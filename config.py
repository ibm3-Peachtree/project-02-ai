# config.py

import os
import logging
from dotenv import load_dotenv
import asyncio
from sshtunnel import SSHTunnelForwarder
import redis.asyncio as aioredis
from neo4j import AsyncGraphDatabase
import aiomysql
import geopandas as gpd

from apps.mas02_reroute.rerouting import TransportApp

load_dotenv()

logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)

API_PREFIX = "/api/v0/ai"

BASTION_HOST = os.getenv("BASTION_HOST")
BASTION_USER = os.getenv("BASTION_USER")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_LOCAL_BIND_PORT = os.getenv("REDIS_LOCAL_BIND_PORT")

NEO4J_HOST = os.getenv("NEO4J_HOST")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PORT = os.getenv("NEO4J_PORT")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_LOCAL_BIND_PORT = os.getenv("NEO4J_LOCAL_BIND_PORT")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_LOCAL_BIND_PORT = os.getenv("MYSQL_LOCAL_BIND_PORT")
MYSQL_DB = os.getenv("MYSQL_DB")

KAKAO_RESTAPI = os.getenv("KAKAO_RESTAPI")
KANANA_MODEL_01_URL = "http://127.0.0.1:8001/v1" # kakaocorp/kanana-1.5-2.1b-instruct-2505
KANANA_MODEL_02_URL = "http://127.0.0.1:8002/v1" # kakaocorp/kanana-1.5-2.1b-instruct-2505

SEOUL_ROADNAME_API = os.getenv("SEOUL_ROADNAME_API")


# 초기 상태는 None
ssh_tunnel_redis = None
ssh_tunnel_neo4j = None
ssh_tunnel_mysql = None

redis_client = None
neo4j_client = None
mysql_pool = None

SHP_DIR = os.path.join(os.path.dirname(__file__), "[2026-01-13]NODELINKDATA")
LINK_GDF = None
NODE_GDF = None
app = None

async def init_gdf() :
    global LINK_GDF, NODE_GDF
    logger.info("config.init_gdf 데이터 로드 시작")
    LINK_GDF = gpd.read_file(os.path.join(SHP_DIR, "MOCT_LINK.shp"), encoding="cp949").to_crs(epsg=5179)
    NODE_GDF = gpd.read_file(os.path.join(SHP_DIR, "MOCT_NODE.shp"), encoding="cp949").to_crs(epsg=5179)
    
    logger.info(f"config.init_gdf LINK 데이터 로드 완료 (건수: {len(LINK_GDF)})")
    logger.info(f"config.init_gdf NODE 데이터 로드 완료 (건수: {len(NODE_GDF)})")
    logger.info("config.init_gdf GIS 인프라 데이터 프리로드 성공!")

async def init_db_connections():
    global ssh_tunnel_redis, ssh_tunnel_neo4j, ssh_tunnel_mysql
    global redis_client, neo4j_client, mysql_pool
    
    loop = asyncio.get_running_loop()

    # Redis SSH 터널 활성화
    ssh_tunnel_redis = SSHTunnelForwarder(
        (BASTION_HOST, 22),
        ssh_username=BASTION_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(REDIS_HOST, int(REDIS_PORT)),
        local_bind_address=('127.0.0.1', int(REDIS_LOCAL_BIND_PORT))
    )
    await loop.run_in_executor(None, ssh_tunnel_redis.start)
    logger.info("config.init_db_connections : 1 Redis SSH 터널 활성화 성공")
    
    # Neo4j SSH 터널 활성화
    ssh_tunnel_neo4j = SSHTunnelForwarder(
        (BASTION_HOST, 22),
        ssh_username=BASTION_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(NEO4J_HOST, int(NEO4J_PORT)),
        local_bind_address=('127.0.0.1', int(NEO4J_LOCAL_BIND_PORT))
    )
    await loop.run_in_executor(None, ssh_tunnel_neo4j.start)
    logger.info("config.init_db_connections : 2 Neo4j SSH 터널 활성화 성공")
    
    # MySQL SSH 터널 활성화
    ssh_tunnel_mysql = SSHTunnelForwarder(
        (BASTION_HOST, 22),
        ssh_username=BASTION_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(MYSQL_HOST, int(MYSQL_PORT)),
        local_bind_address=('127.0.0.1', int(MYSQL_LOCAL_BIND_PORT))
    )
    await loop.run_in_executor(None, ssh_tunnel_mysql.start)
    logger.info("config.init_db_connections : 3 MySQL SSH 터널 활성화 성공")

    # 실객체 할당 및 전역 컨텍스트 바인딩
    local_redis_port = ssh_tunnel_redis.local_bind_port
    redis_client = aioredis.from_url(
        f"redis://127.0.0.1:{local_redis_port}", 
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    logger.info("config.init_db_connections : 4 터널 바인딩 완료: Redis 클라이언트")
    
    local_neo4j_port = ssh_tunnel_neo4j.local_bind_port
    neo4j_client = AsyncGraphDatabase.driver(
        f"bolt://127.0.0.1:{local_neo4j_port}", 
        auth=("neo4j", NEO4J_PASSWORD)
    )
    logger.info("config.init_db_connections : 4 터널 바인딩 완료: Neo4j 드라이버")
    
    app = TransportApp()
    
    await app.delete_gds_graph()
    await app.build_gds_graph1()
    await app.build_gds_graph2()
    await app.build_gds_graph3()
    
    logger.info("config.init_db_connections : Neo4j GDS graph 초기화")
    
    local_mysql_port = ssh_tunnel_mysql.local_bind_port
    mysql_pool = await aiomysql.create_pool(
        host='127.0.0.1',
        port=local_mysql_port,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        autocommit=True,
        minsize=1,
        maxsize=10,
        loop=loop
    )
    logger.info("config.init_db_connections : 4 터널 바인딩 완료: MySQL 커넥션 풀")
    
    logger.info("config.init_db_connections : 모든 인프라 SSH 터널 및 글로벌 DB 클라이언트 개통 성공")


async def close_db_connections():
    global ssh_tunnel_redis, ssh_tunnel_neo4j, ssh_tunnel_mysql
    global redis_client, neo4j_client, mysql_pool
    
    if redis_client:
        await redis_client.close()
    if neo4j_client:
        await neo4j_client.close()
    if mysql_pool: 
        mysql_pool.close()
        await mysql_pool.wait_closed()    
    
    loop = asyncio.get_running_loop()
    if ssh_tunnel_redis:
        await loop.run_in_executor(None, ssh_tunnel_redis.stop)
    if ssh_tunnel_neo4j:
        await loop.run_in_executor(None, ssh_tunnel_neo4j.stop)
    if ssh_tunnel_mysql:
        await loop.run_in_executor(None, ssh_tunnel_mysql.stop)
        
    logger.info("config.close_db_connections : SSH 터널 및 DB 클라이언트 자원 회수")
    
