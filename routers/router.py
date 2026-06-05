import os
from fastapi import APIRouter
import httpx
from fastapi import Header, HTTPException, Depends, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt, JWTError
from dotenv import load_dotenv
import logging
import json

import config
from config import logger


router = APIRouter(prefix="/reroute")

# Security
security = HTTPBearer()
load_dotenv()

@router.get("/test")
async def test() :
    logger.info("/test 접근")
    return {"message" : "test 성공"}

@router.get("/live_reroute")
async def live_reroute(user_id) :
    user_key = f"routine:live:incident:full:{user_id}"
    
    data = await config.redis_client.get(user_key)
    data = json.loads(data)
    logger.info(f"[routers/router.py ] 데이터 조회 (Key: {user_key})")
    
    return data

@router.get("/summarize_topis")
async def summarize_topis(user_id) :
    user_key = f"user:incidents:{user_id}"
    
    data = await config.redis_client.get(user_key)
    data = json.loads(data)
    logger.info(f"[routers/router.py ] User별 Topis 정보 (data: {data})")
    return data