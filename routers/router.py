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
    if data :
        data = json.loads(data)
    if not data :
        data = []
    logger.info(f"[routers/router.py ] 데이터 조회 (Key: {user_key})")
    
    return data

@router.get("/summarize_topis")
async def summarize_topis(user_id) :
    redis_client = config.redis_client
    
    user_incident_set_key = f"user:incidents:{user_id}"
    
    # 1. 유저 키에 매핑된 모든 Set 원소(JSON 문자열들)를 한 번에 긁어옴
    raw_members = await redis_client.smembers(user_incident_set_key)
    
    if not raw_members:
        return []
        
    # 2. 문자열들을 다시 예쁜 파이썬 딕셔너리 객체로 되돌려 최종 리스트로 묶기
    user_timeline = []
    for member in raw_members:
        if isinstance(member, bytes):
            member = member.decode('utf-8')
            
        try:
            user_timeline.append(json.loads(member))
        except Exception as e:
            logger.error(f"타임라인 파싱 실패: {e}")
            continue
            
    # 3. Spring Boot나 Fast API 응답 레이어로 던져줄 깔끔한 JSON 배열 형태 반환!
    return user_timeline