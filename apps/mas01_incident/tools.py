# apps/mas01_incident/tools.py
import hashlib
import json
from datetime import datetime

from config import logger
import config

async def check_duplicate(incident_data: dict) -> bool:
    try:
        # 1. 내용(info) 데이터 추출 및 전처리 (양끝 공백 제거)
        info_text = str(incident_data.get("info", "")).strip()
        
        if not info_text:
            logger.warning("[Check Duplicate] info 내용이 없어 검증을 우회합니다.")
            return True
            
        # 2. info 전체 문장을 고유한 MD5 해시 문자열로 변환
        hash_generator = hashlib.md5()
        hash_generator.update(info_text.encode("utf-8"))
        info_hash = hash_generator.hexdigest()
        
        # 3. Redis Key 설계 (텍스트 내용 고유 해시 적용)
        dedup_key = f"incident:dedup:{incident_data['si']}:{incident_data['gu']}:{info_hash}"
        
        # 4. 종료 시간 기반 TTL 계산
        ttl_seconds = 7200
        end_time_str = incident_data.get("endDateTime")
        if end_time_str:
            try:
                end_dt = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                time_delta = (end_dt - datetime.now()).total_seconds()
                ttl_seconds = max(int(time_delta), 7200)
            except ValueError:
                pass
        
        # 5. Redis SETNX 실행
        is_new = await config.redis_client.set(
            name=dedup_key,
            value="active",
            ex=ttl_seconds,
            nx=True
        )
        
        is_new_bool = bool(is_new)
        
        if not is_new_bool:
            logger.info(f"[Agent 1] 중복 데이터 컷 -> 키: {dedup_key}")
        else:
            logger.info(f"[Agent 1] 신규 돌발 등록 -> 키: {dedup_key} (TTL: {ttl_seconds}초)")
            
        return is_new_bool

    except Exception as e:
        logger.error(f"[Check Duplicate Error] 예외 발생: {e}")
        return True



# async def publish_to_gu_channel(gu_name: str, enriched_data: dict):
#     """분석이 완료된 데이터를 서울시 구별 Pub/Sub 채널로 Broadcast"""
#     # 채널명 예시: incident:강남구
#     channel_key = f"incident:{gu_name}"
    
#     # 딕셔너리 데이터를 문자열(JSON)로 변환
#     payload_string = json.dumps(enriched_data, ensure_ascii=False)
    
#     # 채널에 가입(Subscribe)한 모든 리스너에게 동시에 데이터가 뿌려집니다.
#     await config.redis_client.publish(channel_key, payload_string)
#     print(f"📡 [Dispatcher] 채널 '{channel_key}'로 실시간 데이터 전파 완료!")