# apps/mas02_reroute/alert.py
import json
import datetime
from zoneinfo import ZoneInfo

from openai import AsyncOpenAI

from config import logger
import config

async def summarize_notice(payload:dict) :
    """
    [MAS02 LLM Worker] 교통 공지사항 원본 데이터를 정형화된 알림 알람 JSON으로 요약합니다.
    """
    kanana_client = AsyncOpenAI(base_url=config.KANANA_MODEL_02_URL, api_key="fake-key")
    
    system_instruction = """
    당신은 교통 정보 알림 봇입니다.
    교통 공지사항 정보로부터 사용자가 휴대폰 어플리케이션에서 어떤 문제가 생겼는지 한 눈에 알아볼 수 있도록 어플리케이션 알림을 명확하고 간결하게 작성해야 합니다.
    통제되는 도로, 대중교통(버스, 지하철) 내용 중심으로 간결하게 작성해주세요.
    
    다음과 같은 순수 JSON 형식으로 작성해주세요.
    반드시 순수 JSON 형식으로 작성해야 합니다.
    
    공지사항 내용 :
    
    {'address': 'None',
    'affected': '남부순환로 (김포공항입구 → 공항동천주교회앞)지하차도 옆 하위차로',
    'anchor_node': '지하차도 옆 하위차로',
    'content': '하위차로 시설물보수',
    'endDateTime': '2026-06-04 18:00:00',
    'end_node': '공항동천주교회앞',
    'gu': '강서구',
    'incident_id': '983bf58e9fa5b586fd512c503625e11a',
    'lat': '37.56099476063245',
    'lng': '126.80714845247178',
    'location_type': 'LINEAR_REFERENCE',
    'offset_end': 'None',
    'offset_start': 'None',
    'road_name': '남부순환로',
    'si': '서울특별시',
    'startDateTime': '2026-06-04 08:04:00',
    'start_node': '김포공항입구'}

    
    생성한 JSON(반드시 JSON 내용만 출력할 것) :
    
    {
        "incident_id" : "983bf58e9fa5b586fd512c503625e11a",
        "incident": "남부순환로(김포공항입구 → 공항동천주교회앞) 지하차도 옆 하위차로 시설물 보수공사로 인해 해당 구간 차량 및 대중교통 정체 예상",
        "start": "2026-06-04 08:04:00",
        "end": "2026-06-04 18:00:00"
    }
    """
    response = await kanana_client.chat.completions.create(
        model="kakaocorp/kanana-1.5-8b-instruct-2505",
        messages=[
            {
                "role" : "system",
                "content" : system_instruction
            },
            {
                "role" : "user",
                "content" : f"교통 공지사항 정보 :\n {payload}"
            }
        ],
        max_tokens=3000,
        temperature=0.1, # 답변의 일관성을 위해 0.2~0.3 유지 권장
    )

    result = json.loads(response.choices[0].message.content)
    logger.info(f"[MAS02 incident summary] outputs : {result}")
    return result

async def process_and_save_alerts(payload: dict, affected_user_ids: list):
    """
    [핵심 알림 적재 통제소]
    1. 사건 요약 메타가 Redis에 없으면 LLM을 호출해 최초 1번 생성 후 동적 TTL 캐싱 수행.
    2. 영향권 아래 놓인 유저들의 Set 주소록에 사건 ID 꽂아 넣기.
    """
    redis_client = config.redis_client
    incident_id = payload.get("incident_id")
    meta_key = f"incident:meta:{incident_id}"
    
    # [방어막 1] 캐싱 레이어 점검 - 이미 순찰 루프나 이전 구역에서 요약한 적이 있는지 검증
    cached_meta = await redis_client.get(meta_key)
    
    if not cached_meta:
        # 최초 발견된 사건이므로 LLM 요약 진행
        summary_result = await summarize_notice(payload)
        
        # [동적 TTL 연산부] 사건 종료 시간(endDateTime)에 맞춘 수명 설정
        end_time_str = payload.get("endDateTime")
        try:
            end_dt = datetime.datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
            now_dt = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d : %H:%M:%S")
            
            ttl_seconds = int((end_dt - now_dt).total_seconds())
        except Exception:
            ttl_seconds = 86400 # 파싱 에러 방어벽: 기본 24시간 지정
            # ttl_seconds = 3600
            
        # 사건 메타 캐시 적재 (사건 개별 TTL 작동 개시)
        await redis_client.set(name=meta_key, value=json.dumps(summary_result, ensure_ascii=False), ex=ttl_seconds)
        logger.info(f"[mas02 alert.py Redis Save] 신규 사건 메타 캐시 완료: {incident_id} (TTL: {ttl_seconds}초)")
        
    # 3. 영향권에 포함된 유저 리스트를 돌며 주소록(Set)에 링킹 연산 수행
    summary_str = json.dumps(summary_result, ensure_ascii=False)
    for user_id in affected_user_ids:
        user_incident_set_key = f"user:incidents:{user_id}"
        inserted = await redis_client.sadd(user_incident_set_key, summary_str)
        
        if inserted :
            await redis_client.expire(user_incident_set_key, 172800)
        
    logger.info(f"[mas02 alert.py Reroute Notification] 총 {len(affected_user_ids)}명의 유저 알림창에 사건 [{incident_id}] 배달 완료.")