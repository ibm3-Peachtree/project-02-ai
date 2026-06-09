import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import os
from dotenv import load_dotenv
import redis.asyncio as aioredis
from sshtunnel import SSHTunnelForwarder

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_LOCAL_BIND_PORT = os.getenv("REDIS_LOCAL_BIND_PORT")

BASTION_HOST = os.getenv("BASTION_HOST")
BASTION_USER = os.getenv("BASTION_USER")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")

# 이미지에 나오는 정확한 Stream Key 지정
stream_key = "incident:서울특별시:중구:20260609:stream"

# 2. 임의로 넣을 샘플 데이터 목록
# dummy_incidents = [
#     {
#         "si": "서울특별시",
#         "gu": "동작구",
#         "info": "지하철 7호선 장승배기역 화재로 인한 통제",
#         "x": 126.938988,
#         "y": 37.504828,
#         "startDateTime": "2026-06-05 09:18:00",
#         "endDateTime": "2026-06-05 20:30:00",
#         "lat": 126.938988,
#         "lng": 37.504828
#     }
# ]
# dummy_incidents = [
#     {
#         "si": "서울특별시",
#         "gu": "중구",
#         "info": "서울역 인근 도로 시위로 인한 전면 통제",
#         "x": 126.972582,
#         "y": 37.555349,
#         "startDateTime": "2026-06-08 09:18:00",
#         "endDateTime": "2026-06-08 20:30:00",
#         "lat": 126.972582,
#         "lng": 37.555349
#     }
# ]

dummy_incidents = [
    {
        "si": "서울특별시",
        "gu": "중구",
        "info": "1호선 용산역 화재로 인한 전면 통제",
        "x": 126.964428,
        "y": 37.529679,
        "startDateTime": "2026-06-09 09:18:00",
        "endDateTime": "2026-06-09 20:30:00",
        "lat": 126.964428,
        "lng": 37.529679
    }
]

async def main():
    # SSH 터널은 동기 함수이므로 executor에서 실행하여 시작을 기다립니다.
    ssh_tunnel_redis = SSHTunnelForwarder(
        (BASTION_HOST, 22),
        ssh_username=BASTION_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(REDIS_HOST, int(REDIS_PORT)),
        local_bind_address=('127.0.0.1', int(REDIS_LOCAL_BIND_PORT))
    )
    
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, ssh_tunnel_redis.start)
    print("🚀 SSH 터널이 성공적으로 연결되었습니다.")

    # 실객체 할당 및 전역 컨텍스트 바인딩
    local_redis_port = ssh_tunnel_redis.local_bind_port
    redis_client = aioredis.from_url(
        f"redis://127.0.0.1:{local_redis_port}", 
        password=REDIS_PASSWORD,
        decode_responses=True
    )

    try:
        # 3. Stream에 데이터 삽입 (XADD 명령)
        for data in dummy_incidents:
            # 💡 딕셔너리 내부 키 이름을 데이터 구조에 맞게 변경했습니다.
            entry_id = await redis_client.xadd(stream_key, {
                "si": data["si"],
                "gu": data["gu"],
                "info": data["info"],
                "start": data["startDateTime"],
                "end": data["endDateTime"],
                "lat": str(data["lat"]),
                "lng": str(data["lng"]),
                "created_at": datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
            })
            print(f"✅ 데이터 삽입 성공! Entry ID: {entry_id}")
            
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        
    finally:
        # 연결 리소스 해제 및 터널 종료
        await redis_client.aclose()
        ssh_tunnel_redis.stop()
        print("🔒 Redis 연결 및 SSH 터널이 안전하게 닫혔습니다.")

if __name__ == "__main__":
    asyncio.run(main())