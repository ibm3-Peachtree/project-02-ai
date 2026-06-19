# project-02-ai
## Structure
```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              run_pipeline.py — 데몬 부트스트랩                                    │
│                                                                                                  │
│  init_db_connections()  →  init_gdf()  →  TransportApp()  →  build_gds_graph × 3               │
│  (Redis · Neo4j · MySQL)   (GIS 270만 건)  (Neo4j GDS 메모리     (버스/지하철/복합               │
│                                             프로젝션 객체)          가상 그래프 빌드)              │
└──────────────────────────────────────────┬──────────────────────────────────────────────────────┘
                                           │  asyncio.create_task × 4
              ┌────────────────────────────┼─────────────────────────────────────────┐
              │                            │                                         │
              ▼                            ▼                                         ▼
┌─────────────────────────┐  ┌─────────────────────────┐            ┌───────────────────────────┐
│  [MAS01 Worker 1]       │  │  [MAS01 Worker 2]       │            │  [MAS01 Worker 3]         │
│  redis_topis_listener   │  │  mysql_topis_listener   │            │  stream_end_time_cleaner  │
│                         │  │                         │            │                           │
│  Redis Stream 감시       │  │  MySQL topis_notice     │            │  5분 주기 만료 스캔        │
│  (서울 25개 구 × stream) │  │  폴링 (60초 주기)        │            │  endDateTime 초과 건       │
│  XREADGROUP block=500ms │  │                         │            │  → Neo4j DETACH DELETE    │
│  count=5               │  │  SELECT WHERE            │            │  → Redis XDEL             │
└────────────┬────────────┘  │  start <= now < end     │            └───────────────────────────┘
             │               └────────────┬────────────┘
             │      check_duplicate()     │
             └──────────────┬─────────────┘
                            │  is_new == True
                            ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                        MAS01 LangGraph Agent (agents.py)                             │
│                                                                                      │
│  raw_incident_data (Redis payload or MySQL dict)                                     │
│         │                                                                            │
│         ▼                                                                            │
│  ┌─────────────┐                                                                     │
│  │  node_ner   │  ──►  교통 공지 → 통제/우회 entity 추출                              │
│  │             │       {entity, obj(통제|우회), meta(도로|버스|버스정류장|지하철)}       │
│  └──────┬──────┘                                                                     │
│         ▼                                                                            │
│  ┌─────────────────┐                                                                 │
│  │ node_preprocess │  ──►  구간 기호 통일("-"), ARS ID 정제,                           │
│  │                 │       다중 구간 분리, 버스 번호 쪼개기                              │
│  └──────┬──────────┘                                                                 │
│         ▼                                                                            │
│  ┌──────────────────────────┐                                                        │
│  │ node_location_type_      │  ──►  각 entity → location_type 분류                   │
│  │ classify                 │       BETWEEN_NODES / LINEAR_REFERENCE /               │
│  │                          │       ADDRESS_POINT / BUSSTOP / BUS / SUBWAY           │
│  └──────┬───────────────────┘                                                        │
│         │                                                                            │
│         ▼  (순차 실행 — obj=="통제" 필터링 후 해당 type만 처리)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  ┌──────────┐  ┌──────────┐  │
│  │node_address  │→ │node_linear   │→ │node_between   │→ │node_bus  │→ │node_sub  │  │
│  │_parser       │  │_parser       │  │_parser        │  │stop_     │  │way_      │  │
│  │              │  │              │  │               │  │parser    │  │parser    │  │
│  │ADDRESS_POINT │  │LINEAR_REF    │  │BETWEEN_NODES  │  │BUSSTOP   │  │SUBWAY    │  │
│  │→ 지번/도로명  │  │→ 기준점+거리  │  │→ 시작-종점    │  │→ ARS ID  │  │→ 호선+역  │  │
│  │  주소 파싱    │  │  오프셋 파싱  │  │  구간 파싱    │  │  파싱    │  │  파싱    │  │
│  └──────────────┘  └──────────────┘  └───────────────┘  └──────────┘  └──────────┘  │
│         │  temp_outputs에 누적 (extend)                                               │
│         ▼                                                                            │
│  ┌──────────────────────┐                                                            │
│  │ node_enrich_         │  ──►  GIS SHP 함수로 좌표 보강                              │
│  │ coordinates          │       resolve_between_nodes()                              │
│  │                      │       resolve_linear_reference()                           │
│  │                      │       resolve_address_point()                              │
│  │                      │       + lat/lng 축 전도 자동 보정                           │
│  └──────────┬───────────┘                                                            │
│             ▼                                                                        │
│  ┌──────────────────────┐                                                            │
│  │  node_apply_neo4j    │  ──►  location_type별 5종 Cypher 분기                      │
│  │                      │       ① 좌표 반경 200m 버스정류장 매핑    (일반 좌표)         │
│  │                      │       ② ARS ID 직접 매핑               (BUSSTOP)          │
│  │                      │       ③ 노선 구간 shortestPath 매핑     (SUBWAY 구간)       │
│  │                      │       ④ 단일 역 매핑                   (SUBWAY 단역)        │
│  │                      │       ⑤ 노선 전체 매핑                 (SUBWAY 전노선)      │
│  │                      │                                                            │
│  │                      │       MERGE (s:Station)-[:AFFECTED_BY]->(i:Incident)      │
│  └──────────┬───────────┘                                                            │
│             │  Neo4j 커밋 완료 후                                                     │
│             ▼                                                                        │
│       publish_to_channel(gu, si, node)                                               │
│       → Redis Stream 발행                                                            │
│         incident:stream:서울특별시:{gu}                                               │
└──────────────────────────────────────────────────────────────────────────────────────┘
                            │
                            │  Redis Stream XREADGROUP
                            ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                  [MAS02 Worker] redis_incident_consumer_and_rerouter                 │
│                                                                                      │
│  신규 사고 스트림 수신 (block=10000ms, count=1)                                        │
│  +                                                                                   │
│  patrol_active_incidents_loop  ──►  15초 주기 전체 스트림 스캔 (중간 난입 유저 포착)    │
│                                                                                      │
│  ① get_affected_coordinates_from_neo4j(incident_id)                                 │
│     → Incident 노드에 연결된 Station들의 좌표 배열 획득                                 │
│  ② get_active_users_by_coordinates(affected_coords, incident_id)                    │
│     → Redis GEOEARCH로 해당 좌표 반경 내 활성 유저 ID 추출                              │
│     → 이미 우회 처리된 유저(reroute:history 키) 필터링                                 │
│  ③ summarize_notice(payload)  → LLM 공지 요약 (캐시 우선 / 캐시 미스 시 LLM 호출)      │
│  ④ process_and_save_alerts(result, user_ids)  → 유저 알림 저장                        │
└──────────────────────────────────────┬───────────────────────────────────────────────┘
                                       │  affected_user_ids 순회
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                        MAS02 LangGraph Agent (agents.py)                             │
│                                                                                      │
│  initial_state = {incident_id, user_id, user_live_route_xy}                         │
│         │                                                                            │
│         ▼                                                                            │
│  ┌────────────────────────────┐                                                      │
│  │ fetch_user_realtime_       │  ──►  Redis lrange → 최신 GPS 로그 파싱               │
│  │ context                    │       user_live_route_xy 배열 내 최근접 정류장 인덱스  │
│  │                            │       계산 (Haversine 최소 거리)                      │
│  │                            │       + get_incident_meta_data(incident_id)          │
│  └──────────┬─────────────────┘                                                      │
│             ▼                                                                        │
│  ┌────────────────────────────┐                                                      │
│  │ extract_routing_station_   │  ──►  LLM → 현재 인덱스 기준 [idx+1] or [idx+2]      │
│  │ names                      │       정류장을 start_node로 선정                      │
│  │                            │       end_node = user_live_route_xy 배열의 마지막     │
│  │                            │       유효 좌표 (역방향 탐색)                          │
│  └──────────┬─────────────────┘                                                      │
│             ▼                                                                        │
│  ┌────────────────────────────┐                                                      │
│  │ resolve_neo4j_node_ids     │  ──►  ARS ID → BUS 마스터 node_id                   │
│  │                            │       호선+역명 → SUBWAY 마스터 node_id               │
│  │                            │       도착지 → 반경 1km 최근접 마스터 node_id         │
│  └──────────┬─────────────────┘                                                      │
│             ▼                                                                        │
│  ┌────────────────────────────┐                                                      │
│  │ generate_and_format_routes │  ──►  GDS 메모리 그래프 경로 탐색 3종                 │
│  │                            │       get_optimal_path1() : 환승 최소                │
│  │                            │       get_optimal_path2() : 시간 최소                │
│  │                            │       get_optimal_path3() : 복합 최적                │
│  │                            │       → format_perfect_routing_paths() 정제          │
│  └──────────┬─────────────────┘                                                      │
│             ▼                                                                        │
│  ┌────────────────────────────┐                                                      │
│  │ generate_total_cost        │  ──►  요금·소요시간 합산                              │
│  │                            │       JSON 직렬화                                    │
│  └──────────┬─────────────────┘                                                      │
└─────────────┼────────────────────────────────────────────────────────────────────────┘
              │  final_rerouting_paths
              ▼
┌─────────────────────────────────────────┐
│  Redis SET                              │
│  routine:live:incident:full:{user_id}   │  ──►  TTL = endDateTime - 현재시각 (초)
│                                         │
│  Redis SET                              │
│  user:{user_id}:reroute:history:        │  ──►  TTL = 24h (중복 우회 방지 플래그)
│  {incident_id}  =  "DONE"              │
└─────────────────────────────────────────┘
```

## 실행 방법
### 0. 패키지 설치
```bash
pip install -r requirements.txt
```

### 1. vllm 실행
```bash
nohup ./vllm.sh > vllm.log 2>&1 &
```
### 2. python 실행
```bash
python run_pipeline.py
```

## 필요 data & API
- [전국표준노드링크](https://www.its.go.kr/nodelink/nodelinkRef)
- [카카오 키워드로 장소 검색 API](https://developers.kakao.com/docs/ko/local/dev-guide#search-by-keyword)
- [카카오 주소로 좌표 검색 API](https://developers.kakao.com/docs/ko/local/dev-guide#address-coord)


## Requirements.txt 생성
``` bash
pip list --format=freeze > requirements.txt
```

## 커맨드
```bash
watch -n 1 free -h
watch -n 0.5 nvidia-smi
```