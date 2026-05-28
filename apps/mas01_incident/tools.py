# apps/mas01_incident/tools.py
import hashlib
import json
from datetime import datetime
import requests

import geopandas as gpd
from shapely.ops import nearest_points

from langchain_core.tools import tool

from config import logger
import config

async def check_duplicate(incident_data: dict, mode: str) -> bool:
    try:
        # 1. 내용(info) 데이터 추출 및 전처리 (양끝 공백 제거)
        info_text = None
        if mode == "redis" :
            info_text = str(incident_data.get("info", "")).strip()
        elif mode == "mysql" :
            info_text = incident_data['title'] + "\n" + incident_data['content']
        
        if not info_text:
            logger.warning("[Check Duplicate] info 내용이 없어 검증을 우회합니다.")
            return True
            
        # 2. info 전체 문장을 고유한 MD5 해시 문자열로 변환
        hash_generator = hashlib.md5()
        hash_generator.update(info_text.encode("utf-8"))
        info_hash = hash_generator.hexdigest()
        
        # 3. Redis Key 설계 (텍스트 내용 고유 해시 적용)
        dedup_key = f"incident:dedup:{info_hash}"
        
        # 4. 종료 시간 기반 TTL 계산
        ttl_seconds = 0
        end_time_str = incident_data.get("endDateTime") if mode == "redis" else str(incident_data.get("end_datetime"))
        if end_time_str:
            try:
                end_dt = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                # time_delta = (end_dt - datetime.now()).total_seconds()
                time_delta = (end_dt - datetime.strptime("2026-05-20 13:00:00", "%Y-%m-%d %H:%M:%S")).total_seconds()
                if time_delta <= 0:
                    return False # (또는 상황에 맞는 중복 컷 리턴 처리)
                
                ttl_seconds = max(int(time_delta), 0)
            except ValueError:
                pass
        
        # 5. Redis SETNX 실행
        is_new = await config.redis_client.set(
            name=dedup_key,
            value=info_text,
            ex=ttl_seconds,
            nx=True
        )
        
        is_new_bool = bool(is_new)
        
        if not is_new_bool:
            logger.info(f"[Agent 1] [{mode}] 중복 데이터 컷 -> 키: {dedup_key}")
        else:
            logger.info(f"[Agent 1] [{mode} ]신규 돌발 등록 -> 키: {dedup_key} (TTL: {ttl_seconds}초)")
            
        return is_new_bool

    except Exception as e:
        logger.error(f"[Check Duplicate Error] 예외 발생: {e}")
        return False

@tool
async def resolve_linear_reference(road_name, anchor_node, offset_start, offset_end):
    '''
    기준 노드(예: 행주대교) 위치를 찾아 해당 도로(예: 올림픽대로) 선형에 흡착(Snap)시킨 후,
    지정된 미터(m)만큼 떨어진 시작/종료 구간의 위도와 경도 좌표를 산출합니다.
    '''
    try:
        # [Step A] 기준 노드(행주대교) 찾기
        # ITS 데이터 버전에 따라 NODE_NAME 또는 NODE_NM 컬럼 사용
        node_col = 'NODE_NAME' if 'NODE_NAME' in config.NODE_GDF.columns else 'NODE_NM'
        node_match = config.NODE_GDF[config.NODE_GDF[node_col].str.contains(anchor_node, na=False)]
        
        if node_match.empty:
            return None, f"기준 지점 노드 [{anchor_node}]를 찾을 수 없습니다."
        anchor_geom = node_match.iloc[0].geometry # Point 객체

        # [Step B] 대상 도로(올림픽대로) 링크 가닥들을 찾아 하나로 병합
        link_col = 'ROAD_NAME' if 'ROAD_NAME' in config.LINK_GDF.columns else 'ROAD_NM'
        if link_col not in config.LINK_GDF.columns and 'RN' in config.LINK_GDF.columns:
            link_col = 'RN'
            
        road_links = config.LINK_GDF[config.LINK_GDF[link_col].str.contains(road_name, na=False)]
        
        if road_links.empty:
            return None, f"대상 도로 [{road_name}]를 찾을 수 없습니다."
        
        road_line = road_links.geometry.unary_union

        # 도로 선형(road_line)과 노드 점(anchor_geom) 사이의 최단거리 지점을 찾아 강제로 선 위에 안착시킵니다.
        geom_on_road, _ = nearest_points(road_line, anchor_geom)

        # [선 위의 기준점으로부터 투영(Project) 거리 계산 (단위: 미터)
        base_distance = road_line.project(geom_on_road)
        
        # 기준점 거리에서 공사 구간(예: 300m, 600m)만큼 더 전진한 위치 계산
        target_start_dist = base_distance + offset_start
        target_end_dist = base_distance + offset_end
        
        # 해당 거리 지점의 실제 미터(5179) 좌표 추출
        start_point_5179 = road_line.interpolate(target_start_dist)
        end_point_5179 = road_line.interpolate(target_end_dist)
        
        # 표출용 위경도(EPSG:4326) 좌표계로 최종 변환
        start_series = gpd.GeoSeries([start_point_5179], crs="EPSG:5179").to_crs(epsg=4326)
        end_series = gpd.GeoSeries([end_point_5179], crs="EPSG:5179").to_crs(epsg=4326)
        
        final_start_latlng = (start_series.iloc[0].y, start_series.iloc[0].x) # (위도, 경도)
        final_end_latlng = (end_series.iloc[0].y, end_series.iloc[0].x)     # (위도, 경도)
        
        return {
            "start": final_start_latlng,
            "end": final_end_latlng
        }, "성공"
        
    except Exception as e:
        return None, f"계산 중 에러 발생: {str(e)}"
    

@tool
async def resolve_between_nodes(road_name, start_node, end_node):
    '''
    시작점(start_node)와 끝점(end_node)가 주어진 경우 각 node 마다의 위도와 경도를 산출합니다.
    '''
    
    # 1. 시작 노드와 끝 노드의 위치를 각각 찾음
    s_node = config.NODE_GDF[config.NODE_GDF['NODE_NAME'].str.contains(start_node, na=False)]
    e_node = config.NODE_GDF[config.NODE_GDF['NODE_NAME'].str.contains(end_node, na=False)]
    
    if not s_node.empty and not e_node.empty:
        # 두 노드의 위경도를 변환하여 구간의 시작과 끝으로 반환
        s_series = gpd.GeoSeries([s_node.iloc[0].geometry], crs="EPSG:5179").to_crs(epsg=4326)
        e_series = gpd.GeoSeries([e_node.iloc[0].geometry], crs="EPSG:5179").to_crs(epsg=4326)
        return {"start": (s_series.iloc[0].y, s_series.iloc[0].x), "end": (e_series.iloc[0].y, e_series.iloc[0].x)}
    return None

@tool
async def resolve_address_point(address):
    '''
    지번 주소로 주어진 경우(예 : 가산동 535 또는 가산동 535-31) 위도와 경도를 산출합니다.
    지번 주소의 경우 완벽히 일치하지 않을 수 있으므로 '동' 이름으로 넓게 필터링 후 최접점 검색
    '''
    dong_name = address.split(" ")[0] # '가산동' 추출
    match_nodes = config.NODE_GDF[config.NODE_GDF['NODE_NAME'].str.contains(dong_name, na=False)]
    
    if not match_nodes.empty:
        # 매칭되는 동네 노드들의 기하학적 중심(Centroid)을 대표 좌표로 반환
        center = match_nodes.geometry.unary_union.centroid
        center_series = gpd.GeoSeries([center], crs="EPSG:5179").to_crs(epsg=4326)
        return {"lat": center_series.iloc[0].y, "lng": center_series.iloc[0].x}
    return None

mas01_node2_tools = [resolve_address_point, resolve_between_nodes, resolve_linear_reference]

# @tool
# async def get_seoul_roadname_latlng(roadname) :
#     """
#     입력 : 도로 이름 (예 : 올림픽대로)
#     출력 : 위도, 경도
#     """
    
#     url = f"https://apis.data.go.kr/B553774/RoadGPSInfo/getRoadGPSInfoQry?serviceKey={config.SEOUL_ROADNAME_API}&pageNo=1&numOfRows=10&proadlinename={roadname}"

#     try : 
#         response = requests.get(url)
        
#         response.raise_for_status()
        
#         data = response.json()
        
#         return data
        
#     except requests.exceptions.HTTPError as e:
#         print(f"HTTP 에러 발생: {e}")
#         return None
#     except Exception as e:
#         print(f"기타 에러 발생: {e}")
#         return None
    

# @tool
# async def kakao_address_to_latlng(address) :
#     """
#     입력 : 주소
#     출력 : x, y (위도, 경도)
#     """
    
#     url = "https://dapi.kakao.com/v2/local/search/address.json"
#     headers = {
#         "Authorization" : f"KakaoAK {config.KAKAO_RESTAPI}"
#     }
#     params = {
#         "query" : address
#     }
    
#     try : 
#         response = requests.get(url, headers=headers, params=params)
        
#         response.raise_for_status()
        
#         data = response.json()
        
#     except requests.exceptions.HTTPError as e:
#         print(f"HTTP 에러 발생: {e}")
#         return None
#     except Exception as e:
#         print(f"기타 에러 발생: {e}")
#         return None

# mas01_node2_tools = [kakao_address_to_latlng, get_seoul_roadname_latlng]


# async def publish_to_gu_channel(gu_name: str, enriched_data: dict):
#     """분석이 완료된 데이터를 서울시 구별 Pub/Sub 채널로 Broadcast"""
#     # 채널명 예시: incident:강남구
#     channel_key = f"incident:{gu_name}"
    
#     # 딕셔너리 데이터를 문자열(JSON)로 변환
#     payload_string = json.dumps(enriched_data, ensure_ascii=False)
    
#     # 채널에 가입(Subscribe)한 모든 리스너에게 동시에 데이터가 뿌려집니다.
#     await config.redis_client.publish(channel_key, payload_string)
#     print(f"📡 [Dispatcher] 채널 '{channel_key}'로 실시간 데이터 전파 완료!")