# apps/mas01_incident/tools.py
import hashlib
import json
from datetime import datetime
import requests

import geopandas as gpd
from shapely.ops import nearest_points

from typing import Dict, Any, Tuple, Optional
from config import logger
import config

# 💡 국가 표준 시군구코드(5자리)를 시/구 명칭으로 디코딩하기 위한 매핑 딕셔너리
# 서울시 25개 구 예시 (프로젝트 범위에 따라 타 시도 코드를 확장해 나가시면 됩니다)
ADMIN_DISTRICT_MAP = {
    "11110": ("서울특별시", "종로구"), "11140": ("서울특별시", "중구"),
    "11170": ("서울특별시", "용산구"), "11200": ("서울특별시", "성동구"),
    "11215": ("서울특별시", "광진구"), "11230": ("서울특별시", "동대문구"),
    "11290": ("서울특별시", "성북구"), "11305": ("서울특별시", "강북구"),
    "11320": ("서울특별시", "도봉구"), "11350": ("서울특별시", "노원구"),
    "11380": ("서울특별시", "은평구"), "11410": ("서울특별시", "서대문구"),
    "11440": ("서울특별시", "마포구"), "11470": ("서울특별시", "양천구"),
    "11500": ("서울특별시", "강서구"), "11530": ("서울특별시", "구로구"),
    "11545": ("서울특별시", "금천구"), "11560": ("서울특별시", "영등포구"),
    "11590": ("서울특별시", "동작구"), "11620": ("서울특별시", "관악구"),
    "11650": ("서울특별시", "서초구"), "11680": ("서울특별시", "강남구"),
    "11710": ("서울특별시", "송파구"), "11740": ("서울특별시", "강동구")
}

def get_si_gu_from_row(row: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    SHP 행(Row) 속성에서 행정구역 코드를 찾아 (시, 구) 한글 명칭 튜플을 반환합니다.
    """
    # MOCT 데이터 버전에 따라 SGG_ID, SGG_CD, 이외의 명칭일 수 있으니 존재 여부 체크
    sgg_col = next((col for col in ['SGG_ID', 'SGG_CD'] if col in row.index), None)
    if sgg_col:
        code = str(row[sgg_col])
        if code in ADMIN_DISTRICT_MAP:
            return ADMIN_DISTRICT_MAP[code]
    return None, None

async def check_duplicate(incident_data: dict, mode: str) -> bool:
    try:
        info_text = None
        if mode == "redis" :
            info_text = str(incident_data.get("info", "")).strip()
        elif mode == "mysql" :
            info_text = incident_data['title'] + "\n" + incident_data['content']
        
        if not info_text:
            logger.warning("[Check Duplicate] info 내용이 없어 검증을 우회합니다.")
            return True
            
        hash_generator = hashlib.md5()
        hash_generator.update(info_text.encode("utf-8"))
        info_hash = hash_generator.hexdigest()
        
        dedup_key = f"incident:dedup:{info_hash}"
        
        ttl_seconds = None
        end_time_str = incident_data.get("endDateTime") if mode == "redis" else str(incident_data.get("end_datetime"))
        if end_time_str:
            try:
                end_dt = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                time_delta = (end_dt - datetime.strptime("2026-05-20 13:00:00", "%Y-%m-%d %H:%M:%S")).total_seconds()
                if time_delta <= 0:
                    return False
                
                ttl_seconds = max(int(time_delta), 0)
            except ValueError:
                pass
        
        is_new = await config.redis_client.set(
            name=dedup_key,
            value=info_text,
            ex=ttl_seconds,
            nx=True
        )
        logger.info(f"[MAS01 tools.py check_duplicate] redis에 dedup 생성")
        return bool(is_new)
    except Exception as e:
        logger.error(f"[Check Duplicate Error] 예외 발생: {e}")
        return False

def to_wgs84(geom) -> Tuple[float, float]:
    series = gpd.GeoSeries([geom], crs="EPSG:5179").to_crs(epsg=4326)
    return float(series.iloc[0].y), float(series.iloc[0].x)

async def resolve_linear_reference(road_name, anchor_node, offset_start, offset_end) -> Optional[Dict[str, Any]] :
    link_gdf = config.LINK_GDF
    node_gdf = config.NODE_GDF
    
    node_col = 'NODE_NAME' if 'NODE_NAME' in node_gdf.columns else 'NODE_NM'
    node_match = node_gdf[node_gdf[node_col].str.contains(anchor_node, na=False)]
    
    link_col = 'ROAD_NAME' if 'ROAD_NAME' in link_gdf.columns else 'ROAD_NM'
    if link_col not in link_gdf.columns and 'RN' in link_gdf.columns: link_col = 'RN'
    road_links = link_gdf[link_gdf[link_col].str.contains(road_name, na=False)]
    
    if not node_match.empty and not road_links.empty:
        # 💡 시/구 정보 추출
        si, gu = get_si_gu_from_row(node_match.iloc[0])
        
        road_line = road_links.geometry.unary_union
        geom_on_road, _ = nearest_points(road_line, node_match.iloc[0].geometry)
        
        base_dist = road_line.project(geom_on_road)
        mid_offset = (offset_start + offset_end) / 2
        target_point = road_line.interpolate(base_dist + mid_offset)
        
        lat, lng = to_wgs84(target_point)
        return {"lat": lat, "lng": lng, "si": si, "gu": gu}
    return None

def resolve_between_nodes(road_name: Optional[str], start_node: str, end_node: str) -> Optional[Dict[str, Any]]:
    node_gdf = config.NODE_GDF
    node_col = 'NODE_NAME' if 'NODE_NAME' in node_gdf.columns else 'NODE_NM'
    
    s_match = node_gdf[node_gdf[node_col].str.contains(start_node, na=False)]
    e_match = node_gdf[node_gdf[node_col].str.contains(end_node, na=False)]
    
    if not s_match.empty and not e_match.empty:
        # 💡 시작 노드를 기준으로 행정구역 정보 획득
        si, gu = get_si_gu_from_row(s_match.iloc[0])
        
        s_lat, s_lng = to_wgs84(s_match.iloc[0].geometry)
        e_lat, e_lng = to_wgs84(e_match.iloc[0].geometry)
        return {"lat": (s_lat + e_lat) / 2, "lng": (s_lng + e_lng) / 2, "si": si, "gu": gu}
    
    elif not s_match.empty:
        si, gu = get_si_gu_from_row(s_match.iloc[0])
        lat, lng = to_wgs84(s_match.iloc[0].geometry)
        return {"lat": lat, "lng": lng, "si": si, "gu": gu}
    return None

async def resolve_address_point(address:str) -> Optional[Dict[str, Any]] :
    node_gdf = config.NODE_GDF
    dong_name = address.split(" ")[0]
    match_nodes = node_gdf[node_gdf['NODE_NAME'].str.contains(dong_name, na=False)]
    
    if not match_nodes.empty:
        # 💡 발견된 첫 매칭 노드의 행정구역 활용
        si, gu = get_si_gu_from_row(match_nodes.iloc[0])
        
        center = match_nodes.geometry.unary_union.centroid
        lat, lng = to_wgs84(center)
        return {"lat": lat, "lng": lng, "si": si, "gu": gu}
    return None

async def publish_to_channel(gu_name: str, si_name: str, enriched_data: dict):
    """분석이 완료된 데이터를 서울시 구별 Pub/Sub 채널로 Broadcast"""
    final_si = "서울특별시" if "서울" in si_name else si_name
    final_gu = gu_name if gu_name else "미분류"
    if final_gu[-1] != '구' :
        final_gu += '구'
    stream_key = f"incident:stream:{final_si}:{final_gu}"
    
    details_data = enriched_data.get("details", {}) or {}
    
    flat_data = {
        "incident_id": str(enriched_data.get("incident_id", "")),
        "affected": str(enriched_data.get("affected", "")),
        "location_type": str(enriched_data.get("location_type", "")),
        "lat": str(enriched_data.get("lat", "")),
        "lng": str(enriched_data.get("lng", "")),
        "si": final_si,
        "gu": final_gu,
        "startDateTime": str(enriched_data.get("startDateTime", "")),
        "endDateTime": str(enriched_data.get("endDateTime", "")),
        "content": str(enriched_data.get("content", "")),
        
        # details 내부의 값들을 1차원으로 꺼내서 배치
        "road_name": str(details_data.get("road_name", "")),
        "start_node": str(details_data.get("start_node", "")),
        "end_node": str(details_data.get("end_node", "")),
        "anchor_node": str(details_data.get("anchor_node", "")),
        "offset_start": str(details_data.get("offset_start", "")),
        "offset_end": str(details_data.get("offset_end", "")),
        "address": str(details_data.get("address", ""))
    }
    
    logger.info(f"[MAS01 Tools.py] {stream_key} 스트림 발행 : {flat_data['affected']} {flat_data['content']}")
    await config.redis_client.xadd(name=stream_key, fields=flat_data)

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


