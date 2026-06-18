# apps/mas01_incident/tools.py
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import asyncio # ◀ 스레드 풀 격리를 위해 추가

import geopandas as gpd
from shapely.ops import nearest_points

from typing import Dict, Any, Tuple, Optional
import config

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
    sgg_col = next((col for col in ['SGG_ID', 'SGG_CD'] if col in row.index), None)
    if sgg_col:
        code = str(row[sgg_col])
        if code in ADMIN_DISTRICT_MAP:
            return ADMIN_DISTRICT_MAP[code]
    return None, None

async def check_duplicate(incident_data: dict, mode: str) -> bool:
    try:
        # 안전 보장: 글로벌 레디스가 아직 준비 안 되었으면 무조건 통과시킴
        if not config.redis_client:
            return True
        
        info_text = None
        if mode == "redis" :
            info_text = str(incident_data.get("info", "")).strip()
        elif mode == "mysql" :
            info_text = incident_data['title'] + "\n" + incident_data['content']
        
        if not info_text:
            config.logger.warning("[Check Duplicate] info 내용이 없어 검증을 우회합니다.")
            return True
        
        hash_generator = hashlib.md5()
        hash_generator.update(info_text.encode("utf-8"))
        info_hash = hash_generator.hexdigest()
        
        dedup_key = f"incident:dedup:{info_hash}"
        
        ttl_seconds = None
        end_time_str = incident_data.get("endDateTime") if mode == "redis" else str(incident_data.get("end_datetime"))
        
        if end_time_str:
            try:
                end_dt = datetime.strptime(end_time_str.replace("T", " "), "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("Asia/Seoul"))
                now_dt = datetime.now(ZoneInfo("Asia/Seoul"))
                time_delta = (end_dt - now_dt).total_seconds()
                
                if time_delta <= 0:
                    return False
                ttl_seconds = max(int(time_delta), 600) # 최소 10분은 보장
            except Exception:
                ttl_seconds = 3600 # 파싱 에러 발생 시 기본 1시간 캐싱 기본 세팅
        
        is_new = await config.redis_client.set(
            name=dedup_key,
            value=info_text,
            ex=ttl_seconds,
            nx=True
        )
        config.logger.info(f"[MAS01 tools.py check_duplicate] 중복 체크 결과 완료 (신규 여부: {bool(is_new)})")
        return bool(is_new)
    except Exception as e:
        config.logger.error(f"[Check Duplicate Error] 예외 발생: {e}")
        return True # 에러가 나면 유실 방지를 위해 신규 데이터로 판정함

def to_wgs84(geom) -> Tuple[float, float]:
    try :
        series = gpd.GeoSeries([geom], crs="EPSG:5179").to_crs(epsg=4326)
        return float(series.iloc[0].y), float(series.iloc[0].x)
    except : 
        return None, None

async def resolve_linear_reference(road_name, anchor_node, offset_start, offset_end) -> Optional[Dict[str, Any]] :
    link_gdf = config.LINK_GDF
    node_gdf = config.NODE_GDF
    
    node_col = 'NODE_NAME' if 'NODE_NAME' in node_gdf.columns else 'NODE_NM'
    node_match = node_gdf[node_gdf[node_col].str.contains(anchor_node, na=False)]
    
    link_col = 'ROAD_NAME' if 'ROAD_NAME' in link_gdf.columns else 'ROAD_NM'
    if link_col not in link_gdf.columns and 'RN' in link_gdf.columns: link_col = 'RN'
    road_links = link_gdf[link_gdf[link_col].str.contains(road_name, na=False)]
    
    if not node_match.empty and not road_links.empty:
        si, gu = get_si_gu_from_row(node_match.iloc[0])
        road_line = road_links.geometry.unary_union
        geom_on_road, _ = nearest_points(road_line, node_match.iloc[0].geometry)
        
        base_dist = road_line.project(geom_on_road)
        mid_offset = (offset_start + offset_end) / 2
        target_point = road_line.interpolate(base_dist + mid_offset)
        
        lat, lng = to_wgs84(target_point)
        return {"lat": lat, "lng": lng, "si": si, "gu": gu}
    return None

async def resolve_between_nodes(road_name: Optional[str], start_node: str, end_node: str) -> Optional[Dict[str, Any]]:
    node_gdf = config.NODE_GDF
    for replace_word in ["일대", "부근", "인근"]:
        start_node = start_node.replace(replace_word, '')
        end_node = end_node.replace(replace_word, '')
    
    node_col = 'NODE_NAME' if 'NODE_NAME' in node_gdf.columns else 'NODE_NM'
    s_match = node_gdf[node_gdf[node_col].str.contains(start_node, na=False)]
    e_match = node_gdf[node_gdf[node_col].str.contains(end_node, na=False)]
    
    if not s_match.empty and not e_match.empty:
        si, gu = get_si_gu_from_row(s_match.iloc[0])
        s_lat, s_lng = to_wgs84(s_match.iloc[0].geometry)
        e_lat, e_lng = to_wgs84(e_match.iloc[0].geometry)
        return {"lat": (s_lat + e_lat) / 2, "lng": (s_lng + e_lng) / 2, "si": si, "gu": gu}
    
    elif not s_match.empty and e_match.empty:
        si, gu = get_si_gu_from_row(s_match.iloc[0])
        lat, lng = to_wgs84(s_match.iloc[0].geometry)
        return {"lat": lat, "lng": lng, "si": si, "gu": gu}
    
    if s_match.empty:
        si, gu, s_lat, s_lng = await kakao_keyword_to_latlng(start_node)
    else:
        si, gu = get_si_gu_from_row(s_match.iloc[0])
        s_lat, s_lng = to_wgs84(s_match.iloc[0].geometry)
        
    if e_match.empty:
        si, gu, e_lat, e_lng = await kakao_keyword_to_latlng(end_node)
    else:
        si, gu = get_si_gu_from_row(e_match.iloc[0])
        e_lat, e_lng = to_wgs84(e_match.iloc[0].geometry)
        
    if s_lat and e_lat:
        return {"lat": (float(s_lat) + float(e_lat)) / 2, "lng": (float(s_lng) + float(e_lng)) / 2, "si": si, "gu": gu}
    elif s_lat:
        return {"lat": float(s_lat), "lng": float(s_lng), "si": si, "gu": gu}
        
    return None

async def resolve_address_point(address:str) -> Optional[Dict[str, Any]] :
    node_gdf = config.NODE_GDF
    dong_name = address.split(" ")[0]
    match_nodes = node_gdf[node_gdf['NODE_NAME'].str.contains(dong_name, na=False)]
    
    if not match_nodes.empty:
        si, gu = get_si_gu_from_row(match_nodes.iloc[0])
        center = match_nodes.geometry.unary_union.centroid
        lat, lng = to_wgs84(center)
        return {"lat": lat, "lng": lng, "si": si, "gu": gu}
    else :
        try :
            res = await asyncio.get_running_loop().run_in_executor(None, kakao_address_to_latlng, address)
            if res:
                si, gu, y, x = res
                return {"lat" : y, "lng" : x, "si" : si, "gu" : gu}
            else :
                res = await asyncio.get_running_loop().run_in_executor(None, kakao_keyword_to_latlng, address)
                si, gu, y, x = res
                return {"lat" : y, "lng" : x, "si" : si, "gu" : gu}
        except Exception as e :
            return None
    return None

async def publish_to_channel(gu_name: str, si_name: str, enriched_data: dict):
    if not config.redis_client:
        return
        
    final_si = "서울특별시" if "서울" in str(si_name) else si_name
    final_gu = gu_name if gu_name else "미분류"
    if final_gu and final_gu[-1] != '구' :
        final_gu += '구'
    stream_key = f"incident:stream:{final_si}:{final_gu}"
    
    details_data = enriched_data.get("details", {}) or {}
    
    flat_data = {
        "incident_id": str(enriched_data.get("incident_id", "")),
        "affected": str(enriched_data.get("affected", "")),
        "location_type": str(enriched_data.get("location_type", "")),
        "lat": str(enriched_data.get("lat", "")),
        "lng": str(enriched_data.get("lng", "")),
        "si": str(final_si),
        "gu": str(final_gu),
        "startDateTime": str(enriched_data.get("startDateTime", "")),
        "endDateTime": str(enriched_data.get("endDateTime", "")),
        "content": str(enriched_data.get("content", "")),
        "road_name": str(details_data.get("road_name", "")),
        "start_node": str(details_data.get("start_node", "")),
        "end_node": str(details_data.get("end_node", "")),
        "anchor_node": str(details_data.get("anchor_node", "")),
        "offset_start": str(details_data.get("offset_start", "")),
        "offset_end": str(details_data.get("offset_end", "")),
        "address": str(details_data.get("address", ""))
    }
    
    config.logger.info(f"[MAS01 Tools.py] {stream_key} 스트림 발행 완료 : {flat_data['affected']}")
    await config.redis_client.xadd(name=stream_key, fields=flat_data)

def _execute_kakao_keyword(keyword: str):
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    headers = {"Authorization" : f"KakaoAK {config.KAKAO_RESTAPI}"}
    params = {"query" : keyword}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=2.0)
        response.raise_for_status()
        return response.json()
    except Exception:
        return None

async def kakao_keyword_to_latlng(keyword: str):
    try:
        data = await asyncio.get_running_loop().run_in_executor(None, _execute_kakao_keyword, keyword)
        if not data:
            return ("서울특별시", "미분류", None, None)
            
        output = None
        for doro in data.get('documents', []):
            if '교통,수송' in doro.get('category_name', ''):
                output = doro
                break
            elif doro.get('place_name', '').replace(' ', '') == keyword.replace(' ', ''):
                output = doro
                break
        if not output and data.get('documents'):
            output = data['documents'][0]
            
        if output:
            address = output['address_name'].split(' ')
            return (address[0], address[1], output['y'], output['x'])
    except Exception:
        pass
    return ("서울특별시", "미분류", None, None)

def _execute_kakao_address(keyword: str):
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization" : f"KakaoAK {config.KAKAO_RESTAPI}"}
    params = {"query" : keyword}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=2.0)
        response.raise_for_status()
        return response.json()
    except Exception:
        return None

async def kakao_address_to_latlng(keyword: str):
    try:
        data = await asyncio.get_running_loop().run_in_executor(None, _execute_kakao_address, keyword)
        if data and data.get('documents'):
            output = data['documents'][0]
            address = output['address_name'].split(' ')
            return (address[0], address[1], output['y'], output['x'])
    except Exception:
        pass
    return ("서울특별시", "미분류", None, None)