import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import nearest_points

# ==========================================
# 1. 공간 연산용 데이터 로드 (미터 단위 좌표계 EPSG:5179 필수)
# ==========================================
print("1. MOCT NODE/LINK 데이터셋 로드 중 (기반 좌표계: EPSG:5179)...")
try:
    # 거리를 미터(m) 단위로 정확히 계산하기 위해 5179 좌표계로 로드/변환합니다.
    link_gdf = gpd.read_file("/home/ubuntu/project-02-ai/[2026-01-13]NODELINKDATA/MOCT_LINK.shp", encoding="cp949").to_crs(epsg=5179)
    node_gdf = gpd.read_file("/home/ubuntu/project-02-ai/[2026-01-13]NODELINKDATA/MOCT_NODE.shp", encoding="cp949").to_crs(epsg=5179)
    print("   ✅ 데이터 로드 및 EPSG:5179 변환 완료!\n")
except Exception as e:
    print(f"❌ 파일 로드 실패: {e}")
    exit()


# ==========================================
# 2. 구간 좌표 계산 핵심 함수
# ==========================================
def calculate_section_coords(road_name: str, anchor_name: str, offset_start: float, offset_end: float):
    """
    기준 노드(예: 행주대교) 위치를 찾아 해당 도로(예: 올림픽대로) 선형에 흡착(Snap)시킨 후,
    지정된 미터(m)만큼 떨어진 시작/종료 구간의 위경도 좌표를 계산합니다.
    """
    try:
        # [Step A] 기준 노드(행주대교) 찾기
        # ITS 데이터 버전에 따라 NODE_NAME 또는 NODE_NM 컬럼 사용
        node_col = 'NODE_NAME' if 'NODE_NAME' in node_gdf.columns else 'NODE_NM'
        node_match = node_gdf[node_gdf[node_col].str.contains(anchor_name, na=False)]
        
        if node_match.empty:
            return None, f"기준 지점 노드 [{anchor_name}]를 찾을 수 없습니다."
        anchor_geom = node_match.iloc[0].geometry # Point 객체

        # [Step B] 대상 도로(올림픽대로) 링크 가닥들을 찾아 하나로 병합
        link_col = 'ROAD_NAME' if 'ROAD_NAME' in link_gdf.columns else 'ROAD_NM'
        if link_col not in link_gdf.columns and 'RN' in link_gdf.columns:
            link_col = 'RN'
            
        road_links = link_gdf[link_gdf[link_col].str.contains(road_name, na=False)]
        
        if road_links.empty:
            return None, f"대상 도로 [{road_name}]를 찾을 수 없습니다."
        
        # 여러 개로 쪼개진 도로 링크(선들)를 하나의 거대한 단일 선형(LineString/MultiLineString)으로 병합
        road_line = road_links.geometry.unary_union

        # [Step C] ⭐ nearest_points 활용 안전장치 (스냅 로직)
        # 도로 선형(road_line)과 노드 점(anchor_geom) 사이의 최단거리 지점을 찾아 강제로 선 위에 안착시킵니다.
        geom_on_road, _ = nearest_points(road_line, anchor_geom)

        # [Step D] 선 위의 기준점으로부터 투영(Project) 거리 계산 (단위: 미터)
        base_distance = road_line.project(geom_on_road)
        
        # [Step E] 기준점 거리에서 공사 구간(예: 300m, 600m)만큼 더 전진한 위치 계산
        target_start_dist = base_distance + offset_start
        target_end_dist = base_distance + offset_end
        
        # [Step F] 해당 거리 지점의 실제 미터(5179) 좌표 추출
        start_point_5179 = road_line.interpolate(target_start_dist)
        end_point_5179 = road_line.interpolate(target_end_dist)
        
        # [Step G] 표출용 위경도(EPSG:4326) 좌표계로 최종 변환
        start_series = gpd.GeoSeries([start_point_5179], crs="EPSG:5179").to_crs(epsg=4326)
        end_series = gpd.GeoSeries([end_point_5179], crs="EPSG:5179").to_crs(epsg=4326)
        
        final_start_latlng = (start_series.iloc[0].y, start_series.iloc[0].x) # (위도, 경도)
        final_end_latlng = (end_series.iloc[0].y, end_series.iloc[0].x)     # (위도, 경도)
        
        return {
            "start_latlng": final_start_latlng,
            "end_latlng": final_end_latlng
        }, "성공"
        
    except Exception as e:
        return None, f"계산 중 에러 발생: {str(e)}"


# ==========================================
# 3. 시뮬레이션 실행 (Agent output 기반 테스트)
# ==========================================
if __name__ == "__main__":
    print("2. 에이전트 추출 파라미터 기반 구간 좌표 연산 시작...")
    print("-" * 60)
    
    # 카나나(LLM)가 문맥에서 쪼개서 넘겨주었다고 가정한 딕셔너리 데이터
    mock_agent_output = {
        "road_name": "올림픽대로",
        "anchor_name": "행주대교",
        "offset_start": 300.0,
        "offset_end": 600.0
    }
    
    result, status = calculate_section_coords(
        road_name=mock_agent_output["road_name"],
        anchor_name=mock_agent_output["anchor_name"],
        offset_start=mock_agent_output["offset_start"],
        offset_end=mock_agent_output["offset_end"]
    )
    
    print("-" * 60)
    if result:
        print(f"✅ [구간 매핑 성공]")
        print(f"   📍 공사 시작 지점 (행주대교 +300m) 위경도: {result['start_latlng']}")
        print(f"   📍 공사 종료 지점 (행주대교 +600m) 위경도: {result['end_latlng']}")
    else:
        print(f"❌ [구간 매핑 실패] 원인: {status}")