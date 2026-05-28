# apps/mas01_incident/agents.py
from typing import TypedDict, Annotated, Sequence, Dict, List, Any
import json

from openai import AsyncOpenAI
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, ToolMessage
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
import operator

import config
from config import logger
from apps.mas01_incident.tools import mas01_node2_tools

# 참고 https://taykim.tistory.com/35

class AgentState(TypedDict) :
    raw_incident_data : Dict[str, Any]
    extracted_entities : List[Dict[str, Any]]
    affected_nodes : Dict[str, Any]
    
    messages: Annotated[List[BaseMessage], operator.add]

# LangGraph Node 함수 정의
async def extract_affected_node(state:AgentState) -> Dict[str, Any] :
    """
    교통 공지사항 정보로부터 엔터티(영향 받는 도로, 버스 노선, 버스 정류소, 지하철 노선, 지하철 역)추출
    엔터티 별 경도와 위도를 추출합니다.
    입력 : incident data
    출력 : 영향 받는 도로별 정보 
    """
    raw_data = state['raw_incident_data']
    logger.info(f"[MAS01 Agent : extract_affected] inputs : {raw_data}")
    
    kanana_client = AsyncOpenAI(base_url=config.KANANA_MODEL_02_URL, api_key="fake-key")
    
    system_instruction = """당신은 대한민국 교통 및 지리 전문가입니다.
        사용자가 입력한 교통 공지사항 정보를 분석하여, 영향을 받는 도로, 교차로, 정류소, 지하철역을 '각각 하나의 독립된 항목'으로 분리해야 합니다. 
        여러 개의 장소를 하나의 객체에 묶어서 작성하지 마세요. (예: "A교차로, B교차로" -> 분리하여 각각 2개의 객체로 생성)

        [좌표 처리 핵심 규칙]
        1. 입력된 교통 공지사항 본문 텍스트 안에 '위도', '경도' 혹은 'lat', 'lng', '126.xxxx', '37.xxxx'와 같은 직접적인 좌표 숫자 데이터가 존재할 때만 그 값을 "lat", "lng"에 숫자로 추출하세요.
        2. 직접적인 좌표 숫자가 없다면 다른 숫자(시간, 버스 번호 등)를 절대 가공하지 말고 반드시 `null`로 입력해야 합니다. 임의로 좌표를 추론하거나 지어내지 마세요.

        [위치 유형(location_type) 분류 규칙]
        - "BETWEEN_NODES": 특정 도로 내에서 'A지점(교차로/교량 등)에서 B지점 사이' 구간을 통제하는 경우
        - "LINEAR_REFERENCE": 특정 기준점(나들목/교량/지하차도 등)을 지나 '몇 미터(m) 전방/후방 구간'을 통제하는 경우
        - "ADDRESS_POINT": 교차로나 구간 없이 특정 동 이름, 행정구역, 또는 건물 지번 주소(예: 가산동 535-31)만 명시된 경우
        - "GRAPH_DB" : 버스 정류소, 버스 노선, 지하철 노선, 지하철 역이 명시된 경우
        
        [DateTime 설정 규칙]
        - 종료일이 명시되어 있지 않다면, endDateTime을 무조건 '2099-12-31 23:59:59'로 설정

        반드시 순수 JSON 형식으로만 응답하세요.

        출력 JSON 구조:
        [
            {
                "affected" : "개별 통제/사고 도로명 또는 특정 교차로명, 정류소명 (반드시 딱 1개 장소만 작성)",
                "location_type" : "BETWEEN_NODES 또는 LINEAR_REFERENCE 또는 ADDRESS_POINT 중 하나로 분류",
                "details" : {
                    "road_name" : "해당하는 도로 이름 (예: 올림픽대로, 증산로). 없다면 null",
                    "start_node" : "BETWEEN_NODES 유형일 때 시작 지점 명칭 (예: 증산교). 없다면 null",
                    "end_node" : "BETWEEN_NODES 유형일 때 종료 지점 명칭 (예: 중동교). 없다면 null",
                    "anchor_node" : "LINEAR_REFERENCE 유형일 때 기준이 되는 랜드마크 명칭 (예: 금하지하차도, 행주대교). 없다면 null",
                    "offset_start" : LINEAR_REFERENCE 유형일 때 시작 거리(정수형, m 단위, 예: 250). 없다면 null,
                    "offset_end" : LINEAR_REFERENCE 유형일 때 종료 거리(정수형, m 단위, 예: 650). 없다면 null,
                    "address" : "ADDRESS_POINT 유형일 때 명시된 행정구역 및 지번 주소 (예: 가산동 535-31). 없다면 null"
                },
                "lat" : 본문에 직접 명시된 위도 값(float). 명시되어 있지 않다면 반드시 null,
                "lng" : 본문에 직접 명시된 경도 값(float). 명시되어 있지 않다면 반드시 null,
                "startDateTime" : "datetime 형식. 교통 통제/사고의 시작 시간. %Y-%m-%d %H:%M:%S 형태",
                "endDateTime" : "datetime 형식. 교통 통제/사고의 종료 시간. %Y-%m-%d %H:%M:%S 형태",
                "content" : "이 특정 장소와 관련된 통제 내용 요약"
            }
        ]
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
                "content" : f"교통 공지사항 정보 : {raw_data}"
            }
        ],
        max_tokens=3000,
        temperature=0.3,
    )

    result = json.loads(response.choices[0].message.content)
    
    return {"extracted_entities" : result}
        


mas01_workflow = StateGraph(AgentState)
mas01_workflow.add_node('extract_affected_node', extract_affected_node)

mas01_workflow.set_entry_point('extract_affected_node')

mas01_agent = mas01_workflow.compile()