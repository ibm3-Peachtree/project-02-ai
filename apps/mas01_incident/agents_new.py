# apps/mas01_incident/agents.py
from typing import TypedDict, Annotated, Sequence, Dict, List, Any
import json
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

from openai import AsyncOpenAI
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, ToolMessage
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
import operator

import config
from apps.mas01_incident.tools import resolve_address_point, resolve_between_nodes, resolve_linear_reference, publish_to_channel

class AgentState(TypedDict) :
    raw_incident_data : Dict[str, Any]
    ner_entities : List[Dict[str, Any]] # 통제, 우회, entity
    preprocessed_entities : List[Dict[str, Any]] # 전처리
    classified_entities : List[Dict[str, Any]] # location_type classfication
    temp_outputs : List[Dict[str, Any]]
    final_outputs : List[Dict[str, Any]]
    
async def classify_entity(state : AgentState) -> List[Dict[str, Any]] :
    """
    엔터티 추출 및 각 엔터티 별 통제 도로/정류소/역, 우회 도로/정류소/역 분류
    """
    raw_data = state['raw_incident_data']
    config.logger.info(f"[MAS01 Agent : extract_affected] inputs : {raw_data}")
    
    raw_lat = raw_data.get("lat")
    raw_lng = raw_data.get("lng")
    
    if raw_lat and raw_lng:
        try:
            val_lat = float(raw_lat)
            val_lng = float(raw_lng)
            
            # 100이 넘는 값(124~132)이 lat(위도)에 들어와 있다면 명백한 오류이므로 자리를 바꿉니다.
            if val_lat > 100.0 and val_lng < 50.0:
                config.logger.warning(f"🔄 [Redis 축 전도 감지] lat과 lng가 뒤바뀌어 들어왔습니다. 강제 교정합니다. (입력 lat: {val_lat}, lng: {val_lng})")
                raw_data["lat"] = val_lng  # 37.52... 을 위도로
                raw_data["lng"] = val_lat  # 127.05... 을 경도로
            else:
                # 데이터가 정상적으로 들어왔을 때의 포맷팅
                raw_data["lat"] = val_lat
                raw_data["lng"] = val_lng
        except ValueError:
            pass # 숫자가 아닐 경우의 예외 방어
            
    config.logger.info(f"[MAS01 Agent : extract_affected] 보정 완료된 레디스 데이터 : {raw_data}")
    
    kanana_client = AsyncOpenAI(base_url=config.KANANA_MODEL_02_URL, api_key="fake-key")
    system_instruction = """"""
    
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
        temperature=0.1, # 답변의 일관성을 위해 0.2~0.3 유지 권장
    )
    
