# apps/mas02_reroute/agents.py

from typing import TypedDict, List, Dict, Any

class ReroutingAgentState(TypedDict) :
    incident_id : str
    user_id : str
    
    live_user_xy : List[Dict[str, Any]]
    candidate_paths : List[Dict[str, Any]]
    
    final_rerouting_paths : List[Dict[str, Any]]
    

async def A(state:ReroutingAgentState) -> List[Dict[str, Any]] :
    pass