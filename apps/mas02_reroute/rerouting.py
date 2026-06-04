# apps/mas02_reroute/rerouting.py

import os
import json
import time
import datetime
from collections import defaultdict
from dotenv import load_dotenv

# 프로젝트 전역 DB 클라이언트를 가져옵니다.
import config
from config import logger

load_dotenv()

class TransportApp:
    def __init__(self):
        # 자체 driver 생성 코드를 제거하고, config에 개통된 글로벌 neo4j_client를 바인딩합니다.
        self.driver = config.neo4j_client
        
    def delete_gds_graph(self):
        self.delete_gds_graph1()
        self.delete_gds_graph2()
        self.delete_gds_graph3()
        
    def delete_gds_graph1(self):
        logger.info("기존 가상 메모리 그래프(network_best) 삭제 요청...")
        with self.driver.session(database="neo4j") as session:
            drop_query = "CALL gds.graph.drop('network_best', false) YIELD graphName;"
            try:
                session.run(drop_query)
                time.sleep(0.5) 
            except Exception:
                pass
            
    def delete_gds_graph2(self):
        logger.info("기존 가상 메모리 그래프(network_subway_best) 삭제 요청...")
        with self.driver.session(database="neo4j") as session:
            drop_query = "CALL gds.graph.drop('network_subway_best', false) YIELD graphName;"
            try:
                session.run(drop_query)
                time.sleep(0.5) 
            except Exception:
                pass
    
    def delete_gds_graph3(self):
        logger.info("기존 가상 메모리 그래프(network_bus_only) 삭제 요청...")
        with self.driver.session(database="neo4j") as session:
            drop_query = "CALL gds.graph.drop('network_bus_only', false) YIELD graphName;"
            try:
                session.run(drop_query)
                time.sleep(0.3) 
            except Exception:
                pass
            
    def build_gds_graph1(self):
        logger.info("Best GDS Graph [수단 탑승 최우선 및 도보 차선책 계층화형] 가상 프로젝션 빌드 시작...")
        with self.driver.session(database="neo4j") as session:
            project_query = """
            CALL gds.graph.project.cypher(
                'network_best',
                'MATCH (s:Station) RETURN id(s) AS id',
                
                'MATCH (s1:Station)-[r:NEXT_STOP|ALIGHT]->(s2:Station)
                RETURN id(s1) AS source, id(s2) AS target, type(r) AS type, 
                        (toFloat(coalesce(r.duration_normal, 120)) / 60.0) AS weight
                
                UNION ALL
                
                MATCH (s1:Station)-[b:BOARD]->(s2:Station)
                RETURN id(s1) AS source, id(s2) AS target, "BOARD" AS type, 
                        (toFloat(coalesce(b.duration_normal, 180)) / 60.0) + 2.0 AS weight
                
                UNION ALL
                
                // [도보 가중치 패널티 스케일링 현실화]
                MATCH (s1:Station)-[tf:TRANSFER]->(s2:Station)
                WHERE tf.type = "TRANSFER"
                WITH s1, s2, tf, (toFloat(coalesce(tf.duration_normal, 0))) AS dur_sec
                RETURN id(s1) AS source, id(s2) AS target, "TRANSFER" AS type, 
                        CASE 
                        // 도보 거리가 너무 길어질 경우 (3분 초과) 패널티를 크게 주어 장거리 도보 차단
                        WHEN dur_sec > 180 THEN 
                            15.0 + ((180 / 60.0) * 1.5) + (((dur_sec - 180) / 60.0) * 10.0)
                        ELSE 
                            5.0 + (dur_sec / 60.0) * 1.2
                        END AS weight'
            )
            """
            try:
                project_result = session.run(project_query)
                record = project_result.single()
                if record:
                    logger.info("[성공] 수단 탑승 우선순위 계층화 가상 그래프 빌드 완료!")
            except Exception as e:
                logger.error(f"\n❌ 가상 그래프 빌드 함수 내부 에러: {e}\n")
                raise e
    
    def build_gds_graph2(self):
        logger.info("Subway GDS Graph [데이터 속성 맞춤 지하철 최우선형] 가상 프로젝션 빌드 시작...")
        with self.driver.session(database="neo4j") as session:
            try:
                session.run("CALL gds.graph.drop('network_subway_best', false)")
                time.sleep(0.2)
            except Exception:
                pass

            project_query = """
            CALL gds.graph.project.cypher(
                'network_subway_best',
                'MATCH (s:Station) RETURN id(s) AS id',
                
                'MATCH (s1:Station)-[r:NEXT_STOP|ALIGHT]->(s2:Station)
                WITH s1, s2, r, (toFloat(coalesce(r.duration_normal, 120)) / 60.0) AS base_weight
                RETURN id(s1) AS source, id(s2) AS target, type(r) AS type,
                       CASE 
                         WHEN coalesce(r.type, "") = "SUBWAY" OR coalesce(s1.type, "") = "SUBWAY" OR coalesce(s2.type, "") = "SUBWAY" THEN base_weight * 0.25
                         ELSE base_weight 
                       END AS weight
                
                UNION ALL
                
                MATCH (s1:Station)-[b:BOARD]->(s2:Station)
                WITH s1, s2, b, (toFloat(coalesce(b.duration_normal, 180)) / 60.0) AS board_cost
                RETURN id(s1) AS source, id(s2) AS target, "BOARD" AS type,
                       CASE 
                         WHEN coalesce(s2.type, "") = "SUBWAY" THEN board_cost * 0.1
                         ELSE board_cost + 12.0
                       END AS weight
                
                UNION ALL
                
                MATCH (s1:Station)-[tf:TRANSFER]->(s2:Station)
                WHERE coalesce(tf.type, "") = "TRANSFER"
                WITH s1, s2, tf, (toFloat(coalesce(tf.duration_normal, 0))) AS dur_sec
                RETURN id(s1) AS source, id(s2) AS target, "TRANSFER" AS type, 
                       CASE 
                         WHEN dur_sec > 180 THEN 
                             35.0 + (((dur_sec - 180) / 60.0) * 15.0)
                         ELSE 
                             2.0 + (dur_sec / 60.0) * 1.1
                       END AS weight'
            )
            """
            try:
                project_result = session.run(project_query)
                record = project_result.single()
                if record and record["graphName"]:
                    logger.info(f"[성공] 가상 그래프 '{record['graphName']}' 메모리 프로젝션 빌드 성공!")
            except Exception as e:
                logger.error(f"\n[치명적 오류] 가상 그래프 빌드 함수 내부에서 프로젝션 실패: {e}\n")
                raise e
    
    def build_gds_graph3(self):
        logger.info("Bus-Only GDS Graph [OOM 방어형 초경량 레이어 격리] 빌드 시작...")
        with self.driver.session(database="neo4j") as session:
            try:
                session.run("CALL gds.graph.drop('network_bus_only', false)")
                time.sleep(0.2)
            except Exception:
                pass

            # [OOM 박멸 핵심]: WHERE 조건의 문자열 연산을 전면 제거하고 
            # 라벨과 관계선 종류 자체를 분리하여 오직 'BUS' 관련 컴포넌트만 메모리에 다이렉트로 올립니다.
            project_query = """
            CALL gds.graph.project.cypher(
                'network_bus_only',
                'MATCH (s:Station) 
                 WHERE NOT s.node_id STARTS WITH "SUBWAY" AND NOT s.node_id CONTAINS "_호선"
                 RETURN id(s) AS id',
                
                'MATCH (s1:Station)-[r:NEXT_STOP|ALIGHT]->(s2:Station)
                 WHERE NOT s1.node_id STARTS WITH "SUBWAY" AND NOT s2.node_id STARTS WITH "SUBWAY"
                 RETURN id(s1) AS source, id(s2) AS target, type(r) AS type, (toFloat(coalesce(r.duration_normal, 120)) / 60.0) AS weight
                
                 UNION ALL
                
                 MATCH (s1:Station)-[b:BOARD]->(s2:Station)
                 WHERE NOT s1.node_id STARTS WITH "SUBWAY" AND NOT s2.node_id STARTS WITH "SUBWAY"
                 RETURN id(s1) AS source, id(s2) AS target, "BOARD" AS type, (toFloat(coalesce(b.duration_normal, 180)) / 60.0) + 2.0 AS weight
                
                 UNION ALL
                
                 MATCH (s1:Station)-[tf:TRANSFER]->(s2:Station)
                 WHERE (NOT s1.node_id STARTS WITH "SUBWAY") AND (NOT s2.node_id STARTS WITH "SUBWAY") 
                   AND (coalesce(tf.sub_type, "") <> "SUBWAY_TRANSFER")
                 RETURN id(s1) AS source, id(s2) AS target, "TRANSFER" AS type, (toFloat(coalesce(tf.duration_normal, 0)) / 60.0) AS weight'
            )
            """
            try:
                project_result = session.run(project_query)
                record = project_result.single()
                if record and record["graphName"]:
                    logger.info(f"[성공] 가상 그래프 '{record['graphName']}' OOM 우회 빌드 성공!")
            except Exception as e:
                logger.error(f"\n[빌드 실패] 메모리 에러: {e}\n")
                raise e
            
    def get_optimal_path1(self, start_id, end_id, current_time=None, blocked_ids=[]):
        if not current_time:
            now = datetime.datetime.now()
            hour = now.hour
            minute = now.minute
            if 0 <= hour <= 3: hour += 24
            current_time = f"{str(hour).zfill(2)}:{str(minute).zfill(2)}"
        if not blocked_ids: blocked_ids = []
        
        # 버스와 지하철의 ID 체계 차이로 인한 레코드 증발 버그를 완벽히 튜닝한 최종 쿼리
        cypher_query = """
        MATCH (start:Station {node_id: $start_id, is_master: true})  
        MATCH (end:Station {node_id: $end_id, is_master: true})    
        WITH start, end, end AS end_gateway
        WITH start, end, collect(DISTINCT end_gateway) AS targetNodes
        CALL gds.shortestPath.dijkstra.stream('network_best', {
            sourceNode: start, targetNodes: targetNodes, relationshipWeightProperty: 'weight'
        })
        YIELD index, nodeIds, totalCost
        
        WITH index AS raw_idx, [nodeId IN nodeIds | gds.util.asNode(nodeId)] AS finalNodes, totalCost
        WHERE finalNodes[1].is_master = false

        WITH finalNodes, totalCost, [i IN range(0, size(finalNodes)-2) | finalNodes[i].node_id + "->" + finalNodes[i+1].node_id] AS pathLinks
        WITH collect({nodes: finalNodes, links: pathLinks, cost: totalCost}) AS raw_all_paths
        WITH reduce(acc = {accepted: [], all_links: []}, p IN raw_all_paths |
            CASE 
              WHEN size(acc.accepted) = 0 THEN {accepted: acc.accepted + p, all_links: acc.all_links + p.links}
              WHEN toFloat(size([lk IN p.links WHERE lk IN acc.all_links])) / size(p.links) <= 0.5 THEN {accepted: acc.accepted + p, all_links: acc.all_links + p.links}
              ELSE acc 
            END
        ) AS filter_result

        UNWIND range(0, size(filter_result.accepted)-1) AS path_idx
        WITH path_idx, filter_result.accepted[path_idx].nodes AS finalNodes, filter_result.accepted[path_idx].cost AS totalCost

        UNWIND range(0, size(finalNodes)-2) AS idx
        WITH path_idx, totalCost, idx, finalNodes[idx] AS fs1, finalNodes[idx+1] AS fs2

        OPTIONAL MATCH (fs1)-[ns:NEXT_STOP]->(fs2)
        OPTIONAL MATCH (r_node:Route {route_id: ns.route_id})
        WHERE coalesce(r_node.starttime, '00:00') <= $current_time 
          AND $current_time <= coalesce(r_node.endtime, '26:00')

        OPTIONAL MATCH (fs1)-[b:BOARD]->(fs2)
        OPTIONAL MATCH (p_r:Route {route_id: fs2.route_id}) 
        OPTIONAL MATCH (fs1)-[a:ALIGHT]->(fs2)
        OPTIONAL MATCH (fs1)-[tf_rel:TRANSFER]->(fs2)

        WITH path_idx, totalCost, idx, fs1, fs2, ns, r_node, b, p_r, a, tf_rel,
             CASE 
               WHEN ns IS NOT NULL THEN 'NEXT_STOP'
               WHEN b  IS NOT NULL THEN 'BOARD'
               WHEN a  IS NOT NULL THEN 'ALIGHT'
               WHEN tf_rel IS NOT NULL THEN 'TRANSFER'
               ELSE null 
             END AS rel_type
             
        CALL {
            WITH rel_type, fs1, fs2, r_node, ns
            WITH rel_type, fs1, fs2, r_node, ns,
                 CASE WHEN rel_type = 'NEXT_STOP' AND NOT fs1.node_id STARTS WITH 'SUBWAY' AND fs1.node_id CONTAINS '_' THEN split(fs1.node_id, '_')[0] ELSE null END AS m1_id,
                 CASE WHEN rel_type = 'NEXT_STOP' AND NOT fs2.node_id STARTS WITH 'SUBWAY' AND fs2.node_id CONTAINS '_' THEN split(fs2.node_id, '_')[0] ELSE null END AS m2_id
            
            OPTIONAL MATCH (m1:Station {node_id: m1_id, is_master: true})
            OPTIONAL MATCH (m2:Station {node_id: m2_id, is_master: true})
            OPTIONAL MATCH (m1)-[:BOARD]->(p1:Station)-[all_ns:NEXT_STOP]->(p2:Station)-[:ALIGHT]->(m2)
            OPTIONAL MATCH (all_r:Route {route_id: all_ns.route_id})
            
            WITH rel_type, fs1, fs2, r_node, ns, collect(DISTINCT coalesce(all_r.num, all_r.name)) AS all_sharing_routes
            WITH rel_type, fs1, fs2, r_node, ns, all_sharing_routes,
                 coalesce(r_node.num, r_node.name, CASE WHEN ns.route_id CONTAINS '_' THEN split(ns.route_id, '_')[1] ELSE ns.route_id END) AS backup_route_name
                 
            WITH rel_type, fs1, fs2, backup_route_name,
                 CASE 
                   WHEN rel_type = 'NEXT_STOP' THEN 
                     CASE 
                       WHEN size(all_sharing_routes) > 0 THEN apoc.coll.toSet(all_sharing_routes + backup_route_name)
                       ELSE [backup_route_name]
                     END
                   ELSE [] 
                 END AS final_routes
            RETURN final_routes
        }

        WITH path_idx, totalCost, idx, {
          idx: idx, from_id: fs1.node_id, from_name: fs1.name, to_id: fs2.node_id, to_name: fs2.name, rel_type: rel_type,
          route_name: CASE 
                        WHEN rel_type = 'TRANSFER' THEN '도보' 
                        WHEN rel_type = 'BOARD' THEN coalesce(p_r.num, p_r.name) + ' 대기'
                        WHEN rel_type = 'ALIGHT' THEN '하차'
                        ELSE apoc.text.join(final_routes, ',')
                      END,
          distance: coalesce(coalesce(ns, b, a, tf_rel).distance, 0),
          duration_sec: coalesce(coalesce(ns, b, a, tf_rel).duration_normal, 0)
        } AS record
        ORDER BY totalCost ASC, idx ASC

        RETURN path_idx, totalCost, collect(record) AS path_records
        ORDER BY totalCost ASC
        """
        try:
            with self.driver.session(database="neo4j") as session:
                result = session.run(cypher_query, start_id=start_id, end_id=end_id, current_time=current_time)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Neo4j 쿼리 1 실행 에러: {e}")
            return []
        
    def get_optimal_path2(self, start_id, end_id, current_time=None, blocked_ids=[]):
        cypher_query = """
        MATCH (start:Station {node_id: $start_id, is_master: true})  
        MATCH (end:Station {node_id: $end_id, is_master: true})    
        WITH start, end
        CALL gds.shortestPath.yens.stream('network_subway_best', {
            sourceNode: id(start), targetNode: id(end), k: 15, relationshipWeightProperty: 'weight'
        })
        YIELD index, nodeIds, totalCost

        WITH index AS raw_idx, [nodeId IN nodeIds | gds.util.asNode(nodeId)] AS finalNodes, totalCost
        WHERE NONE(node IN finalNodes WHERE node.node_id IN $blocked_ids)
        WITH finalNodes, totalCost

        WITH finalNodes, totalCost, [i IN range(0, size(finalNodes)-2) | finalNodes[i].node_id + "->" + finalNodes[i+1].node_id] AS pathLinks
        WITH collect({nodes: finalNodes, links: pathLinks, cost: totalCost}) AS raw_all_paths
        
        WITH reduce(acc = {accepted: [], all_links: []}, p IN raw_all_paths |
            CASE 
              WHEN size(acc.accepted) < 3 THEN {accepted: acc.accepted + p, all_links: acc.all_links + p.links}
              WHEN toFloat(size([lk IN p.links WHERE lk IN acc.all_links])) / size(p.links) <= 0.85 THEN {accepted: acc.accepted + p, all_links: acc.all_links + p.links}
              ELSE acc 
            END
        ) AS filter_result

        UNWIND range(0, size(filter_result.accepted)-1) AS path_idx
        WITH path_idx, filter_result.accepted[path_idx].nodes AS finalNodes, filter_result.accepted[path_idx].cost AS totalCost

        UNWIND range(0, size(finalNodes)-2) AS idx
        WITH path_idx, totalCost, idx, finalNodes[idx] AS fs1, finalNodes[idx+1] AS fs2

        OPTIONAL MATCH (fs1)-[ns:NEXT_STOP]->(fs2)
        OPTIONAL MATCH (r_node:Route {route_id: ns.route_id})
        OPTIONAL MATCH (fs1)-[b:BOARD]->(fs2)
        OPTIONAL MATCH (p_r:Route {route_id: fs2.route_id}) 
        OPTIONAL MATCH (fs1)-[a:ALIGHT]->(fs2)
        OPTIONAL MATCH (fs1)-[tf_rel:TRANSFER]->(fs2)

        WITH path_idx, totalCost, idx, fs1, fs2, ns, r_node, b, p_r, a, tf_rel,
             CASE 
               WHEN ns IS NOT NULL THEN 'NEXT_STOP'
               WHEN b  IS NOT NULL THEN 'BOARD'
               WHEN a  IS NOT NULL THEN 'ALIGHT'
               WHEN tf_rel IS NOT NULL THEN 'TRANSFER'
               ELSE null 
             END AS rel_type
             
        CALL {
            WITH rel_type, fs1, fs2, r_node, ns
            WITH rel_type, fs1, fs2, r_node, ns,
                 CASE WHEN rel_type = 'NEXT_STOP' AND coalesce(fs1.type, '') <> "SUBWAY" AND fs1.node_id CONTAINS '_' THEN split(fs1.node_id, '_')[0] ELSE null END AS m1_id,
                 CASE WHEN rel_type = 'NEXT_STOP' AND coalesce(fs2.type, '') <> "SUBWAY" AND fs2.node_id CONTAINS '_' THEN split(fs2.node_id, '_')[0] ELSE null END AS m2_id
            
            OPTIONAL MATCH (m1:Station {node_id: m1_id, is_master: true})
            OPTIONAL MATCH (m2:Station {node_id: m2_id, is_master: true})
            OPTIONAL MATCH (m1)-[:BOARD]->(p1:Station)-[all_ns:NEXT_STOP]->(p2:Station)-[:ALIGHT]->(m2)
            OPTIONAL MATCH (all_r:Route {route_id: all_ns.route_id})
            
            WITH rel_type, fs1, fs2, r_node, ns, collect(DISTINCT coalesce(all_r.num, all_r.name)) AS all_sharing_routes
            WITH rel_type, fs1, fs2, r_node, ns, all_sharing_routes,
                 coalesce(r_node.num, r_node.name, CASE WHEN ns.route_id CONTAINS '_' THEN split(ns.route_id, '_')[1] ELSE ns.route_id END) AS backup_route_name
                 
            WITH rel_type, fs1, fs2, backup_route_name,
                 CASE 
                   WHEN rel_type = 'NEXT_STOP' THEN 
                     CASE 
                       WHEN size(all_sharing_routes) > 0 THEN apoc.coll.toSet(all_sharing_routes + backup_route_name)
                       ELSE [backup_route_name]
                     END
                   ELSE [] 
                 END AS final_routes
            RETURN final_routes
        }

        WITH path_idx, totalCost, idx, fs1, fs2, rel_type, ns, r_node, b, p_r, a, tf_rel, final_routes
        WHERE rel_type IS NOT NULL

        WITH path_idx, totalCost, idx, {
          idx: idx, from_id: fs1.node_id, from_name: fs1.name, to_id: fs2.node_id, to_name: fs2.name, rel_type: rel_type,
          route_name: CASE 
                        WHEN rel_type = 'TRANSFER' THEN '도보' 
                        WHEN rel_type = 'BOARD' THEN coalesce(p_r.num, p_r.name) + ' 대기'
                        WHEN rel_type = 'ALIGHT' THEN '하차'
                        ELSE apoc.text.join(final_routes, ',')
                      END,
          distance: coalesce(coalesce(ns, b, a, tf_rel).distance, 0),
          duration_sec: coalesce(coalesce(ns, b, a, tf_rel).duration_normal, 0)
        } AS record
        ORDER BY totalCost ASC, idx ASC

        RETURN path_idx, totalCost, collect(record) AS path_records
        ORDER BY totalCost ASC
        """
        try:
            with self.driver.session(database="neo4j") as session:
                result = session.run(cypher_query, start_id=start_id, end_id=end_id, blocked_ids=blocked_ids)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Neo4j 쿼리 2 실행 에러: {e}")
            return []
        
    def get_optimal_path3(self, start_id, end_id, current_time=None, blocked_ids=[]):
        if not current_time:
            now = datetime.datetime.now()
            hour = now.hour
            minute = now.minute
            if 0 <= hour <= 3: hour += 24
            current_time = f"{str(hour).zfill(2)}:{str(minute).zfill(2)}"
        if not blocked_ids: blocked_ids = []

        cypher_query = """
        MATCH (start:Station {node_id: $start_id, is_master: true})  
        MATCH (end:Station {node_id: $end_id, is_master: true})    
        WITH id(start) AS srcId, id(end) AS tgtId
        
        CALL gds.shortestPath.yens.stream('network_bus_only', {
            sourceNode: srcId, targetNode: tgtId, k: 15, relationshipWeightProperty: 'weight'
        })
        YIELD index, nodeIds, totalCost

        WITH index AS raw_idx, [nodeId IN nodeIds | gds.util.asNode(nodeId)] AS finalNodes, totalCost
        WHERE NONE(node IN finalNodes WHERE node.node_id IN $blocked_ids)
        WITH finalNodes, totalCost

        WITH finalNodes, totalCost, [i IN range(0, size(finalNodes)-2) | finalNodes[i].node_id + "->" + finalNodes[i+1].node_id] AS pathLinks
        WITH collect({nodes: finalNodes, links: pathLinks, cost: totalCost}) AS raw_all_paths
        
        WITH reduce(acc = {accepted: [], all_links: []}, p IN raw_all_paths |
            CASE 
              WHEN size(acc.accepted) = 0 THEN {accepted: acc.accepted + p, all_links: acc.all_links + p.links}
              WHEN toFloat(size([lk IN p.links WHERE lk IN acc.all_links])) / size(p.links) <= 0.80 THEN {accepted: acc.accepted + p, all_links: acc.all_links + p.links}
              ELSE acc 
            END
        ) AS filter_result

        UNWIND range(0, size(filter_result.accepted)-1) AS path_idx
        WITH path_idx, filter_result.accepted[path_idx].nodes AS finalNodes, filter_result.accepted[path_idx].cost AS totalCost

        UNWIND range(0, size(finalNodes)-2) AS idx
        WITH path_idx, totalCost, idx, finalNodes[idx] AS fs1, finalNodes[idx+1] AS fs2

        OPTIONAL MATCH (fs1)-[ns:NEXT_STOP]->(fs2)
        OPTIONAL MATCH (r_node:Route {route_id: ns.route_id})
        OPTIONAL MATCH (fs1)-[b:BOARD]->(fs2)
        OPTIONAL MATCH (p_r:Route {route_id: fs2.route_id}) 
        OPTIONAL MATCH (fs1)-[a:ALIGHT]->(fs2)
        OPTIONAL MATCH (fs1)-[tf_rel:TRANSFER]->(fs2)

        WITH path_idx, totalCost, idx, fs1, fs2, ns, r_node, b, p_r, a, tf_rel,
             CASE 
               WHEN ns IS NOT NULL THEN 'NEXT_STOP'
               WHEN b  IS NOT NULL THEN 'BOARD'
               WHEN a  IS NOT NULL THEN 'ALIGHT'
               WHEN tf_rel IS NOT NULL THEN 'TRANSFER'
               ELSE null 
             END AS rel_type
             
        CALL {
            WITH rel_type, fs1, fs2, r_node, ns
            WITH rel_type, fs1, fs2, r_node, ns,
                 CASE WHEN rel_type = 'NEXT_STOP' AND fs1.node_id CONTAINS '_' THEN split(fs1.node_id, '_')[0] ELSE null END AS m1_id,
                 CASE WHEN rel_type = 'NEXT_STOP' AND fs2.node_id CONTAINS '_' THEN split(fs2.node_id, '_')[0] ELSE null END AS m2_id
            
            OPTIONAL MATCH (m1:Station {node_id: m1_id, is_master: true})
            OPTIONAL MATCH (m2:Station {node_id: m2_id, is_master: true})
            OPTIONAL MATCH (m1)-[:BOARD]->(p1:Station)-[all_ns:NEXT_STOP]->(p2:Station)-[:ALIGHT]->(m2)
            OPTIONAL MATCH (all_r:Route {route_id: all_ns.route_id})
            
            WITH rel_type, fs1, fs2, r_node, ns, collect(DISTINCT coalesce(all_r.num, all_r.name)) AS all_sharing_routes
            WITH rel_type, fs1, fs2, r_node, ns, all_sharing_routes,
                 coalesce(r_node.num, r_node.name, CASE WHEN ns.route_id CONTAINS '_' THEN split(ns.route_id, '_')[1] ELSE ns.route_id END) AS backup_route_name
                 
            WITH rel_type, fs1, fs2, backup_route_name,
                 CASE 
                   WHEN rel_type = 'NEXT_STOP' THEN 
                     CASE 
                       WHEN size(all_sharing_routes) > 0 THEN apoc.coll.toSet(all_sharing_routes + backup_route_name)
                       ELSE [backup_route_name]
                     END
                   ELSE [] 
                 END AS final_routes
            RETURN final_routes
        }

        WITH path_idx, totalCost, idx, fs1, fs2, rel_type, ns, r_node, b, p_r, a, tf_rel, final_routes
        WHERE rel_type IS NOT NULL

        WITH path_idx, totalCost, idx, {
          idx: idx, from_id: fs1.node_id, from_name: fs1.name, to_id: fs2.node_id, to_name: fs2.name, rel_type: rel_type,
          route_name: CASE 
                        WHEN rel_type = 'TRANSFER' THEN '도보' 
                        WHEN rel_type = 'BOARD' THEN coalesce(p_r.num, p_r.name) + ' 대기'
                        WHEN rel_type = 'ALIGHT' THEN '하차'
                        ELSE apoc.text.join(final_routes, ',')
                      END,
          distance: coalesce(coalesce(ns, b, a, tf_rel).distance, 0),
          duration_sec: coalesce(coalesce(ns, b, a, tf_rel).duration_normal, 0)
        } AS record
        ORDER BY totalCost ASC, idx ASC

        RETURN path_idx, totalCost, collect(record) AS path_records
        ORDER BY totalCost ASC
        """
        try:
            with self.driver.session(database="neo4j") as session:
                result = session.run(cypher_query, start_id=start_id, end_id=end_id, current_time=current_time, blocked_ids=blocked_ids)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Neo4j 쿼리 3 실행 에러: {e}")
            return []
    
    def format_perfect_routing_paths(self, records):
        if not records: return []
        
        path_groups = defaultdict(list)
        for path_data in records:
            p_idx = path_data['path_idx']
            for row in path_data['path_records']:
                row['path_idx'] = p_idx
                path_groups[p_idx].append(row)

        results = []
        existing_path_station_sets = []
        
        def clean_station_core_name(name):
            if not name: return ""
            name = name.split(" (")[0].split("(")[0]
            for i in range(1, 15):
                name = name.replace(f"{i}번출구", "")
            return name.strip().rstrip('.')

        for path_idx, rows in path_groups.items():
            rows.sort(key=lambda x: x['idx'])
            
            compressed_segments = []
            current_seg = None
            active_bus_intersection = set()
            is_first_link_in_transit = False
            
            for row in rows:
                rel_type = row['rel_type']
                duration_min = round(row['duration_sec'] / 60, 1)
                
                clean_from_name = row['from_name'].split(" (")[0]
                clean_to_name = row['to_name'].split(" (")[0]

                if rel_type == 'BOARD':
                    if current_seg:
                        if current_seg['type'] == 'TRANSIT':
                            current_seg['display_name'] = sorted(list(active_bus_intersection))
                        compressed_segments.append(current_seg)
                        
                    route_name = row['route_name'].replace(" 대기", "") if row['route_name'] else "정체불명 노선"
                    active_bus_intersection = {route_name}
                    is_first_link_in_transit = True
                    
                    current_seg = {
                        "type": "TRANSIT", "display_name": [], "segment_duration_min": duration_min, 
                        "total_distance_m": 0, "stop_count": 0, "stations": [clean_from_name]
                    }
                elif rel_type == 'NEXT_STOP':
                    if current_seg and current_seg['type'] == 'TRANSIT':
                        current_seg['segment_duration_min'] += duration_min
                        current_seg['total_distance_m'] += row['distance']
                        current_seg['stop_count'] += 1
                        if clean_to_name not in current_seg['stations']:
                            current_seg['stations'].append(clean_to_name)
                        
                        link_buses = set()
                        if row['route_name']:
                            link_buses = {b.strip() for b in row['route_name'].split(',') if b.strip()}
                        
                        if is_first_link_in_transit:
                            if link_buses: active_bus_intersection = link_buses.copy()
                            is_first_link_in_transit = False
                        else:
                            if link_buses: active_bus_intersection = active_bus_intersection.intersection(link_buses)
                elif rel_type == 'TRANSFER':
                    if current_seg:
                        if current_seg['type'] == 'TRANSIT':
                            current_seg['display_name'] = sorted(list(active_bus_intersection))
                        compressed_segments.append(current_seg)
                        
                    tf_seg = {
                        "type": "TRANSFER", "display_name": ["도보"], "segment_duration_min": duration_min,
                        "total_distance_m": row['distance'], "stop_count": 0, "stations": [clean_from_name, clean_to_name]
                    }
                    compressed_segments.append(tf_seg)
                    current_seg = None
                    active_bus_intersection = set()
                elif rel_type == 'ALIGHT':
                    if current_seg and current_seg['type'] == 'TRANSIT':
                        current_seg['segment_duration_min'] += duration_min
                        if clean_to_name not in current_seg['stations']:
                            current_seg['stations'].append(clean_to_name)
                        current_seg['display_name'] = sorted(list(active_bus_intersection))

            if current_seg:
                if current_seg['type'] == 'TRANSIT':
                    current_seg['display_name'] = sorted(list(active_bus_intersection))
                compressed_segments.append(current_seg)

            final_segments = []
            for seg in compressed_segments:
                if not seg['stations']: continue
                
                if seg['type'] == 'TRANSFER' and len(seg['stations']) >= 2:
                    if seg['stations'][0] == seg['stations'][-1] and seg['segment_duration_min'] == 0:
                        continue
                        
                if not final_segments:
                    final_segments.append(seg)
                else:
                    prev = final_segments[-1]
                    if prev['type'] == 'TRANSFER' and seg['type'] == 'TRANSFER':
                        prev['segment_duration_min'] += seg['segment_duration_min']
                        prev['total_distance_m'] += seg['total_distance_m']
                        prev['stations'] = [prev['stations'][0], seg['stations'][-1]]
                    else:
                        final_segments.append(seg)

            for seg in final_segments:
                if seg['type'] == 'TRANSFER' and len(seg['stations']) > 2:
                    seg['stations'] = [seg['stations'][0], seg['stations'][-1]]
                
                clean_st = []
                for st in seg['stations']:
                    if not clean_st or clean_st[-1] != st:
                        clean_st.append(st)
                seg['stations'] = clean_st

            current_path_stations = set()
            for s in final_segments:
                for st_name in s['stations']:
                    current_path_stations.add(clean_station_core_name(st_name))

            is_substandard_duplicate = False
            for existing_set in existing_path_station_sets:
                intersection_size = len(current_path_stations.intersection(existing_set))
                if len(current_path_stations) > 0:
                    match_ratio = intersection_size / float(len(current_path_stations))
                    if match_ratio >= 0.85 or current_path_stations.issubset(existing_set):
                        is_substandard_duplicate = True
                        break
            
            if is_substandard_duplicate:
                continue 
                
            existing_path_station_sets.append(current_path_stations)

            transit_seg_count = sum(1 for s in final_segments if s['type'] == 'TRANSIT')
            calculated_transfer = max(0, transit_seg_count - 1)

            has_marathon_walk = False
            for i, seg in enumerate(final_segments):
                if i == 0 or i == len(final_segments) - 1:
                    continue 
                if seg['type'] == 'TRANSFER' and seg['total_distance_m'] > 400:
                    has_marathon_walk = True
                    break
                    
            if has_marathon_walk:
                continue 

            results.append({
                "path_id": len(results),
                "total_duration_min": round(sum(s['segment_duration_min'] for s in final_segments), 1),
                "transfer_count": calculated_transfer, 
                "path_segments": final_segments
            })

        results.sort(key=lambda x: x['total_duration_min'])
        for i, res in enumerate(results): res['path_id'] = i
        return results