
import json
import re
import os
import sys
from typing import List, Dict, Any

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from spark_log_parser import get_log_lines

def parse_metadata(node):
    info = {
        "File Path": "Unknown",
        "Format": "Unknown", 
        "Partition Filters": "None"
    }
    
    metadata = node.get("metadata", {})
    simple_string = node.get("simpleString", "")
    
    if "Location" in metadata:
        loc = metadata["Location"]
        m = re.search(r"\[(.*?)\]", loc)
        if m: info["File Path"] = m.group(1)
        else: info["File Path"] = loc
    
    if "Format" in metadata:
        fmt = metadata["Format"]
        if "Parquet" in fmt: info["Format"] = "Parquet"
        elif "Orc" in fmt: info["Format"] = "ORC"
        elif "BinaryFile" in fmt: info["Format"] = "Binary"
        elif "Json" in fmt: info["Format"] = "JSON"
        elif "Csv" in fmt: info["Format"] = "CSV"
        else: info["Format"] = fmt
    
    if "PartitionFilters" in metadata:
        pf = metadata["PartitionFilters"]
        if pf and pf != "[]": info["Partition Filters"] = pf
            
    return info

def analyze_read_metrics(log_files: List[str]) -> List[Dict[str, Any]]:
    # 1. State Tracking
    active_scans = {} # (exec_id, acc_set_key) -> scan_obj
    stage_to_execution = {}
    
    files = sorted(log_files)
    
    for log_path in files:
        line_gen = get_log_lines(log_path)
        
        for line in line_gen:
            try:
                event = json.loads(line)
                evt_type = event.get("Event", "")
                
                if evt_type == "SparkListenerJobStart":
                     props = event.get("Properties", {})
                     exec_id_str = props.get("spark.sql.execution.id")
                     stage_ids = event.get("Stage IDs", [])
                     if exec_id_str:
                         exec_id = int(exec_id_str)
                         for sid in stage_ids:
                             stage_to_execution[sid] = exec_id
                             
                elif evt_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                    exec_id = event.get("executionId")
                    plan = event.get("sparkPlanInfo", {})
                    
                    def find_scan_nodes(node, nodes_found=None):
                        if nodes_found is None: nodes_found = []
                        name = node.get("nodeName", "")
                        if "Scan" in name or "BatchScan" in name:
                            nodes_found.append(node)
                        for child in node.get("children", []):
                            find_scan_nodes(child, nodes_found)
                        return nodes_found
                        
                    scans = find_scan_nodes(plan)
                    for node in scans:
                        metrics_map = {m['name']: m['accumulatorId'] for m in node.get("metrics", [])}
                        if not metrics_map: continue
                        
                        # Use a stable key. ID is not available.
                        # Use accum IDs tuple as unique signature.
                        # A Scan node always has unique accumulator IDs allocated to it.
                        acc_signature = tuple(sorted(metrics_map.values()))
                        
                        active_scans[acc_signature] = {
                            "exec_id": exec_id,
                            "meta": parse_metadata(node),
                            "metrics_map": metrics_map,
                            "accumulators": {},
                            "task_input_bytes": 0,
                            "task_input_records": 0
                        }
                        
                elif evt_type == "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates":
                    updates = event.get("accumUpdates", [])
                    exec_id = event.get("executionId")
                    for update in updates:
                        if len(update) >= 2:
                            aid = int(update[0])
                            val = update[1]
                            # Find which scan has this aid
                            for scan in active_scans.values():
                                if aid in scan["metrics_map"].values():
                                    scan["accumulators"][aid] = val
                                    
                elif evt_type == "SparkListenerTaskEnd":
                    sid = event.get("Stage ID")
                    exec_id = stage_to_execution.get(sid)
                    
                    if exec_id is not None:
                        metrics = event.get("Task Metrics", {}).get("Input Metrics", {})
                        br = metrics.get("Bytes Read", 0)
                        rr = metrics.get("Records Read", 0)
                        
                        if br > 0 or rr > 0:
                            # Find scans for this exec
                            relevant_scans = [s for s in active_scans.values() if s["exec_id"] == exec_id]
                            
                            if len(relevant_scans) == 1:
                                relevant_scans[0]["task_input_bytes"] += br
                                relevant_scans[0]["task_input_records"] += rr
                            elif len(relevant_scans) > 1:
                                # distribute? 
                                # If multiple scans, we don't know which task read which file.
                                # But typically, different scans are in different Stages (unless broadcast join or union).
                                # If they are in the SAME stage, it's ambiguous.
                                # PROPOSAL: Sum them up for the first scan? 
                                # Or split? splitting is bad.
                                # Assigning to ALL is worse (double counting).
                                # Let's assign to the FIRST one found. It's a heuristic.
                                # Better than 0.
                                relevant_scans[0]["task_input_bytes"] += br
                                relevant_scans[0]["task_input_records"] += rr

            except: continue

    results = []
    
    for scan in active_scans.values():
        m = scan["metrics_map"]
        accs = scan["accumulators"]
        
        acc_files = accs.get(m.get("number of files read"), 0)
        acc_rows = accs.get(m.get("number of output rows"), 0)
        acc_bytes = accs.get(m.get("size of files read"), 0)
        acc_parts = accs.get(m.get("number of partitions read"), 0)
        
        final_bytes = max(acc_bytes, scan["task_input_bytes"])
        final_rows = max(acc_rows, scan["task_input_records"])
        
        final_files = acc_files
        if final_files == 0 and final_bytes > 0:
             final_files = 1 
             
        avg_size = 0
        if final_files > 0:
            avg_size = final_bytes / final_files
            
        if final_files > 0 or final_rows > 0:
            results.append({
                "Files Read": final_files,
                "Rows": final_rows,
                "Bytes Read": final_bytes,
                "Partitions Read": acc_parts,
                "Average file size": int(avg_size),
                "File Path": scan["meta"]["File Path"],
                "Partition Filters": scan["meta"]["Partition Filters"]
            })
            
    return results

if __name__ == "__main__":
    import glob
    target_files = glob.glob("event_logs/appstatus_*")
    if not target_files:
        target_files = glob.glob("event_logs/events_*")
        
    if target_files:
        print("Analyzing:", target_files[0])
        metrics = analyze_read_metrics([target_files[0]])
        print(json.dumps(metrics, indent=2))
