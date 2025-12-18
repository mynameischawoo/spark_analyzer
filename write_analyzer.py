
import json
import re
import os
import sys
from typing import List, Dict, Any, Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from spark_log_parser import get_log_lines

def parse_simple_string(node_name: str, simple_string: str) -> Dict[str, str]:
    info = {"Path": "Unknown", "Format": "Unknown", "Mode": "Unknown"}
    if not simple_string:
        return info

    if "InsertIntoHadoopFsRelationCommand" in node_name:
        cmd_prefix = "Execute InsertIntoHadoopFsRelationCommand "
        if simple_string.startswith(cmd_prefix):
             remainder = simple_string[len(cmd_prefix):]
             args = remainder.split(", ")
             info["Path"] = args[0]
             if len(args) > 2: info["Format"] = args[2]
             if len(args) > 4: info["Mode"] = args[4]
             
    elif "SaveIntoDataSourceCommand" in node_name:
        # SaveIntoDataSourceCommand usually follows: Format, Mode, ...
        # Example: SaveIntoDataSourceCommand org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat@..., Overwrite, ...
        parts = simple_string.split(", ")
        if len(parts) >= 2:
             # Format often needs cleaning "org....ParquetFileFormat"
             fmt = parts[0].split("SaveIntoDataSourceCommand ")[-1]
             if "Parquet" in fmt: info["Format"] = "Parquet"
             elif "Orc" in fmt: info["Format"] = "ORC"
             elif "Csv" in fmt: info["Format"] = "CSV"
             elif "Json" in fmt: info["Format"] = "JSON"
             else: info["Format"] = fmt
             
             info["Mode"] = parts[1]
             # Path might be in metadata or arguments
    
    return info

def analyze_write_metrics(log_files: List[str]) -> List[Dict[str, Any]]:
    # 1. State Tracking
    active_writes = {} # exec_id -> {meta, metrics_map, acc_val, task_bytes, task_rows, task_files_est}
    
    # Map Stage -> Job -> Execution
    # But usually Stage -> Execution directly via Job
    stage_to_execution = {}
    job_to_execution = {}
    
    log_files = sorted(log_files)
    
    for log_path in log_files:
        line_gen = get_log_lines(log_path)
        for line in line_gen:
            try:
                event = json.loads(line)
                evt_type = event.get("Event", "")
                
                # Link Job to Execution (via JobStart)
                if evt_type == "SparkListenerJobStart":
                    job_id = event.get("Job ID")
                    props = event.get("Properties", {})
                    exec_id_str = props.get("spark.sql.execution.id")
                    stage_ids = event.get("Stage IDs", [])
                    
                    if exec_id_str:
                        exec_id = int(exec_id_str)
                        job_to_execution[job_id] = exec_id
                        for sid in stage_ids:
                            stage_to_execution[sid] = exec_id
                            
                # Detect Write (SQL Execution Start)
                elif evt_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                    exec_id = event.get("executionId")
                    plan = event.get("sparkPlanInfo", {})
                    
                    # Find Write Node
                    def find_write_node(node):
                        if not node: return None
                        metrics = node.get("metrics", [])
                        for m in metrics:
                            if m['name'] == 'number of written files':
                                return node
                        for child in node.get("children", []):
                            res = find_write_node(child)
                            if res: return res
                        return None
                        
                    write_node = find_write_node(plan)
                    if write_node and exec_id is not None:
                        # Init entry
                        metrics_map = {m['name']: m['accumulatorId'] for m in write_node.get("metrics", [])}
                        meta = parse_simple_string(write_node.get("nodeName", ""), write_node.get("simpleString", ""))
                        
                        active_writes[exec_id] = {
                            "executionId": exec_id,
                            "metrics_map": metrics_map,
                            "meta": meta,
                            "accumulators": {}, # Driver updates
                            "task_bytes": 0,
                            "task_records": 0,
                            "task_files_est": 0
                        }

                # Driver Updates (Accumulators)
                elif evt_type == "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates":
                    exec_id = event.get("executionId")
                    if exec_id in active_writes:
                        updates = event.get("accumUpdates", [])
                        for update in updates:
                            if len(update) >= 2:
                                aid = int(update[0])
                                val = update[1]
                                active_writes[exec_id]["accumulators"][aid] = val
                                
                # Task End (Standard Metrics)
                elif evt_type == "SparkListenerTaskEnd":
                    sid = event.get("Stage ID")
                    exec_id = stage_to_execution.get(sid)
                    
                    if exec_id in active_writes:
                        out_metrics = event.get("Task Metrics", {}).get("Output Metrics", {})
                        bw = out_metrics.get("Bytes Written", 0)
                        rw = out_metrics.get("Records Written", 0)
                        
                        active_writes[exec_id]["task_bytes"] += bw
                        active_writes[exec_id]["task_records"] += rw
                        if bw > 0:
                            active_writes[exec_id]["task_files_est"] += 1
                            
            except: continue
            
    # Compile Results
    results = []
    for exec_id, info in active_writes.items():
        m = info["metrics_map"]
        accs = info["accumulators"]
        
        # Consolidate
        acc_files = accs.get(m.get("number of written files"), 0)
        acc_bytes = accs.get(m.get("written output"), 0)
        acc_rows = accs.get(m.get("number of output rows"), 0)
        
        # Use Max of Acc vs TaskSum
        # (Usually Acc is 0 if missing, so TaskSum wins if available)
        final_bytes = max(acc_bytes, info["task_bytes"])
        final_rows = max(acc_rows, info["task_records"])
        final_files = max(acc_files, info["task_files_est"])
        
        avg_size = 0
        if final_files > 0:
            avg_size = final_bytes / final_files
            
        results.append({
            "Files Written": final_files,
            "Rows": final_rows,
            "Bytes Written": final_bytes,
            "Average File Size": int(avg_size), # Int for cleaner output
            "File Path": info["meta"]["Path"],
            "Format": info["meta"]["Format"],
            "Mode": info["meta"]["Mode"]
        })
        
    return results

if __name__ == "__main__":
    import glob
    target_files = glob.glob("event_logs/appstatus_*")
    if not target_files:
        target_files = glob.glob("event_logs/events_*")
        
    if target_files:
        print("Analyzing:", target_files[0])
        metrics = analyze_write_metrics([target_files[0]])
        print(json.dumps(metrics, indent=2))
