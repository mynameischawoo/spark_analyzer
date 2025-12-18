
import json
import re
import os
import sys
import datetime
from typing import List, Dict, Any

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from spark_log_parser import get_log_lines

# --- Metadata Parsing Helpers ---

def parse_read_metadata(node):
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

def parse_write_metadata(node_name: str, simple_string: str) -> Dict[str, str]:
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
        parts = simple_string.split(", ")
        if len(parts) >= 2:
             fmt = parts[0].split("SaveIntoDataSourceCommand ")[-1]
             if "Parquet" in fmt: info["Format"] = "Parquet"
             elif "Orc" in fmt: info["Format"] = "ORC"
             elif "Csv" in fmt: info["Format"] = "CSV"
             elif "Json" in fmt: info["Format"] = "JSON"
             else: info["Format"] = fmt
             info["Mode"] = parts[1]
    
    return info

def simplify_node_name(name: str) -> str:
    if "Scan" in name or "BatchScan" in name or "FileScan" in name:
        parts = name.split(" ")
        fmt = parts[1] if len(parts) > 1 else ""
        if fmt: return f"Read {fmt.capitalize()}"
        return "Read"
    elif "Filter" in name: return "Filter"
    elif "Project" in name: return "Select"
    elif "Aggregate" in name: return "Aggregate"
    elif "Exchange" in name: return "Shuffle"
    elif "InsertInto" in name or "Write" in name: return "Write"
    elif "AdaptiveSparkPlan" in name: return "Adaptive Query Execution"
    # Mapping simple names
    replacements = {
        "Union": "Union", "Sort": "Sort", "Join": "Join", "Window": "Window",
        "Limit": "Limit", "Collect": "Collect", "Command": "Command"
    }
    for k, v in replacements.items():
        if k in name: return v
    return name

# --- Analysis Logic ---

def analyze_job_operations(log_files: List[str]) -> List[Dict[str, Any]]:
    files = sorted(log_files)
    
    execution_info = {}
    stage_to_execution = {}
    
    def find_tracked_nodes(node, tracked=None):
        if tracked is None: tracked = {}
        name = node.get("nodeName", "")
        
        # Identify interesting nodes
        is_read = "Scan" in name or "BatchScan" in name
        
        is_write = False
        metrics = node.get("metrics", [])
        m_names = [m['name'] for m in metrics]
        
        # Check for Write
        if 'number of written files' in m_names:
            is_write = True
            
        # Check for Shuffle (Exchange)
        is_shuffle = 'shuffle write time' in m_names
        
        # Check for Aggregate
        is_agg = 'time in aggregation build' in m_names
        
        # Decide if we track this node
        if is_read or is_write or is_shuffle or is_agg:
            # Generate unique ID for this node instance in this plan
            m_map = {m['name']: m['accumulatorId'] for m in metrics}
            if m_map:
                sig = tuple(sorted(m_map.values()))
                
                info = {
                    "type": "read" if is_read else ("write" if is_write else ("shuffle" if is_shuffle else "agg")),
                    "metrics_map": m_map,
                    "accumulators": {},
                    "task_input_bytes": 0, "task_input_records": 0, # For reads
                    "task_output_bytes": 0, "task_output_records": 0, # For writes
                    "task_files_est": 0
                }
                
                if is_read:
                    info["meta"] = parse_read_metadata(node)
                elif is_write:
                    info["meta"] = parse_write_metadata(node.get("nodeName", ""), node.get("simpleString", ""))
                else:
                    info["meta"] = {}
                
                node["_analyzer_sig"] = sig
                tracked[sig] = info
                
        for child in node.get("children", []):
            find_tracked_nodes(child, tracked)
        return tracked

    for log_path in files:
        line_gen = get_log_lines(log_path)
        for line in line_gen:
            try:
                event = json.loads(line)
                evt = event.get("Event", "")
                
                if evt == "SparkListenerJobStart":
                     props = event.get("Properties", {})
                     exec_id_str = props.get("spark.sql.execution.id")
                     stage_ids = event.get("Stage IDs", [])
                     if exec_id_str:
                         eid = int(exec_id_str)
                         for sid in stage_ids:
                             stage_to_execution[sid] = eid
                             
                elif evt == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                    exec_id = event.get("executionId")
                    desc = event.get("description", "Unknown")
                    plan = event.get("sparkPlanInfo", {})
                    time = event.get("time", 0)
                    
                    tracked = find_tracked_nodes(plan)
                    
                    execution_info[exec_id] = {
                        "executionId": exec_id,
                        "description": desc,
                        "plan": plan,
                        "startTime": time,
                        "endTime": 0,
                        "tracked_nodes": tracked
                    }
                    
                elif evt == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
                    eid = event.get("executionId")
                    if eid in execution_info:
                        execution_info[eid]["endTime"] = event.get("time", 0)

                elif evt == "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates":
                    eid = event.get("executionId")
                    updates = event.get("accumUpdates", [])
                    if eid in execution_info:
                        nodes = execution_info[eid]["tracked_nodes"]
                        for update in updates:
                            if len(update) >= 2:
                                aid = int(update[0])
                                val = update[1]
                                # Find node with this aid
                                for n_info in nodes.values():
                                    if aid in n_info["metrics_map"].values():
                                        n_info["accumulators"][aid] = val
                                        
                elif evt == "SparkListenerTaskEnd":
                    sid = event.get("Stage ID")
                    eid = stage_to_execution.get(sid)
                    
                    if eid in execution_info:
                        t_metrics = event.get("Task Metrics", {})
                        
                        # Input
                        i_metrics = t_metrics.get("Input Metrics", {})
                        br = i_metrics.get("Bytes Read", 0)
                        rr = i_metrics.get("Records Read", 0)
                        
                        # Output
                        o_metrics = t_metrics.get("Output Metrics", {})
                        bw = o_metrics.get("Bytes Written", 0)
                        rw = o_metrics.get("Records Written", 0)
                        
                        nodes = execution_info[eid]["tracked_nodes"]
                        
                        # Distribute Input
                        if br > 0 or rr > 0:
                            reads = [n for n in nodes.values() if n["type"] == "read"]
                            if reads:
                                reads[0]["task_input_bytes"] += br
                                reads[0]["task_input_records"] += rr
                                
                        # Distribute Output
                        if bw > 0 or rw > 0:
                            writes = [n for n in nodes.values() if n["type"] == "write"]
                            if writes:
                                writes[0]["task_output_bytes"] += bw
                                writes[0]["task_output_records"] += rw
                                if bw > 0: writes[0]["task_files_est"] += 1

            except: continue

    unique_types = {}
    sorted_execs = sorted(execution_info.values(), key=lambda x: x["executionId"])
    for info in sorted_execs:
        if info["description"] not in unique_types:
            unique_types[info["description"]] = info
            
    start_ordered = sorted(unique_types.values(), key=lambda x: x["executionId"])
    results = []
    
    for info in start_ordered:
        
        def resolve_metrics(node_sig):
            n_data = info["tracked_nodes"].get(node_sig)
            if not n_data: return None
            
            m = n_data["metrics_map"]
            accs = n_data["accumulators"]
            res = {}
             
            # Time Metrics Calculation (Sum of accumulators)
            # Scan: scan time + metadata time
            # Write: task commit time + job commit time
            # Shuffle: shuffle write time
            # Agg: time in aggregation build
            
            dur_ms = 0
            
            if n_data["type"] == "read":
                 dur_ms += accs.get(m.get("scan time"), 0)
                 dur_ms += accs.get(m.get("metadata time"), 0)
                 
                 acc_files = accs.get(m.get("number of files read"), 0)
                 acc_rows = accs.get(m.get("number of output rows"), 0)
                 acc_bytes = accs.get(m.get("size of files read"), 0)
                 
                 final_bytes = max(acc_bytes, n_data["task_input_bytes"])
                 final_rows = max(acc_rows, n_data["task_input_records"])
                 final_files = acc_files if acc_files > 0 else (1 if final_bytes > 0 else 0)
                 
                 res["Files Read"] = final_files
                 res["Rows"] = final_rows
                 res["Bytes Read"] = final_bytes
                 res["Average File Size"] = int(final_bytes / final_files) if final_files > 0 else 0
                 res.update(n_data["meta"])

            elif n_data["type"] == "write":
                 dur_ms += accs.get(m.get("task commit time"), 0)
                 dur_ms += accs.get(m.get("job commit time"), 0)
                 
                 acc_files = accs.get(m.get("number of written files"), 0)
                 acc_rows = accs.get(m.get("number of output rows"), 0)
                 acc_bytes = accs.get(m.get("written output"), 0)
                 
                 final_bytes = max(acc_bytes, n_data["task_output_bytes"])
                 final_rows = max(acc_rows, n_data["task_output_records"])
                 final_files = max(acc_files, n_data["task_files_est"])
                 
                 res["Files Written"] = final_files
                 res["Rows"] = final_rows
                 res["Bytes Written"] = final_bytes
                 res["Average File Size"] = int(final_bytes / final_files) if final_files > 0 else 0
                 res.update(n_data["meta"])
                 
            elif n_data["type"] == "shuffle":
                 dur_ms += accs.get(m.get("shuffle write time"), 0)
                 # Wait time usually on read side?
                 
            elif n_data["type"] == "agg":
                 dur_ms += accs.get(m.get("time in aggregation build"), 0)
                 
            # Add Duration Field
            if dur_ms > 0:
                 res["Operation Duration"] = round(dur_ms / 1000.0, 2)
            
            return res

        def linearize(node):
            ops = []
            for child in node.get("children", []):
                ops.extend(linearize(child))
            
            name = node.get("nodeName", "")
            simple = simplify_node_name(name)
            
            # Construct Operation Object
            op_obj = {"Operation": simple}
            
            # If tracked, add details
            sig = node.get("_analyzer_sig")
            if sig:
                details = resolve_metrics(sig)
                if details:
                   op_obj.update(details)
                   
            if "Adaptive" in simple or "WholeStage" in name:
                pass
            else:
                ops.append(op_obj)
            return ops

        ops_list = linearize(info["plan"])
        
        clean_ops = []
        prev = None
        for op in ops_list:
            if op == prev: continue
            clean_ops.append(op)
            prev = op
            
        start_str = ""
        if info["startTime"] > 0:
            dt = datetime.datetime.fromtimestamp(info["startTime"] / 1000.0)
            start_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            
        duration = (info["endTime"] - info["startTime"]) / 1000.0 if (info["endTime"] > 0) else 0
        
        results.append({
            "Execution Order": info["executionId"],
            "Job Description": info["description"],
            "Start Time": start_str,
            "Duration": round(duration, 2),
            "Operations": clean_ops
        })
        
    return results

if __name__ == "__main__":
    import glob
    target_files = glob.glob("event_logs/appstatus_*")
    if not target_files:
        target_files = glob.glob("event_logs/events_*")
        
    if target_files:
        print("Analyzing:", target_files[0])
        metrics = analyze_job_operations([target_files[0]])
        print(json.dumps(metrics, indent=2))
