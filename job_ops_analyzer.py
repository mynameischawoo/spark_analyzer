
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
        is_shuffle = 'shuffle records written' in m_names or 'shuffle bytes written' in m_names or 'Exchange' in name
        
        # Check for Aggregate
        is_agg = 'time in aggregation build' in m_names
        
        # Check for Filter
        is_filter = 'number of output rows' in m_names and 'Filter' in name
        
        # Check for Project (Select)
        is_project = 'Project' in name
        
        # Check for Coalesce
        is_coalesce = 'Coalesce' in name
        
        # Decide if we track this node
        if is_read or is_write or is_shuffle or is_agg or is_filter or is_project or is_coalesce:
            # Generate unique ID for this node instance in this plan
            # Project/Coalesce might not have metrics. Use safe signature.
            m_map = {m['name']: m['accumulatorId'] for m in metrics}
            
            if m_map:
                sig = tuple(sorted(m_map.values()))
            else:
                # Use a unique signature for metric-less nodes
                sig = f"node_{id(node)}"
                
            info = {
                    "type": "read" if is_read else ("write" if is_write else ("shuffle" if is_shuffle else ("agg" if is_agg else ("filter" if is_filter else ("project" if is_project else "coalesce"))))),
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
            elif is_filter:
                # Parse Condition from simpleString: "Filter (condition)"
                s_str = node.get("simpleString", "")
                cond = "Unknown"
                if s_str.startswith("Filter "):
                    cond = s_str[7:].strip()
                info["meta"] = {"Condition": cond}
            elif is_project:
                # Parse Selected Fields from simpleString: "Project [col1, col2]"
                s_str = node.get("simpleString", "")
                fields = "Unknown"
                if s_str.startswith("Project [") and s_str.endswith("]"):
                    fields = s_str[9:-1]
                info["meta"] = {"Selected Fields": fields}
            elif is_agg:
                # Parse Keys and Functions from simpleString
                s_str = node.get("simpleString", "")
                keys = "Unknown"
                funcs = "Unknown"
                
                k_start = s_str.find("keys=[")
                if k_start != -1:
                    k_end = s_str.find("]", k_start)
                    if k_end != -1:
                        keys = s_str[k_start+6:k_end]
                
                f_start = s_str.find("functions=[")
                if f_start != -1:
                    f_end = s_str.find("]", f_start)
                    if f_end != -1:
                        funcs = s_str[f_start+11:f_end]
                        
                info["meta"] = {"Aggregate Keys": keys, "Aggregate Functions": funcs}
            elif is_shuffle:
                 # Parse Partitioning from simpleString: "Exchange hashpartitioning(..., 4), ..."
                 s_str = node.get("simpleString", "")
                 part = "Unknown"
                 num_part = "Unknown"
                 
                 # Heuristic: looks for 'partitioning'
                 # Example: Exchange hashpartitioning(sscode#27, ..., 4), ...
                 if "Exchange" in s_str:
                     parts = s_str.split("Exchange ", 1)
                     if len(parts) > 1:
                         # content after Exchange
                         rest = parts[1]
                         # partitioning usually until first comma or ')'
                         # e.g. "hashpartitioning(blah, 4), ENSURE_REQUIREMENTS"
                         # Let's verify structure
                         p_end = rest.find(", ENSURE")
                         if p_end == -1: p_end = len(rest)
                         part_str = rest[:p_end].strip()
                         
                         # Parse Num Partitions if possible (usually last arg in parens)
                         # hashpartitioning(col, 4)
                         r_paren = part_str.rfind(")")
                         if r_paren != -1:
                             # search backwards for comma
                             l_comma = part_str.rfind(",", 0, r_paren)
                             if l_comma != -1:
                                 try:
                                     num_part = int(part_str[l_comma+1:r_paren].strip())
                                 except: pass
                         
                         part = part_str
                 
                 info["meta"] = {"Partitioning": part, "Num Partitions": num_part}
            elif is_coalesce:
                 # Parse Coalesce target: "Coalesce 1"
                 s_str = node.get("simpleString", "")
                 num_part = "Unknown"
                 if "Coalesce" in s_str:
                     try:
                         num_part = int(s_str.replace("Coalesce", "").strip())
                     except: pass
                 info["meta"] = {"Num Partitions": num_part}
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
                                
                elif evt == "SparkListenerStageCompleted":
                    # Capture hidden accumulators for Filters
                    stage_info = event.get("Stage Info", {})
                    accumulables = stage_info.get("Accumulables", [])
                    
                    # Extract key metrics
                    scan_output_ids = set()
                    shuffle_records = 0
                    candidate_rows = []
                    
                    for acc in accumulables:
                        name = acc.get("Name")
                        val = int(acc.get("Value", 0))
                        aid = acc.get("ID")
                        
                        if name == "shuffle records written":
                            shuffle_records = val
                            
                        if name == "number of output rows":
                            candidate_rows.append((aid, val))
                            
                    # We store this info to link later: 
                    # Stage -> {scan_ids_found: [], candidates: [], shuffle: val}
                    # But we need to link it to Execution.
                    # We can use stage_to_execution map? But that map is built during JobStart.
                    # StageCompleted comes after. So map should be ready.
                    sid = stage_info.get("Stage ID")
                    eid = stage_to_execution.get(sid)
                    
                    if eid in execution_info:
                        if "stage_metrics" not in execution_info[eid]:
                             execution_info[eid]["stage_metrics"] = []
                        execution_info[eid]["stage_metrics"].append({
                            "candidates": candidate_rows,
                            "shuffle": shuffle_records
                        })

            except: continue

    unique_types = {}
    sorted_execs = sorted(execution_info.values(), key=lambda x: x["executionId"])
    for info in sorted_execs:
        if info["description"] not in unique_types:
            unique_types[info["description"]] = info
            
    start_ordered = sorted(unique_types.values(), key=lambda x: x["executionId"])
    results = []
    
    for info in start_ordered:
        
        # Pre-process stage metrics to find Filter outputs
        # We need to map Filter -> Scan ID
        # But 'tracked_nodes' doesn't explicitly store parent-child.
        # However, we can infer it: Filter usually shares the same Stage as its child Scan.
        # And we know the Scan's "number of output rows" accumulator ID from tracked_nodes.
        # Let's iterate tracked Reads, find their ID, then find the corresponding Stage, then find the 'candidate' that is not Scan/Shuffle.
        
        filter_overrides = {} # FilterSig -> Rows
        
        reads = [n for n in info["tracked_nodes"].values() if n["type"] == "read"]
        filters = [n for n in info["tracked_nodes"].values() if n["type"] == "filter"]
        
        # Assuming 1-to-1 mapping or simple branches.
        # For each read, find the stage metrics that contain its accumulator ID.
        for read in reads:
            read_rows_id = read["metrics_map"].get("number of output rows")
            if not read_rows_id: continue
            
            # Find matching stage metric block
            target_block = None
            for block in info.get("stage_metrics", []):
                # block['candidates'] is list of (id, val)
                ids = [c[0] for c in block["candidates"]]
                if read_rows_id in ids:
                    target_block = block
                    break
            
            if target_block:
                # We found the stage. Now find the Filter output.
                # Filter Output = Value in candidates that is NOT read_rows (or duplicates) AND NOT shuffle.
                # Also Scan might have duplicates (e.g. 211 and 296).
                # Shuffle records is 'target_block["shuffle"]'.
                
                scan_val = 0
                for c in target_block["candidates"]:
                    if c[0] == read_rows_id: scan_val = c[1]
                
                # Filter candidates:
                # Exclude if val == scan_val (unless filter didn't filter anything? risk.)
                # Exclude if val == shuffle (unless shuffle == filter?)
                # Exclude if val == 0
                
                potential = []
                for aid, val in target_block["candidates"]:
                    if val == 0: continue
                    if val == target_block["shuffle"]: continue
                    # Heuristic: Filter output usually < Scan output
                    if val >= scan_val and scan_val > 0: continue 
                    potential.append(val)
                
                if potential:
                    # Pick largest? Filter output is intermediate.
                    # In our case: 92k (Scan 162k). 1.9k (Scan 162k).
                    best_match = max(potential)
                    
                    # Assign to a Filter. Which one?
                    # The one "closest" to this Read?
                    # Since we don't have graph structure here easily, assign to the FIRST Filter node we haven't assigned yet?
                    # Risky if multiple filters in chain.
                    # But in this job, 1 Filter per branch.
                    # Let's assign to the first filter that currently has 0 rows.
                    for f in filters:
                        if filter_overrides.get(tuple(sorted(f["metrics_map"].values()))) is None:
                             # Use signature as key
                             sig = tuple(sorted(f["metrics_map"].values()))
                             filter_overrides[sig] = best_match
                             break
        
        def resolve_metrics(node_sig):
            n_data = info["tracked_nodes"].get(node_sig)
            if not n_data: return None
            
            m = n_data["metrics_map"]
            accs = n_data["accumulators"]
            res = {}
             
            # Time Metrics
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
                 
            elif n_data["type"] == "agg":
                 dur_ms += accs.get(m.get("time in aggregation build"), 0)
                 res.update(n_data["meta"])
                 
                 # Infer Distinct
                 keys = n_data["meta"].get("Aggregate Keys", "")
                 funcs = n_data["meta"].get("Aggregate Functions", "")
                 if keys and (not funcs or funcs == "[]" or funcs == "Unknown"):
                     res["Is Distinct"] = True

            elif n_data["type"] == "shuffle":
                 dur_ms += accs.get(m.get("shuffle write time"), 0)
                 
                 # Metrics
                 s_bytes = accs.get(m.get("shuffle bytes written"), 0)
                 s_recs = accs.get(m.get("shuffle records written"), 0)
                 if s_bytes > 0: res["Shuffle Bytes Written"] = s_bytes
                 if s_recs > 0: res["Shuffle Records Written"] = s_recs
                 
                 res.update(n_data["meta"])
                 
            elif n_data["type"] == "coalesce":
                 res["Operation"] = "Repartition (Coalesce)"
                 res.update(n_data["meta"])

            elif n_data["type"] == "filter":
                 raw_rows = accs.get(m.get("number of output rows"), 0)
                 
                 # Check override
                 if raw_rows == 0:
                      sig = tuple(sorted(m.values()))
                      if sig in filter_overrides:
                           raw_rows = filter_overrides[sig]
                           
                 res["Output Rows"] = raw_rows
                 res.update(n_data["meta"])

            elif n_data["type"] == "project":
                 res.update(n_data["meta"])
                 
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
