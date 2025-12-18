
import json
import re
import os
import sys
from typing import List, Dict, Any

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from spark_log_parser import get_log_lines

def simplify_node_name(name: str) -> str:
    # Simplify common Spark node names for readability
    if "Scan" in name or "BatchScan" in name or "FileScan" in name:
        # Extract format often in name: "Scan orc" -> "Read Orc"
        # name "Scan orc" -> "Read Orc"
        # name "FileScan parquet" -> "Read Parquet"
        parts = name.split(" ")
        fmt = parts[1] if len(parts) > 1 else ""
        if fmt:
            return f"Read {fmt.capitalize()}"
        return "Read"
    elif "Filter" in name:
        return "Filter"
    elif "Project" in name:
        return "Select"
    elif "Aggregate" in name:
        return "Aggregate"
    elif "Exchange" in name:
        return "Shuffle"
    elif "Union" in name:
        return "Union"
    elif "Sort" in name:
        return "Sort"
    elif "Join" in name:
        return "Join"
    elif "Window" in name:
        return "Window"
    elif "Limit" in name:
        return "Limit"
    elif "InsertInto" in name or "Write" in name:
        return "Write"
    elif "AdaptiveSparkPlan" in name:
        return "Adaptive Query Execution"
    elif "Collect" in name:
        return "Collect"
    elif "Command" in name:
        return "Command"
    else:
        return name

def linearize_plan(node):
    """
    Traverses the plan tree and returns a list of operations in data flow order (Bottom-Up).
    """
    ops = []
    
    # Children first (Input)
    for child in node.get("children", []):
        ops.extend(linearize_plan(child))
        
    # Then self (Process)
    name = node.get("nodeName", "")
    simple = simplify_node_name(name)
    
    # Avoid duplicates if similar nodes are nested directly (optimization)?
    # Or just list them.
    # User example: "Read Orc, Filter, Select, Distinct..."
    # If we have Scan -> Filter -> Project -> HashAggregate
    # Output: Read Orc, Filter, Select, Aggregate
    
    # Avoid generic wrappers like "AdaptiveSparkPlan" or "WholeStageCodegen"
    if "WholeStageCodegen" in name or "AdaptiveSparkPlan" in name:
        pass # Skip wrapper, just get children
    else:
        ops.append(simple)
        
    return ops

def analyze_job_operations(log_files: List[str]) -> List[Dict[str, Any]]:
    """
    Groups tasks by Job Description and lists sequence of operations.
    """
    files = sorted(log_files)
    
    # State tracking
    execution_info = {} # ExecID -> {desc, plan, startTime, duration}
    
    for log_path in files:
        line_gen = get_log_lines(log_path)
        for line in line_gen:
            try:
                event = json.loads(line)
                evt = event.get("Event", "")
                
                if evt == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                    exec_id = event.get("executionId")
                    desc = event.get("description", "Unknown")
                    plan = event.get("sparkPlanInfo", {})
                    time = event.get("time", 0)
                    
                    execution_info[exec_id] = {
                        "executionId": exec_id,
                        "description": desc,
                        "plan": plan,
                        "startTime": time,
                        "endTime": 0 # Will update if found
                    }
                    
                elif evt == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
                    exec_id = event.get("executionId")
                    time = event.get("time", 0)
                    if exec_id in execution_info:
                        execution_info[exec_id]["endTime"] = time
                    
            except: continue
            
    # Group by description but retain distinct execution info for the first occurrence
    unique_types = {} # Description -> ExecInfo (First Occurrence)
    
    # Sort executions by ID to ensure procession order
    sorted_execs = sorted(execution_info.values(), key=lambda x: x["executionId"])
    
    for info in sorted_execs:
        desc = info["description"]
        if desc not in unique_types:
            unique_types[desc] = info
            
    # Format outcome
    results = []
    # Sort output by Execution Order of the FIRST occurrence
    start_ordered = sorted(unique_types.values(), key=lambda x: x["executionId"])
    
    import datetime
    
    for info in start_ordered:
        ops = linearize_plan(info["plan"])
        
        # Clean ops
        clean_ops = []
        prev = None
        for op in ops:
            if op == prev: continue
            if op == "Adaptive Query Execution": continue
            clean_ops.append(op)
            prev = op
            
        start_ts = info["startTime"]
        end_ts = info["endTime"]
        
        start_str = ""
        if start_ts > 0:
            dt = datetime.datetime.fromtimestamp(start_ts / 1000.0)
            start_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            
        duration = 0
        if start_ts > 0 and end_ts > 0:
            duration = (end_ts - start_ts) / 1000.0
            
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
