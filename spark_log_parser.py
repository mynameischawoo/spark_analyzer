import json
import glob
import os
import sys
import pandas as pd
import argparse
from typing import List, Dict, Any

def parse_memory(mem_str):
    if not mem_str: return 0
    mem_str = str(mem_str).lower()
    if mem_str.endswith('g'):
        return int(float(mem_str[:-1]) * 1024 * 1024 * 1024)
    elif mem_str.endswith('m'):
        return int(float(mem_str[:-1]) * 1024 * 1024)
    elif mem_str.endswith('k'):
        return int(float(mem_str[:-1]) * 1024)
    elif mem_str.endswith('t'):
        return int(float(mem_str[:-1]) * 1024 * 1024 * 1024 * 1024)
    else:
        try:
            return int(mem_str)
        except:
            return 0

def analyze_single_application(files: List[str]) -> Dict[str, Any]:
    # Initialize metrics
    app_id = "Unknown"
    app_name = "Unknown"
    app_start_time = 0
    app_end_time = 0
    
    # Resources
    driver_cores = 0
    driver_memory = 0
    executor_cores = 1
    executor_memory = 0
    executor_overhead = 0
    executor_instances = 0
    dyn_alloc_enabled = False
    max_executors_conf = 0
    
    # Runtime Tracking
    active_executors = set()
    max_active_executors = 0
    
    executor_launch_times = {} # exec_id -> start_time
    executor_end_times = {}   # exec_id -> end_time
    
    # Task Metrics
    total_task_time_ms = 0
    peak_jvm_heap = 0
    
    # I/O
    total_input_bytes = 0
    total_output_bytes = 0
    
    # Shuffle
    total_shuffle_read = 0
    total_shuffle_write = 0
    
    # Spill
    total_spill_mem = 0
    total_spill_disk = 0
    
    # Max Tracking
    max_shuffle_read_task = 0
    max_shuffle_write_task = 0
    max_spill_mem_task = 0
    max_spill_disk_task = 0
    max_spill_stage_id_for_task = None
    
    # Aggregations
    stage_shuffle_read = {}
    stage_shuffle_write = {}
    stage_spill_mem = {}
    stage_to_job = {}
    job_shuffle_read = {}
    job_shuffle_write = {}
    
    files = sorted(files)
    
    for file_path in files:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("Event")
                    
                    if event_type == "SparkListenerApplicationStart":
                        app_name = event.get("App Name", "Unknown")
                        app_id = event.get("App ID", "Unknown")
                        app_start_time = event.get("Timestamp", 0)
                        
                    elif event_type == "SparkListenerApplicationEnd":
                        app_end_time = event.get("Timestamp", 0)
                        
                    elif event_type == "SparkListenerEnvironmentUpdate":
                        spark_props = event.get("Spark Properties", {})
                        driver_cores = int(spark_props.get("spark.driver.cores", 0))
                        driver_memory = parse_memory(spark_props.get("spark.driver.memory", "0"))
                        executor_cores = int(spark_props.get("spark.executor.cores", 1))
                        executor_memory = parse_memory(spark_props.get("spark.executor.memory", "0"))
                        
                        overhead = spark_props.get("spark.executor.memoryOverhead")
                        if overhead:
                            executor_overhead = parse_memory(overhead)
                        else:
                            # Default overhead logic
                            executor_overhead = max(int(executor_memory * 0.10), 384 * 1024 * 1024)
                            
                        executor_instances = int(spark_props.get("spark.executor.instances", 0))
                        dyn_alloc_enabled = spark_props.get("spark.dynamicAllocation.enabled", "false") == "true"
                        max_executors_conf = int(spark_props.get("spark.dynamicAllocation.maxExecutors", 0))

                    elif event_type == "SparkListenerJobStart":
                        job_id = event.get("Job ID")
                        stage_ids = event.get("Stage IDs", [])
                        for sid in stage_ids:
                            stage_to_job[sid] = job_id
                            
                    elif event_type == "SparkListenerExecutorAdded":
                        exec_id = event.get("Executor ID")
                        if exec_id != "driver":
                            active_executors.add(exec_id)
                            max_active_executors = max(max_active_executors, len(active_executors))
                            executor_launch_times[exec_id] = event.get("Timestamp")
                            
                    elif event_type == "SparkListenerExecutorRemoved":
                        exec_id = event.get("Executor ID")
                        if exec_id in active_executors:
                            active_executors.remove(exec_id)
                        executor_end_times[exec_id] = event.get("Timestamp")

                    elif event_type == "SparkListenerTaskEnd":
                        task_info = event.get("Task Info", {})
                        task_metrics = event.get("Task Metrics", {})
                        
                        if not task_metrics: continue
                        
                        # Task Duration
                        duration = task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0)
                        total_task_time_ms += duration
                        
                        # Peak Heap from Task End (snapshot)
                        task_exec_metrics = event.get("Task Executor Metrics", {})
                        t_heap = task_exec_metrics.get("JVMHeapMemory", 0)
                        if t_heap > peak_jvm_heap:
                            peak_jvm_heap = t_heap
                        
                        # I/O
                        input_metrics = task_metrics.get("Input Metrics", {})
                        output_metrics = task_metrics.get("Output Metrics", {})
                        total_input_bytes += input_metrics.get("Bytes Read", 0)
                        total_output_bytes += output_metrics.get("Bytes Written", 0)
                        
                        # Spill
                        mem_spill = task_metrics.get("Memory Bytes Spilled", 0)
                        disk_spill = task_metrics.get("Disk Bytes Spilled", 0)
                        total_spill_mem += mem_spill
                        total_spill_disk += disk_spill
                        
                        stage_id = event.get("Stage ID")
                        
                        # Aggregate Spill per Stage
                        if stage_id is not None:
                            stage_spill_mem[stage_id] = stage_spill_mem.get(stage_id, 0) + mem_spill
                        
                        # Max Spill Task logic removed from output but kept for internal if needed
                        # if mem_spill > max_spill_mem_task:
                        #     max_spill_mem_task = mem_spill
                        #     max_spill_stage_id_for_task = stage_id
                        
                        # if disk_spill > max_spill_disk_task:
                        #     max_spill_disk_task = disk_spill
                            
                        # Shuffle
                        shuffle_read_metrics = task_metrics.get("Shuffle Read Metrics", {})
                        # Remote + Local
                        s_read = shuffle_read_metrics.get("Remote Bytes Read", 0) + shuffle_read_metrics.get("Local Bytes Read", 0)
                        
                        shuffle_write_metrics = task_metrics.get("Shuffle Write Metrics", {})
                        s_write = shuffle_write_metrics.get("Shuffle Bytes Written", 0)
                        
                        total_shuffle_read += s_read
                        total_shuffle_write += s_write
                        
                        # Max Shuffle Task logic removed from output columns
                        if s_read > max_shuffle_read_task: max_shuffle_read_task = s_read
                        if s_write > max_shuffle_write_task: max_shuffle_write_task = s_write
                        
                        # Aggregations for Stage
                        if stage_id is not None:
                            stage_shuffle_read[stage_id] = stage_shuffle_read.get(stage_id, 0) + s_read
                            stage_shuffle_write[stage_id] = stage_shuffle_write.get(stage_id, 0) + s_write
                            
                    elif event_type == "SparkListenerExecutorMetricsUpdate":
                        # Support both direct 'Executor Metrics' and 'Executor Updates' map
                        if "Executor Metrics" in event:
                             heap_mem = event["Executor Metrics"].get("JVMHeapMemory", 0)
                             if heap_mem > peak_jvm_heap: peak_jvm_heap = heap_mem
                        elif "executorsUpdates" in event:
                             for _, info in event["executorsUpdates"].items():
                                 heap_mem = info.get("JVMHeapMemory", 0)
                                 if heap_mem > peak_jvm_heap: peak_jvm_heap = heap_mem

                except Exception:
                    continue

    # Post-processing calculations
    duration_sec = (app_end_time - app_start_time) / 1000.0 if (app_end_time > 0 and app_start_time > 0) else 0
    
    start_date_str = ""
    if app_start_time > 0:
        import datetime
        dt = datetime.datetime.fromtimestamp(app_start_time / 1000.0)
        start_date_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Job Aggregations
    for sid, s_read in stage_shuffle_read.items():
        jid = stage_to_job.get(sid)
        if jid is not None:
            job_shuffle_read[jid] = job_shuffle_read.get(jid, 0) + s_read
            
    for sid, s_write in stage_shuffle_write.items():
        jid = stage_to_job.get(sid)
        if jid is not None:
            job_shuffle_write[jid] = job_shuffle_write.get(jid, 0) + s_write
            
    # Max Stage/Job Logic
    max_shuffle_read_stage = 0
    max_shuffle_read_stage_id = ""
    if stage_shuffle_read:
        max_sid = max(stage_shuffle_read, key=stage_shuffle_read.get)
        max_shuffle_read_stage = stage_shuffle_read[max_sid]
        max_shuffle_read_stage_id = max_sid

    max_shuffle_write_stage = 0
    max_shuffle_write_stage_id = ""
    if stage_shuffle_write:
        max_sid = max(stage_shuffle_write, key=stage_shuffle_write.get)
        max_shuffle_write_stage = stage_shuffle_write[max_sid]
        max_shuffle_write_stage_id = max_sid

    # Max Spill Stage Logic
    max_spill_mem_stage = 0
    max_spill_stage_id = ""
    if stage_spill_mem:
        max_sid = max(stage_spill_mem, key=stage_spill_mem.get)
        max_spill_mem_stage = stage_spill_mem[max_sid]
        max_spill_stage_id = max_sid

    max_shuffle_read_job = 0
    max_shuffle_read_job_id = ""
    if job_shuffle_read:
        max_jid = max(job_shuffle_read, key=job_shuffle_read.get)
        max_shuffle_read_job = job_shuffle_read[max_jid]
        max_shuffle_read_job_id = max_jid

    max_shuffle_write_job = 0
    max_shuffle_write_job_id = ""
    if job_shuffle_write:
        max_jid = max(job_shuffle_write, key=job_shuffle_write.get)
        max_shuffle_write_job = job_shuffle_write[max_jid]
        max_shuffle_write_job_id = max_jid
    
    # Capacity & Idle
    # Total Capacity = Sum(Executor Lifetime * Cores)
    total_capacity_ms = 0
    for eid in set(list(executor_launch_times.keys()) + list(executor_end_times.keys())):
        start = executor_launch_times.get(eid, app_start_time)
        end = executor_end_times.get(eid, app_end_time)
        if not end: end = app_end_time
        
        life = end - start
        if life > 0:
            total_capacity_ms += life * executor_cores
            
    avg_idle_cores_pct = 0
    if total_capacity_ms > 0:
        idle_ms = total_capacity_ms - total_task_time_ms
        avg_idle_cores_pct = (idle_ms / total_capacity_ms) * 100.0
        
    requested_executors = max_executors_conf if dyn_alloc_enabled else executor_instances
    if requested_executors == 0 and max_active_executors > 0:
        requested_executors = max_active_executors # Fallback if conf missing?
        
    total_cores_capacity = max_active_executors * executor_cores
    total_mem_capacity = max_active_executors * (executor_memory + executor_overhead)
    
    peak_mem_usage_pct = 0
    if executor_memory > 0:
        peak_mem_usage_pct = (peak_jvm_heap / executor_memory) * 100.0
        
    # Construct Result Dict
    metrics = {
        "Start Date": start_date_str,
        "Application ID": app_id,
        "Application Name": app_name,
        "Duration (Sec)": round(duration_sec, 2),
        "Total Input": total_input_bytes,
        "Total Output": total_output_bytes,
        "Driver Cores": driver_cores,
        "Driver Memory": driver_memory,
        "Executor Cores": executor_cores,
        "Executor Memory": executor_memory,
        "Executor Overhead Memory": executor_overhead,
        "Requested Executors": requested_executors,
        "Max Active Executors": max_active_executors,
        "Total Cores Capacity": total_cores_capacity,
        "Total Memory Capacity": total_mem_capacity,
        "Avg Idle Cores (%)": round(avg_idle_cores_pct, 2),
        "Peak Memory Usage (%)": round(peak_mem_usage_pct, 2),
        "Peak Heap Usage": peak_jvm_heap,
        "Total Shuffle Read": total_shuffle_read,
        "Total Shuffle Write": total_shuffle_write,
        "Max Shuffle Read (Stage)": max_shuffle_read_stage,
        "Max Shuffle Read Stage ID": max_shuffle_read_stage_id,
        "Max Shuffle Write (Stage)": max_shuffle_write_stage,
        "Max Shuffle Write Stage ID": max_shuffle_write_stage_id,
        "Max Shuffle Read (Job)": max_shuffle_read_job,
        "Max Shuffle Read Job ID": max_shuffle_read_job_id,
        "Max Shuffle Write (Job)": max_shuffle_write_job,
        "Max Shuffle Write Job ID": max_shuffle_write_job_id,
        "Total Spill (Memory)": total_spill_mem,
        "Total Spill (Disk)": total_spill_disk,
        "Max Spill Memory (Stage)": max_spill_mem_stage,
        "Max Spill Stage ID": max_spill_stage_id
    }
    return metrics

def analyze_stage_details(files: List[str]) -> List[Dict]:
    """
    Analyzes log files to extract detailed stage-level metrics.
    Returns a list of dicts, one per stage, sorted by Stage ID.
    """
    # Sort files by part if needed, usually passed sorted or globbed
    files = sorted(files)
    
    stages = {} # stage_id -> dict
    app_info = {"id": "Unknown", "name": "Unknown"}
    
    for file_path in files:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("Event")
                    
                    if event_type == "SparkListenerApplicationStart":
                        app_info["id"] = event.get("App ID", app_info["id"])
                        app_info["name"] = event.get("App Name", app_info["name"])

                    elif event_type == "SparkListenerStageSubmitted":
                        info = event.get("Stage Info", {})
                        sid = info.get("Stage ID")
                        if sid is not None:
                            # Try to find description
                            desc = info.get("Stage Name", "")
                            props = event.get("Properties", {})
                            if not props:
                                # Sometimes properties are inside Stage Info?? No usually top level in JSON or separate event
                                # But SparkListenerStageSubmitted JSON has "Properties": {...} at top level
                                pass
                            
                            # Standard Spark properties for description
                            job_desc = props.get("spark.job.description", "")
                            if job_desc:
                                desc = job_desc
                            elif "callSite.short" in props:
                                desc = props["callSite.short"]

                            if sid not in stages:
                                stages[sid] = {
                                    "Stage ID": sid,
                                    "Stage Name": info.get("Stage Name", ""),
                                    "Description": desc,
                                    "Status": "Running",
                                    "Submission Time": event.get("Stage Info", {}).get("Submission Time", 0),
                                    "Completion Time": 0,
                                    "Duration": 0,
                                    "Tasks": 0,
                                    "Input": 0,
                                    "Output": 0,
                                    "Shuffle Read": 0,
                                    "Shuffle Write": 0,
                                    "Spill Memory": 0,
                                    "Spill Disk": 0
                                }
                            else:
                                stages[sid]["Stage Name"] = info.get("Stage Name", "")
                                stages[sid]["Description"] = desc
                                stages[sid]["Submission Time"] = info.get("Submission Time", 0)

                    elif event_type == "SparkListenerStageCompleted":
                        info = event.get("Stage Info", {})
                        sid = info.get("Stage ID")
                        if sid is not None:
                            if sid not in stages: stages[sid] = {"Stage ID": sid, "Tasks":0, "Input":0, "Output":0, "Shuffle Read":0, "Shuffle Write":0, "Spill Memory":0, "Spill Disk":0, "Description": ""}
                            stages[sid]["Completion Time"] = info.get("Completion Time", 0)
                            stages[sid]["Status"] = "Completed"
                            if "Failure Reason" in info:
                                stages[sid]["Status"] = "Failed"

                    elif event_type == "SparkListenerTaskEnd":
                        sid = event.get("Stage ID")
                        if sid is not None:
                            if sid not in stages: stages[sid] = {"Stage ID": sid, "Tasks":0, "Input":0, "Output":0, "Shuffle Read":0, "Shuffle Write":0, "Spill Memory":0, "Spill Disk":0, "Description": ""}
                            
                            stages[sid]["Tasks"] = stages[sid].get("Tasks", 0) + 1
                            
                            task_metrics = event.get("Task Metrics", {})
                            if task_metrics:
                                stages[sid]["Input"] = stages[sid].get("Input", 0) + task_metrics.get("Input Metrics", {}).get("Bytes Read", 0)
                                stages[sid]["Output"] = stages[sid].get("Output", 0) + task_metrics.get("Output Metrics", {}).get("Bytes Written", 0)
                                sr = task_metrics.get("Shuffle Read Metrics", {})
                                stages[sid]["Shuffle Read"] = stages[sid].get("Shuffle Read", 0) + sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0)
                                sw = task_metrics.get("Shuffle Write Metrics", {})
                                stages[sid]["Shuffle Write"] = stages[sid].get("Shuffle Write", 0) + sw.get("Shuffle Bytes Written", 0)
                                stages[sid]["Spill Memory"] = stages[sid].get("Spill Memory", 0) + task_metrics.get("Memory Bytes Spilled", 0)
                                stages[sid]["Spill Disk"] = stages[sid].get("Spill Disk", 0) + task_metrics.get("Disk Bytes Spilled", 0)

                except Exception:
                    continue
                    
    # Finalize
    result_list = []
    for sid, data in stages.items():
        # Duration
        sub = data.get("Submission Time", 0)
        comp = data.get("Completion Time", 0)
        if sub > 0 and comp > 0:
            data["Duration"] = (comp - sub) / 1000.0
        else:
            data["Duration"] = 0
        
        # Avg metrics
        tasks = data.get("Tasks", 0)
        if tasks > 0:
            data["Avg Input"] = data.get("Input", 0) / tasks
            data["Avg Shuffle Read"] = data.get("Shuffle Read", 0) / tasks
        else:
            data["Avg Input"] = 0
            data["Avg Shuffle Read"] = 0
            
        result_list.append(data)
        
    # Sort by ID
    sorted_stages = sorted(result_list, key=lambda x: x["Stage ID"])
    
    return {
        "appInfo": app_info,
        "stages": sorted_stages
    }

def main():
    parser = argparse.ArgumentParser(description='Spark Event Log Parser')
    parser.add_argument('--files', nargs='+', required=True, help='List of event log files')
    parser.add_argument('--output', required=True, help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Grouping Logic
    app_groups = {}
    for f in args.files:
        basename = os.path.basename(f)
        import re
        match = re.search(r"(application_\d+_\d+)", basename)
        if match:
            app_id_key = match.group(1)
            if app_id_key not in app_groups:
                app_groups[app_id_key] = []
            app_groups[app_id_key].append(f)
        else:
            app_groups[basename] = [f]
            
    results = []
    print(f"Analyzing {len(app_groups)} applications...")
    
    for app_key, files in app_groups.items():
        metrics = analyze_single_application(files)
        results.append(metrics)
        
    df = pd.DataFrame(results)
    
    # Reorder columns to match definitions (optional but good)
    desired_order = [
        "Start Date", "Application ID", "Application Name", "Duration (Sec)", "Total Input", "Total Output",
        "Driver Cores", "Driver Memory", "Executor Cores", "Executor Memory", "Executor Overhead Memory",
        "Requested Executors", "Max Active Executors", "Total Cores Capacity", "Total Memory Capacity",
        "Avg Idle Cores (%)", "Peak Memory Usage (%)", "Peak Heap Usage",
        "Total Shuffle Read", "Total Shuffle Write",
        "Max Shuffle Read (Stage)", "Max Shuffle Read Stage ID",
        "Max Shuffle Write (Stage)", "Max Shuffle Write Stage ID",
        "Max Shuffle Read (Job)", "Max Shuffle Read Job ID",
        "Max Shuffle Write (Job)", "Max Shuffle Write Job ID",
        "Total Spill (Memory)", "Total Spill (Disk)",
        "Max Spill Memory (Stage)", "Max Spill Stage ID"
    ]
    
    # Select only existing columns (avoid key errors depending on what happened)
    cols = [c for c in desired_order if c in df.columns]
    df = df[cols]
    
    df.to_csv(args.output, index=False)
    print(f"Analysis complete. Results saved to {args.output}")

if __name__ == "__main__":
    main()
