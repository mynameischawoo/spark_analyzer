import json
import glob
import os
import sys
import pandas as pd
import argparse
from typing import List, Dict, Any

import glob
import subprocess
import re

def get_log_lines(log_path):
    """Yields lines from log file, handling ZSTD rolling logs transparently.
    Also handles 'appstatus_' files by redirecting to the associated rolling logs."""
    
    dirname = os.path.dirname(log_path)
    basename = os.path.basename(log_path)
    
    # 1. Handle appstatus_ redirect
    if basename.startswith("appstatus_"):
        app_id = basename.replace("appstatus_", "")
        # Look for events_1_{app_id}*
        # We need to construct the search pattern for events.
        # events_*_{app_id}* covers events_1, events_2, etc.
        # Note: app_id is "application_..." usually.
        # Pattern: events_*_application_...
        
        # Check if we have events files
        pattern = os.path.join(dirname, f"events_*_{app_id}*")
        candidates = glob.glob(pattern)
        
        if candidates:
            # We found rolling logs!
            def get_index(f):
                fname = os.path.basename(f)
                m = re.match(r"events_(\d+)_", fname)
                return int(m.group(1)) if m else 999999
            
            sorted_files = sorted(candidates, key=get_index)
            
            # Decide if we need zstd
            # If any file ends with .zstd or .zst, use zstd -dc for all?
            # Usually they are consistent.
            use_zstd = any(f.endswith(".zstd") or f.endswith(".zst") for f in sorted_files)
            
            if use_zstd:
                cmd = ["zstd", "-dc"] + sorted_files
                print(f"Decoding rolling logs (via appstatus): {cmd}")
                try:
                    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    for line in process.stdout: yield line
                    process.wait()
                except Exception as e:
                    print(f"Error streaming zstd: {e}")
            else:
                 # Concatenate plain text files
                 for fpath in sorted_files:
                     with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                         for line in f: yield line
            return # Done with appstatus redirect

    # 2. Existing Direct ZSTD logic
    if log_path.endswith(".zstd") and "events_" in basename:
         match = re.match(r"events_\d+_(.+)\.zstd", basename)
         if match:
             app_id_part = match.group(1)
             pattern = os.path.join(dirname, f"events_*_{app_id_part}.zstd")
             candidates = glob.glob(pattern)
             def get_index(f):
                 fname = os.path.basename(f)
                 m = re.match(r"events_(\d+)_", fname)
                 return int(m.group(1)) if m else 999999
             sorted_files = sorted(candidates, key=get_index)
             cmd = ["zstd", "-dc"] + sorted_files
             print(f"Decoding rolling logs: {cmd}")
             try:
                 process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                 for line in process.stdout: yield line
                 process.wait()
                 if process.returncode != 0:
                      print(f"zstd processing error: {process.stderr.read()}")
             except Exception as e:
                 print(f"Error streaming zstd: {e}")
         else:
             cmd = ["zstd", "-dc", log_path]
             try:
                 process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                 for line in process.stdout: yield line
             except: pass
    else:
         with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
             for line in f: yield line

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
    
    # Task Counts
    total_tasks = 0
    preempted_tasks = 0

    # Executor Counts
    preempted_executors = 0
    preempted_executor_ids = set()
    
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
        if True: # Indentation shim to match original 'with open' level
            lines_gen = get_log_lines(file_path)
            for line in lines_gen:
                try:
                    event = json.loads(line)
                    event_type = event.get("Event")
                    ts = event.get("Timestamp", 0) 
                    
                    if event_type == "SparkListenerApplicationStart":
                        app_id = event.get("App ID", app_id)
                        app_name = event.get("App Name", app_name)
                        app_start_time = event.get("Timestamp", 0)
                        if "Timestamp" in event:
                             executor_time_series.append({"time": event["Timestamp"], "count": 0})
                        
                    elif event_type == "SparkListenerApplicationEnd":
                        app_end_time = event.get("Timestamp", 0)
                        
                    elif event_type == "SparkListenerEnvironmentUpdate":
                        spark_props = event.get("Spark Properties", {})
                        driver_memory = parse_memory(spark_props.get("spark.driver.memory", "0"))
                        driver_cores = int(spark_props.get("spark.driver.cores", 0))
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
                        
                        # Check for preemption
                        # User specified "Removed Reason" key for SparkListenerExecutorRemoved
                        # e.g. {"Event":"SparkListenerExecutorRemoved", ..., "Removed Reason":"... was preempted."}
                        # We also keep "Reason" as fallback.
                        raw_reason = event.get("Removed Reason") or event.get("Reason")
                        reason_str = str(raw_reason).lower() if raw_reason else ""
                        
                        if "was preempted" in reason_str or "preempt" in reason_str:
                            preempted_executors += 1
                            preempted_executor_ids.add(exec_id)

                    elif event_type == "SparkListenerTaskEnd":
                        task_info = event.get("Task Info", {})
                        task_metrics = event.get("Task Metrics", {})
                        
                        # Process Task Count and Duration (Available in Task Info)
                        total_tasks += 1
                        duration = task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0)
                        total_task_time_ms += duration

                        # Check Task End Reason for Preemption
                        task_end_reason = event.get("Task End Reason", {})
                        is_preempted = False
                        
                        # 1. Check Task End Reason keys explicitly
                        if isinstance(task_end_reason, dict):
                            # ExecutorLostFailure -> Loss Reason
                            loss_reason = str(task_end_reason.get("Loss Reason", "")).lower()
                            if "was preempted" in loss_reason or "preempt" in loss_reason:
                                is_preempted = True
                            
                            # TaskKilled -> Kill Reason (fallback)
                            if not is_preempted:
                                kill_reason = str(task_end_reason.get("Kill Reason", "")).lower()
                                if "preempt" in kill_reason:
                                    is_preempted = True
                        
                        # 2. Global String Check (Backup)
                        if not is_preempted:
                            full_reason_str = str(task_end_reason).lower()
                            if "was preempted" in full_reason_str or "preempt" in full_reason_str:
                                is_preempted = True
                        
                        # 3. Correlation with Preempted Executor
                        if not is_preempted:
                            task_exec_id = task_info.get("Executor ID")
                            if task_exec_id in preempted_executor_ids:
                                # Safe check for "Reason"
                                reason_val = task_end_reason.get("Reason") if isinstance(task_end_reason, dict) else str(task_end_reason)
                                if reason_val != "Success":
                                    is_preempted = True
                                    
                        if is_preempted:
                             preempted_tasks += 1
                        
                        if not task_metrics:
                             continue
                        
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
        "Max Spill Memory (Stage)": max_spill_mem_stage,
        "Max Spill Stage ID": max_spill_stage_id,
        "Total Tasks": total_tasks,
        "Preempted Tasks": preempted_tasks,
        "Preempted Executors": preempted_executors
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
    
    # Executor Time Series
    executor_time_series = [] # List of {"time": timestamp, "count": N}
    current_executors = 0
    
    # SQL & Accumulators
    sql_executions = {}
    accumulators = {}
    stages_to_execution = {}
    
    for file_path in files:
        if True: # Indentation shim to match original 'with open' level
            lines_gen = get_log_lines(file_path)
            for line in lines_gen:
                try:
                    event = json.loads(line)
                    event_type = event.get("Event")
                    ts = event.get("Timestamp", 0) 
                    
                    if event_type == "SparkListenerApplicationStart":
                        app_info["id"] = event.get("App ID", app_info["id"])
                        app_info["name"] = event.get("App Name", app_info["name"])
                        # Initialize series at start
                        if "Timestamp" in event:
                             executor_time_series.append({"time": event["Timestamp"], "count": 0})

                    elif event_type == "SparkListenerExecutorAdded":
                        current_executors += 1
                        if "Timestamp" in event:
                            executor_time_series.append({"time": event["Timestamp"], "count": current_executors})
                            
                    elif event_type == "SparkListenerExecutorRemoved":
                        current_executors = max(0, current_executors - 1)
                        if "Timestamp" in event:
                            executor_time_series.append({"time": event["Timestamp"], "count": current_executors})

                    elif event_type == "SparkListenerStageSubmitted":
                        info = event.get("Stage Info", {})
                        sid = info.get("Stage ID")
                        if sid is not None:
                            # Try to find description
                            desc = info.get("Stage Name", "")
                            props = event.get("Properties", {})
                            if not props:
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
                                stages[sid]["Input Records"] = stages[sid].get("Input Records", 0) + task_metrics.get("Input Metrics", {}).get("Records Read", 0)
                                stages[sid]["Output"] = stages[sid].get("Output", 0) + task_metrics.get("Output Metrics", {}).get("Bytes Written", 0)
                                stages[sid]["Output Records"] = stages[sid].get("Output Records", 0) + task_metrics.get("Output Metrics", {}).get("Records Written", 0)
                                sr = task_metrics.get("Shuffle Read Metrics", {})
                                stages[sid]["Shuffle Read"] = stages[sid].get("Shuffle Read", 0) + sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0)
                                sw = task_metrics.get("Shuffle Write Metrics", {})
                                stages[sid]["Shuffle Write"] = stages[sid].get("Shuffle Write", 0) + sw.get("Shuffle Bytes Written", 0)
                                stages[sid]["Spill Memory"] = stages[sid].get("Spill Memory", 0) + task_metrics.get("Memory Bytes Spilled", 0)
                                stages[sid]["Spill Disk"] = stages[sid].get("Spill Disk", 0) + task_metrics.get("Disk Bytes Spilled", 0)
                            
                            # Accumulator Updates from Task
                            task_accum = task_metrics.get("Accumulators", [])
                            if not task_accum:
                                # Sometimes it's directly under TaskInfo or Accumulables
                                # But standard JSON has "Task Metrics" -> "Accumulators" or top level "Task Info" -> "Accumulables"
                                # Let's check top level accumulables for accuracy
                                pass
                                
                            # 'Accumulables' is often at top level of SparkListenerTaskEnd in some versions, or inside TaskInfo
                            accumulables = event.get("Task Info", {}).get("Accumulables", [])
                            if not accumulables: 
                                # Fallback to standard location if any
                                accumulables = task_metrics.get("Accumulators", []) # Older spark?
                                
                            for acc in accumulables:
                                try:
                                    aid = int(acc.get("ID"))
                                    # We need to sum up values. 
                                    # NOTE: Some accumulators are "internal.metrics..." and might need summing.
                                    # Value can be string for some types, but mostly numbers for metrics.
                                    val_str = acc.get("Update", "0")
                                    try:
                                        val = float(val_str) if '.' in val_str else int(val_str)
                                    except:
                                        val = 0
                                        
                                    if aid in accumulators:
                                        accumulators[aid] = accumulators[aid] + val
                                    else:
                                        accumulators[aid] = val
                                except:
                                    pass

                                    pass

                    elif event_type.endswith("SparkListenerSQLExecutionStart"):
                        exec_id = event.get("executionId")
                        desc = event.get("description", "")
                        plan_info = event.get("sparkPlanInfo", {})
                        time = event.get("time", 0)
                        
                        nodes = {} # nodeId -> {name, metrics: {name: accumId}}
                        
                        def parse_plan_info(node):
                            # ... (same logic)
                            
                            node_data = {
                                "nodeName": node.get("nodeName", "Unknown"),
                                "simpleString": node.get("simpleString", ""),
                                "metrics": [] # List of {name, accumulatorId}
                            }
                            
                            for m in node.get("metrics", []):
                                node_data["metrics"].append({
                                    "name": m.get("name"),
                                    "accumulatorId": m.get("accumulatorId")
                                })
                            
                            children = []
                            for child in node.get("children", []):
                                children.append(parse_plan_info(child))
                            
                            node_data["children"] = children
                            return node_data

                        if exec_id is not None:
                            try:
                                root_node = parse_plan_info(plan_info)
                                sql_executions[exec_id] = {
                                    "executionId": exec_id,
                                    "description": desc,
                                    "startTime": time,
                                    "plan": root_node, # Tree structure
                                    "jobs": [] # List of Job IDs
                                }
                            except Exception as e:
                                print(f"Error parsing plan: {e}")

                    elif event_type.endswith("SparkListenerJobStart"):
                        job_id = event.get("Job ID")
                        props = event.get("Properties", {})
                        exec_id_str = props.get("spark.sql.execution.id")
                        if exec_id_str and job_id is not None:
                            try:
                                exec_id = int(exec_id_str)
                                if exec_id in sql_executions:
                                    sql_executions[exec_id]["jobs"].append(job_id)
                                    
                                # Map stages to this execution
                                stage_ids = event.get("Stage IDs", [])
                                for sid in stage_ids:
                                    stages_to_execution[sid] = exec_id
                            except:
                                pass

                    elif event_type.endswith("SparkListenerDriverAccumUpdates"):
                        # Driver side updates
                        exec_id = event.get("executionId")
                        updates = event.get("accumUpdates", [])
                        for update in updates:
                            # [accumulatorId, value]
                            if len(update) >= 2:
                                aid = int(update[0])
                                val = update[1] # Can be int
                                # Driver updates REPLACING the value often wipes out Task updates if driver sends 0.
                                # Use max to preserve aggregated counters.
                                curr = accumulators.get(aid, 0)
                                if isinstance(val, (int, float)) and isinstance(curr, (int, float)):
                                     accumulators[aid] = max(curr, val)
                                else:
                                     accumulators[aid] = val

                except Exception:
                    continue
                    
    # Finalize
    stage_list = sorted(stages.values(), key=lambda x: int(x["Stage ID"]))
    
    # Add aggregated metrics to sql executions if needed (already done in parser logic?)
    # The parser logic captured nodes and metrics.
    
    return {
        "appId": app_info["id"],
        "appName": app_info["name"],
        "stages": stage_list,
        "sqlExecutions": sorted(sql_executions.values(), key=lambda x: x["startTime"]),
        "executorTimeSeries": executor_time_series
    }

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse Spark Event Logs")
    # Support both positional and flagged arguments to handle web_app calling convention
    parser.add_argument("pos_files", nargs="*", help="Log files to parse (positional)")
    parser.add_argument("--files", nargs="+", help="Log files to parse (flag)")
    parser.add_argument("--output", help="Output CSV file path for summary analysis")
    
    args = parser.parse_args()
    
    # Prioritize --files, fallback to positional
    files = args.files if args.files else args.pos_files
    
    if not files:
        parser.print_help()
        sys.exit(1)
    
    if args.output:
        # Summary Analysis Mode (for Web App Summary)
        # Iterate over each file and treat it as a separate application analysis
        results = []
        try:
            for f in files:
                # analyze_single_application now expects a list of files for A SINGLE app.
                # Since the UI passes main entry points (app file or events_1), we pass [f].
                # get_log_lines handles finding siblings for rolling logs.
                try:
                    metrics = analyze_single_application([f])
                    results.append(metrics)
                except Exception as e:
                    print(f"Error analyzing {f}: {e}")
                    # Add a partial result or skip?
                    # Let's add a placeholder to show error in CSV
                    results.append({
                        "Application ID": os.path.basename(f),
                        "Application Name": "Error: " + str(e),
                        "Start Date": "Error"
                    })

            if not results:
                print("No results generated.")
                sys.exit(1)

            # Convert to DataFrame and save
            df = pd.DataFrame(results)
            # Ensure directory exists
            out_dir = os.path.dirname(args.output)
            if out_dir and not os.path.exists(out_dir):
                os.makedirs(out_dir)
            
            df.to_csv(args.output, index=False)
            print(f"Summary analysis saved to {args.output}")
        except Exception as e:
            print(f"Error during summary analysis: {e}")
            sys.exit(1)
            
    else:
        # Detailed Analysis Mode (Standard CLI or Debug)
        # If multiple files are passed here (e.g. app and events), it might be intended as one app?
        # But for 'analyze_stage_details', usually it's one app's files.
        # If the UI calls this for "Detail Analysis", it passes a single file (or list of siblings).
        # We'll leave this as is for now, assuming detail view calls with one app's context.
        data = analyze_stage_details(files)
        print(json.dumps(data, indent=2, default=str))
