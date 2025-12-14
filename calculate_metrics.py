import json
import glob
import os
import re
import pandas as pd

def format_bytes(size):
    power = 2**10
    n = size
    power_labels = {0 : '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    count = 0
    while n > power:
        n /= power
        count += 1
    return f"{n:.2f} {power_labels.get(count, 'B')}"

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

def analyze_application(app_id, files):
    files = sorted(files)
    
    # Metrics
    app_name = "Unknown"
    real_app_id = app_id # Default to grouping ID
    
    executors = {} 
    current_executors = 0
    max_concurrent_executors = 0
    
    total_task_time_ms = 0
    peak_execution_memory = 0
    peak_jvm_heap = 0
    
    driver_cores = 0
    driver_memory = 0
    executor_cores = 0
    executor_memory = 0
    executor_memory_overhead = 0
    executor_instances = 0
    dyn_alloc_enabled = False
    dyn_alloc_max_execs = 0
    
    app_start_time = 0
    app_end_time = 0
    total_input_bytes = 0
    total_output_bytes = 0
    total_memory_spill = 0
    total_disk_spill = 0
    
    # Shuffle tracking
    total_shuffle_read_bytes = 0
    total_shuffle_write_bytes = 0
    stage_shuffle_read = {} # {stage_id: bytes}
    stage_shuffle_write = {} # {stage_id: bytes}
    
    # Spill tracking per stage
    stage_spill_memory = {}
    stage_spill_disk = {}
    stage_to_job = {}
    
    input_info = {} 
    output_info = {}
    
    for file_path in files:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("Event")
                    
                    if event_type == "SparkListenerApplicationStart":
                        app_start_time = event.get("Timestamp", 0)
                        app_name = event.get("App Name", "Unknown")
                        real_app_id = event.get("App ID", real_app_id)
                        
                    elif event_type == "SparkListenerJobStart":
                        job_id = event.get("Job ID")
                        stage_ids = event.get("Stage IDs", [])
                        for sid in stage_ids:
                            stage_to_job[sid] = job_id
                        
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
                            executor_memory_overhead = parse_memory(overhead)
                        else:
                            executor_memory_overhead = max(int(executor_memory * 0.10), 384 * 1024 * 1024)
                        executor_instances = int(spark_props.get("spark.executor.instances", 0))
                        dyn_alloc_enabled = spark_props.get("spark.dynamicAllocation.enabled", "false") == "true"
                        dyn_alloc_max_execs = int(spark_props.get("spark.dynamicAllocation.maxExecutors", 0))

                    elif event_type == "SparkListenerExecutorAdded":
                        exec_id = event.get("Executor ID")
                        if exec_id != "driver":
                            current_executors += 1
                            if current_executors > max_concurrent_executors:
                                max_concurrent_executors = current_executors
                        info = event.get("Executor Info", {})
                        executors[exec_id] = {
                            "start_time": event.get("Timestamp"),
                            "end_time": None,
                            "cores": info.get("Total Cores", 1)
                        }
                        
                    elif event_type == "SparkListenerExecutorRemoved":
                        exec_id = event.get("Executor ID")
                        if exec_id != "driver":
                            current_executors -= 1
                        if exec_id in executors:
                            executors[exec_id]["end_time"] = event.get("Timestamp")
                            
                    elif event_type == "SparkListenerTaskEnd":
                        task_info = event.get("Task Info", {})
                        task_metrics = event.get("Task Metrics", {})
                        duration = task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0)
                        total_task_time_ms += duration
                        peak_mem = task_metrics.get("Peak Execution Memory", 0)
                        if peak_mem > peak_execution_memory:
                            peak_execution_memory = peak_mem
                        task_exec_metrics = event.get("Task Executor Metrics", {})
                        heap_mem = task_exec_metrics.get("JVMHeapMemory", 0)
                        if heap_mem > peak_jvm_heap:
                            peak_jvm_heap = heap_mem
                        input_metrics = task_metrics.get("Input Metrics", {})
                        total_input_bytes += input_metrics.get("Bytes Read", 0)
                        output_metrics = task_metrics.get("Output Metrics", {})
                        total_output_bytes += output_metrics.get("Bytes Written", 0)
                        total_memory_spill += task_metrics.get("Memory Bytes Spilled", 0)
                        total_disk_spill += task_metrics.get("Disk Bytes Spilled", 0)
                        
                        stage_id = event.get("Stage ID")
                        if stage_id is not None:
                            mem_spill = task_metrics.get("Memory Bytes Spilled", 0)
                            disk_spill = task_metrics.get("Disk Bytes Spilled", 0)
                            if mem_spill > 0:
                                stage_spill_memory[stage_id] = stage_spill_memory.get(stage_id, 0) + mem_spill
                            if disk_spill > 0:
                                stage_spill_disk[stage_id] = stage_spill_disk.get(stage_id, 0) + disk_spill
                        
                        # Shuffle Metrics
                        shuffle_read_metrics = task_metrics.get("Shuffle Read Metrics", {})
                        shuffle_read = shuffle_read_metrics.get("Remote Bytes Read", 0) + shuffle_read_metrics.get("Local Bytes Read", 0)
                        total_shuffle_read_bytes += shuffle_read
                        
                        shuffle_write_metrics = task_metrics.get("Shuffle Write Metrics", {})
                        shuffle_write = shuffle_write_metrics.get("Shuffle Bytes Written", 0)
                        total_shuffle_write_bytes += shuffle_write
                        
                        stage_id = event.get("Stage ID")
                        if stage_id is not None:
                            stage_shuffle_read[stage_id] = stage_shuffle_read.get(stage_id, 0) + shuffle_read
                            stage_shuffle_write[stage_id] = stage_shuffle_write.get(stage_id, 0) + shuffle_write

                    elif event_type == "SparkListenerExecutorMetricsUpdate":
                        exec_metrics = event.get("Executor Metrics", {})
                        heap_mem = exec_metrics.get("JVMHeapMemory", 0)
                        if heap_mem > peak_jvm_heap:
                            peak_jvm_heap = heap_mem
                            
                    elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                        desc = event.get("physicalPlanDescription", "")
                        scan_matches = re.finditer(r'Scan\s+(\w+).*?Location:.*?(hdfs://\S+|s3://\S+|file://\S+|viewfs://\S+)', desc, re.DOTALL)
                        for m in scan_matches:
                            fmt = m.group(1)
                            path = m.group(2).strip('",)]}')
                            if fmt not in input_info: input_info[fmt] = set()
                            input_info[fmt].add(path)
                        if "InsertInto" in desc or "Write" in desc or "SaveInto" in desc:
                            loc_matches = re.findall(r'Location:.*?(hdfs://\S+|s3://\S+|file://\S+|viewfs://\S+)', desc)
                            for l in loc_matches:
                                path = l.strip('",)]}')
                                if "Output" not in output_info: output_info["Output"] = set()
                                output_info["Output"].add(path)
                except:
                    continue

    # Calculations
    total_capacity_ms = 0
    for exec_id, data in executors.items():
        start = data["start_time"]
        end = data["end_time"] if data["end_time"] else app_end_time
        if end > start:
            duration = end - start
            total_capacity_ms += duration * data["cores"]

    idle_cores_pct = 0
    if total_capacity_ms > 0:
        idle_time_ms = total_capacity_ms - total_task_time_ms
        idle_cores_pct = (idle_time_ms / total_capacity_ms) * 100

    peak_mem_pct = 0
    if executor_memory > 0:
        peak_mem_pct = (peak_jvm_heap / executor_memory) * 100
        
    max_stage_shuffle_read = max(stage_shuffle_read.values()) if stage_shuffle_read else 0
    max_stage_shuffle_write = max(stage_shuffle_write.values()) if stage_shuffle_write else 0
    
    # Max Spill Calculations
    max_spill_mem_stage = max(stage_spill_memory, key=stage_spill_memory.get) if stage_spill_memory else None
    max_spill_mem_amount = stage_spill_memory[max_spill_mem_stage] if max_spill_mem_stage is not None else 0
    max_spill_mem_job = stage_to_job.get(max_spill_mem_stage, "Unknown") if max_spill_mem_stage is not None else "Unknown"
    
    max_spill_disk_stage = max(stage_spill_disk, key=stage_spill_disk.get) if stage_spill_disk else None
    max_spill_disk_amount = stage_spill_disk[max_spill_disk_stage] if max_spill_disk_stage is not None else 0
    max_spill_disk_job = stage_to_job.get(max_spill_disk_stage, "Unknown") if max_spill_disk_stage is not None else "Unknown"
        
    duration_sec = (app_end_time - app_start_time)/1000 if app_end_time > 0 else 0
    
    requested_executors = dyn_alloc_max_execs if dyn_alloc_enabled else executor_instances

    return {
        "App ID": real_app_id,
        "App Name": app_name,
        "Requested Executors": requested_executors,
        "Duration (s)": duration_sec,
        "Idle Cores (%)": idle_cores_pct,
        "Peak Mem (%)": peak_mem_pct,
        "Peak Heap": peak_jvm_heap,
        "Exec Mem": executor_memory,
        "Total Spill (Mem)": total_memory_spill,
        "Total Spill (Disk)": total_disk_spill,
        "Driver Cores": driver_cores,
        "Driver Mem": driver_memory,
        "Exec Cores": executor_cores,
        "Exec Overhead": executor_memory_overhead,
        "Max Concurrent Execs": max_concurrent_executors,
        "Total Cores (Peak)": max_concurrent_executors * executor_cores,
        "Total Mem (Peak)": max_concurrent_executors * (executor_memory + executor_memory_overhead),
        "Input Size": total_input_bytes,
        "Output Size": total_output_bytes,
        "Total Shuffle Read": total_shuffle_read_bytes,
        "Total Shuffle Write": total_shuffle_write_bytes,
        "Max Stage Shuffle Read": max_stage_shuffle_read,
        "Total Shuffle Write": total_shuffle_write_bytes,
        "Max Stage Shuffle Read": max_stage_shuffle_read,
        "Max Stage Shuffle Write": max_stage_shuffle_write,
        "Max Spill Mem Stage": max_spill_mem_stage,
        "Max Spill Mem Job": max_spill_mem_job,
        "Max Spill Mem Amount": max_spill_mem_amount,
        "Max Spill Disk Stage": max_spill_disk_stage,
        "Max Spill Disk Job": max_spill_disk_job,
        "Max Spill Disk Amount": max_spill_disk_amount,
        "Input Info": input_info,
        "Output Info": output_info
    }

def log_print(message, file_handle=None):
    print(message)
    if file_handle:
        file_handle.write(message + "\n")

def print_app_report(metrics, file_handle=None):
    log_print("=" * 120, file_handle)
    log_print(f"🚀 Application Report: {metrics['App Name']} ({metrics['App ID']})", file_handle)
    log_print("=" * 120, file_handle)
    
    log_print(f"Duration: {metrics['Duration (s)']:.2f} s", file_handle)
    log_print(f"Idle Cores: {metrics['Idle Cores (%)']:.2f}%", file_handle)
    log_print(f"Peak Memory Usage: {metrics['Peak Mem (%)']:.2f}% (Peak Heap: {format_bytes(metrics['Peak Heap'])} / {format_bytes(metrics['Exec Mem'])})", file_handle)
    log_print(f"Total Spill: Memory={format_bytes(metrics['Total Spill (Mem)'])} , Disk={format_bytes(metrics['Total Spill (Disk)'])}", file_handle)
    if metrics['Max Spill Mem Amount'] > 0:
        log_print(f"  Max Memory Spill: {format_bytes(metrics['Max Spill Mem Amount'])} (Job ID: {metrics['Max Spill Mem Job']}, Stage ID: {metrics['Max Spill Mem Stage']})", file_handle)
    if metrics['Max Spill Disk Amount'] > 0:
        log_print(f"  Max Disk Spill: {format_bytes(metrics['Max Spill Disk Amount'])} (Job ID: {metrics['Max Spill Disk Job']}, Stage ID: {metrics['Max Spill Disk Stage']})", file_handle)
    log_print(f"Shuffle Read: Total={format_bytes(metrics['Total Shuffle Read'])}, Max/Stage={format_bytes(metrics['Max Stage Shuffle Read'])}", file_handle)
    log_print(f"Shuffle Write: Total={format_bytes(metrics['Total Shuffle Write'])}, Max/Stage={format_bytes(metrics['Max Stage Shuffle Write'])}", file_handle)
    
    log_print("-" * 60, file_handle)
    log_print("Resource Configuration & Usage:", file_handle)
    log_print(f"Driver: {metrics['Driver Cores']} Cores, {format_bytes(metrics['Driver Mem'])} Memory", file_handle)
    log_print(f"Executor: {metrics['Exec Cores']} Cores, {format_bytes(metrics['Exec Mem'])} Memory, {format_bytes(metrics['Exec Overhead'])} Overhead", file_handle)
    log_print(f"Requested Executors: {metrics['Requested Executors']}", file_handle)
    log_print(f"Max Concurrent Executors: {metrics['Max Concurrent Execs']}", file_handle)
    log_print(f"Total Resources (Peak): {metrics['Total Cores (Peak)']} Cores, {format_bytes(metrics['Total Mem (Peak)'])} Memory", file_handle)

    log_print("-" * 60, file_handle)
    log_print("Input Data:", file_handle)
    if metrics['Input Size'] > 0:
        log_print(f"  Total Size: {format_bytes(metrics['Input Size'])}", file_handle)
    else:
        log_print("  Total Size: 0 B", file_handle)
        
    for fmt, paths in metrics['Input Info'].items():
        log_print(f"  Format: {fmt}", file_handle)
        for p in paths:
            log_print(f"    - {p}", file_handle)
            
    log_print("-" * 60, file_handle)
    log_print("Output Data:", file_handle)
    if metrics['Output Size'] > 0:
        log_print(f"  Total Size: {format_bytes(metrics['Output Size'])}", file_handle)
    else:
        log_print("  Total Size: 0 B", file_handle)
        
    for fmt, paths in metrics['Output Info'].items():
        for p in paths:
            log_print(f"    - {p}", file_handle)
    log_print("\n", file_handle)

def main(directory):
    # Group files by application ID
    # Pattern: application_TIMESTAMP_ID_ATTEMPT or just application_TIMESTAMP_ID
    # We'll assume files starting with application_ belong to some app.
    # We need to group them.
    # application_1758091571353_25638135_1 matches ..._part_001
    
    all_files = sorted(glob.glob(os.path.join(directory, "application_*")))
    app_groups = {}
    
    for f in all_files:
        basename = os.path.basename(f)
        # Regex to extract the base application ID part
        # Matches: application_123_456_1  (and ignores _part_XX)
        match = re.match(r"(application_\d+_\d+(?:_\d+)?)", basename)
        if match:
            app_id = match.group(1)
            if app_id not in app_groups:
                app_groups[app_id] = []
            app_groups[app_id].append(f)
            
    if not app_groups:
        print("No application logs found.")
        return

    results = []
    
    results = []
    report_file = None
    
    print(f"Found {len(app_groups)} applications. Analyzing...\n")
    
    try:
        for app_id, files in app_groups.items():
            metrics = analyze_application(app_id, files)
            results.append(metrics)
            
            if report_file is None:
                # Determine filename from first app
                app_name = metrics.get("App Name", "Unknown")
                # Remove date pattern like -2025-11-29
                sanitized_name = re.sub(r"-\d{4}-\d{2}-\d{2}", "", app_name)
                filename = f"metrics_report_{sanitized_name}.txt"
                output_file_path = os.path.join(directory, filename)
                print(f"Writing report to: {output_file_path}\n")
                report_file = open(output_file_path, 'w', encoding='utf-8')
            
            print_app_report(metrics, report_file)
            
        # Create DataFrame
        df_data = []
        for m in results:
            row = {
                "App ID": m['App ID'],
                "App Name": m['App Name'],
                "Duration (s)": m['Duration (s)'],
                "Idle Cores (%)": m['Idle Cores (%)'],
                "Peak Mem (%)": m['Peak Mem (%)'],
                "Peak Heap": format_bytes(m['Peak Heap']),
                "Total Spill (Mem)": format_bytes(m['Total Spill (Mem)']),
                "Total Spill (Disk)": format_bytes(m['Total Spill (Disk)']),
                "Max Spill Mem": f"{format_bytes(m['Max Spill Mem Amount'])} (J:{m['Max Spill Mem Job']} S:{m['Max Spill Mem Stage']})",
                "Max Spill Disk": f"{format_bytes(m['Max Spill Disk Amount'])} (J:{m['Max Spill Disk Job']} S:{m['Max Spill Disk Stage']})",
                "Shuffle Read": format_bytes(m['Total Shuffle Read']),
                "Shuffle Write": format_bytes(m['Total Shuffle Write']),
                "Max Stage Read": format_bytes(m['Max Stage Shuffle Read']),
                "Max Stage Write": format_bytes(m['Max Stage Shuffle Write']),
                "Requested Executors": m['Requested Executors'],
                "Max Executors": m['Max Concurrent Execs'],
                "Total Cores": m['Total Cores (Peak)'],
                "Total Mem": format_bytes(m['Total Mem (Peak)']),
                "Input Size": format_bytes(m['Input Size']),
                "Output Size": format_bytes(m['Output Size']),
                "Driver Cores": m['Driver Cores'],
                "Driver Memory": format_bytes(m['Driver Mem']),
                "Executor Cores": m['Exec Cores'],
                "Executor Memory": format_bytes(m['Exec Mem']),
                "Executor Overhead": format_bytes(m['Exec Overhead'])
            }
            df_data.append(row)
            
        df = pd.DataFrame(df_data)
        df = df.sort_values(by="App Name", ascending=True)
        
        if report_file:
            log_print("=" * 60, report_file)
            log_print("Summary DataFrame:", report_file)
            log_print("=" * 60, report_file)
            log_print(df.to_string(index=False), report_file)
            
    finally:
        if report_file:
            report_file.close()

if __name__ == "__main__":
    base_dir = "/Users/user/NaverCorp/oss/DnA/0_jmkim/shs_logs"
    
    target_dirs = []
    for root, dirs, files in os.walk(base_dir):
        # Check if there are any application logs in this directory
        if any(f.startswith("application_") for f in files):
            target_dirs.append(root)
            
    if not target_dirs:
        print(f"No directories with application logs found in {base_dir}")
    else:
        print(f"Found {len(target_dirs)} directories to process.")
        for target_dir in sorted(target_dirs):
            print(f"\n{'#'*80}")
            print(f"Processing Directory: {target_dir}")
            print(f"{'#'*80}\n")
            main(target_dir)
