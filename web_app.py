from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import os
import glob
import json
import subprocess
import datetime
import pandas as pd
import zipfile
import tempfile
import shutil
from typing import List

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(BASE_DIR, "event_logs")
STATIC_DIR = os.path.join(BASE_DIR, "static")
OUTPUT_CSV = os.path.join(BASE_DIR, "latest_analysis_result.csv")

# Ensure directories exist
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

class AnalyzeRequest(BaseModel):
    files: List[str]

@app.get("/api/config")
async def get_config():
    """Returns application configuration."""
    return {
        "shs_url": os.environ.get("SHS_URL", "")
    }

@app.get("/")
async def read_index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

@app.get("/api/logs")
async def list_logs():
    """Lists application logs in the event_logs directory."""
    if not os.path.exists(LOGS_DIR):
        return []
    
    # Include standard application_* and appstatus_application_*
    # We display appstatus files as "application_..." by stripping the prefix
    files_std = glob.glob(os.path.join(LOGS_DIR, "application_*"))
    files_rolling = glob.glob(os.path.join(LOGS_DIR, "appstatus_application_*"))
    
    logs = []
    
    # Process standard files
    for f in files_std:
        fname = os.path.basename(f)
        # Skip if it's actually an appstatus file (glob overlap prevention if pattern changes, though "application_*" shouldn't match "appstatus_")
        if fname.startswith("appstatus_"): continue 
        
        mtime = os.path.getmtime(f)
        dt = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
        logs.append({
            "filename": fname,
            "date": dt,
            "path": f
        })

    # Process rolling log anchors (appstatus)
    for f in files_rolling:
        fname = os.path.basename(f)
        # Display name: remove 'appstatus_'
        display_name = fname.replace("appstatus_", "")
        
        mtime = os.path.getmtime(f)
        dt = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
        logs.append({
            "filename": display_name, # Virtual name
            "date": dt,
            "path": f # Real path
        })
        
    # Sort by date desc
    logs.sort(key=lambda x: x["date"], reverse=True)
    return logs

def resolve_real_path(fname):
    """Resolves a filename from request to physical path, handling appstatus virtualization."""
    # 1. Try direct match
    p = os.path.join(LOGS_DIR, fname)
    if os.path.exists(p):
        return p
    
    # 2. Try adding appstatus_ prefix
    p_status = os.path.join(LOGS_DIR, "appstatus_" + fname)
    if os.path.exists(p_status):
        return p_status
        
    return None

@app.delete("/api/logs")
async def delete_logs(request: AnalyzeRequest):
    """Deletes selected log files."""
    deleted_count = 0
    errors = []
    for fname in request.files:
        if ".." in fname or "/" in fname:
            errors.append(f"{fname}: Invalid filename")
            continue

        real_path = resolve_real_path(fname)
        
        if real_path and os.path.exists(real_path):
            try:
                # If it's an appstatus file, delete group
                basename = os.path.basename(real_path)
                if basename.startswith("appstatus_"):
                    app_id = basename.replace("appstatus_", "")
                    # Delete appstatus file
                    os.remove(real_path)
                    # Delete siblings events_*_APPID* (zstd or others)
                    # Be careful not to delete standard application_APPID if app_id matches exactly?
                    # Usually IDs are unique.
                    # Pattern for rolling logs: events_*_APPID...
                    for sib in glob.glob(os.path.join(LOGS_DIR, f"events_*_{app_id.replace('application_', '')}*")):
                         try: os.remove(sib)
                         except: pass
                else:
                    # Standard file
                    os.remove(real_path)
                    
                deleted_count += 1
            except Exception as e:
                errors.append(f"{fname}: {str(e)}")
        else:
             errors.append(f"{fname}: Not found")
    
    return {"status": "success", "deleted": deleted_count, "errors": errors}

@app.post("/api/upload")
async def upload_logs(files: List[UploadFile] = File(...)):
    """Uploads new log files. Supports ZIP files which are automatically extracted."""
    saved_count = 0
    
    def is_spark_log_file(filename):
        """Check if a filename matches Spark log patterns."""
        basename = os.path.basename(filename)
        return (
            basename.startswith("application_") or
            basename.startswith("appstatus_application_") or
            basename.startswith("events_") or
            basename.startswith("eventlog_v2_")
        )
    
    def extract_zip_file(zip_content, original_filename):
        """Extract ZIP file and move Spark log files to LOGS_DIR."""
        extracted_count = 0
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, original_filename)
            
            # Write ZIP to temp
            with open(zip_path, "wb") as f:
                f.write(zip_content)
            
            # Extract
            extract_dir = os.path.join(temp_dir, "extracted")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Walk extracted files and move matching ones
            for root, dirs, extracted_files in os.walk(extract_dir):
                for ef in extracted_files:
                    # Skip hidden files
                    if ef.startswith('.'):
                        continue
                    
                    src_path = os.path.join(root, ef)
                    
                    # Check if it's a Spark log file pattern
                    if is_spark_log_file(ef):
                        dest_path = os.path.join(LOGS_DIR, ef)
                        shutil.copy2(src_path, dest_path)
                        extracted_count += 1
                        print(f"Extracted: {ef}")
        
        return extracted_count
    
    for file in files:
        try:
            # Basic validation
            filename = file.filename
            if ".." in filename:
                continue
            
            # Handle path separators for files from different OS
            basename = os.path.basename(filename)
            
            content = await file.read()
            
            # Check if it's a ZIP file
            if basename.lower().endswith('.zip'):
                count = extract_zip_file(content, basename)
                saved_count += count
            else:
                # Regular file upload
                path = os.path.join(LOGS_DIR, basename)
                with open(path, "wb") as buffer:
                    buffer.write(content)
                saved_count += 1
                
        except Exception as e:
            print(f"Failed to upload {file.filename}: {e}")
            import traceback
            traceback.print_exc()
            
    return {"status": "success", "saved": saved_count}

def get_app_name_from_file(filepath):
    """Reads the beginning of the file to find SparkListenerApplicationStart."""
    # If appstatus file, redirect to events_1
    basename = os.path.basename(filepath)
    if basename.startswith("appstatus_"):
        # appstatus_application_ID
        # Extract ID part properly. "appstatus_application_123" -> ID "application_123"
        # events files are "events_X_application_123..."
        # So we need the "application_..." part.
        app_id_full = basename.replace("appstatus_", "") 
        # events file name format: events_1_application_123...
        # so we search for events_1_{app_id_full}*
        
        dirname = os.path.dirname(filepath)
        pattern = os.path.join(dirname, f"events_1_{app_id_full}*")
        candidates = glob.glob(pattern)
        if candidates:
            filepath = candidates[0]

    try:
        if filepath.endswith(".zstd") or filepath.endswith(".zst"):
            # Use zstd -dc to stream
            cmd = ["zstd", "-dc", filepath]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                for _ in range(5000):
                    line = process.stdout.readline()
                    if not line: break
                    try:
                        event = json.loads(line)
                        if event.get("Event") == "SparkListenerApplicationStart":
                            process.terminate()
                            return event.get("App Name", "Unknown")
                    except:
                        continue
            finally:
                if process.poll() is None:
                    process.terminate()
        else:
            with open(filepath, 'r', encoding='utf-8') as f:
                for _ in range(5000): # Check first 5000 lines to find App Name
                    line = f.readline()
                    if not line: break
                    try:
                        event = json.loads(line)
                        if event.get("Event") == "SparkListenerApplicationStart":
                            return event.get("App Name", "Unknown")
                    except:
                        continue
    except Exception:
        return "Read Error"
    return "Unknown"

class NameRequest(BaseModel):
    files: List[str]

@app.get("/.well-known/appspecific/com.chrome.devtools.json")
async def chrome_devtools_silencer():
    return {}

@app.post("/api/extract-names")
async def extract_names(request: NameRequest):
    """Returns app names for requested files."""
    results = {}
    for fname in request.files:
        real_path = resolve_real_path(fname)
        if real_path and os.path.exists(real_path):
            results[fname] = get_app_name_from_file(real_path)
        else:
            results[fname] = "File Not Found"
    return results

from spark_log_parser import analyze_stage_details

@app.post("/api/analyze")
async def analyze_logs(request: AnalyzeRequest):
    """Triggers the python analysis script."""
    if not request.files:
        raise HTTPException(status_code=400, detail="No files selected")
    
    # Resolve real paths
    target_files = []
    for f in request.files:
        rp = resolve_real_path(f)
        if rp: target_files.append(rp)
        else: print(f"Warning: Could not resolve {f}")
    
    if not target_files:
         raise HTTPException(status_code=400, detail="No valid files found")

    # Run script
    cmd = ["python3", "spark_log_parser.py", "--output", OUTPUT_CSV, "--files"] + target_files
    
    try:
        # Run in CWD = BASE_DIR
        result = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise HTTPException(status_code=500, detail=f"Analysis failed: {result.stderr}")
            
        return {"status": "success", "message": "Analysis complete"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/analyze/detail")
async def analyze_detail(request: AnalyzeRequest):
    """Analyzes a single application to return stage details."""
    input_files = []
    for fname in request.files:
        rp = resolve_real_path(fname)
        if rp: input_files.append(rp)
    
    if not input_files:
        raise HTTPException(status_code=400, detail="No valid files selected")
        
    try:
        # Dynamic import and reload to pick up latest parser changes without restart
        import importlib
        import spark_log_parser
        importlib.reload(spark_log_parser)
        
        # input_files contains the real paths (e.g. appstatus_...)
        # spark_log_parser should handle them
        details = spark_log_parser.analyze_stage_details(input_files)
        
        import read_analyzer
        import write_analyzer
        import importlib
        importlib.reload(read_analyzer)
        importlib.reload(write_analyzer)
        
        reads = read_analyzer.analyze_read_metrics(input_files)
        writes = write_analyzer.analyze_write_metrics(input_files)
        
        if isinstance(details, dict):
            details['reads'] = reads
            details['writes'] = writes
            
        return details
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/results/recent")
async def get_results():
    """Returns the content of the latest CSV."""
    if not os.path.exists(OUTPUT_CSV):
        raise HTTPException(status_code=404, detail="No analysis results found")
    
    # Read CSV and return as JSON for easier frontend rendering
    try:
        df = pd.read_csv(OUTPUT_CSV)
        # Handle NaN
        df = df.fillna("")
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")

class DeleteResultsRequest(BaseModel):
    app_ids: List[str]

@app.delete("/api/results")
async def delete_results(request: DeleteResultsRequest):
    """Deletes rows from the analysis results CSV and removes corresponding log files."""
    if not os.path.exists(OUTPUT_CSV):
        raise HTTPException(status_code=404, detail="No analysis results found")
    
    try:
        df = pd.read_csv(OUTPUT_CSV)
        
        # Check if Application ID column exists
        if "Application ID" not in df.columns:
             raise HTTPException(status_code=400, detail="CSV does not contain 'Application ID' column")

        original_count = len(df)
        
        # Filter out rows to be deleted
        # Keep rows where Application ID is NOT in the request list
        df = df[~df["Application ID"].isin(request.app_ids)]
        
        deleted_count = original_count - len(df)
        
        # Save back to CSV
        df.to_csv(OUTPUT_CSV, index=False)
        
        # --- Delete Actual Log Files ---
        deleted_files_count = 0
        file_errors = []
        
        for app_id in request.app_ids:
            # Handle potential "application_" prefix if it's missing or present variously, 
            # but usually App ID in CSV is "application_123..."
            # Clean ID just in case
            clean_id = app_id.strip()
            
            # 1. Patterns to match
            # Note: app_id normally includes 'application_' prefix. e.g. 'application_170...'
            # Pattern A: Exact match or simple extensions
            # Pattern B: appstatus_ prefix
            # Pattern C: events_* rolling logs
            
            # If app_id starts with 'application_', we can use it directly.
            # If it's just numbers, we might need to be careful? Assuming standard Spark App IDs.
            
            # We will try to be safe and specific.
            # 1. Standard log: {clean_id} (plus extensions like .zstd, .lz4 is handled by glob if not exact)
            # Actually, standard logs are usually just the AppID filename, sometimes with compression ext.
            
            patterns = [
                os.path.join(LOGS_DIR, f"{clean_id}*"),              # Standard: application_123...
                os.path.join(LOGS_DIR, f"appstatus_{clean_id}*"),    # Rolling Anchor: appstatus_application_123...
            ]
            
            # For rolling parts (events_X_...), the format is events_X_application_123...
            # Note: clean_id usually is "application_123..."
            # So pattern is events_*_{clean_id}*
            patterns.append(os.path.join(LOGS_DIR, f"events_*_{clean_id}*"))

            for pattern in patterns:
                for fpath in glob.glob(pattern):
                    try:
                         if os.path.exists(fpath):
                             os.remove(fpath)
                             deleted_files_count += 1
                             print(f"Deleted log file: {fpath}")
                    except Exception as e:
                        print(f"Error deleting file {fpath}: {e}")
                        file_errors.append(str(e))

        return {
            "status": "success", 
            "deleted_rows": deleted_count,
            "deleted_files": deleted_files_count,
            "remaining_count": len(df),
            "file_errors": file_errors
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error deleting results: {str(e)}")

@app.get("/api/results/download")
async def download_results():
    """Downloads the latest analysis result CSV."""
    if not os.path.exists(OUTPUT_CSV):
        raise HTTPException(status_code=404, detail="No analysis results found")
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"spark_analysis_{timestamp}.csv"
    
    return FileResponse(OUTPUT_CSV, media_type='text/csv', filename=filename)

@app.get("/api/definitions")
async def get_definitions():
    """Returns the metric definitions JSON."""
    def_path = os.path.join(BASE_DIR, "spark_metric_definitions.json")
    if os.path.exists(def_path):
        with open(def_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
