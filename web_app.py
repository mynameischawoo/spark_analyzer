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

@app.get("/")
async def read_index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

@app.get("/api/logs")
async def list_logs():
    """Lists application logs in the event_logs directory."""
    if not os.path.exists(LOGS_DIR):
        return []
    
    files = sorted(glob.glob(os.path.join(LOGS_DIR, "application_*")))
    logs = []
    
    for f in files:
        fname = os.path.basename(f)
        mtime = os.path.getmtime(f)
        dt = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
        logs.append({
            "filename": fname,
            "date": dt,
            "path": f
        })
        
    return logs

@app.delete("/api/logs")
async def delete_logs(request: AnalyzeRequest):
    """Deletes selected log files."""
    deleted_count = 0
    errors = []
    for fname in request.files:
        # Security check: basic path traversal prevention
        if ".." in fname or "/" in fname or "\\" in fname:
            errors.append(f"{fname}: Invalid filename")
            continue

        path = os.path.join(LOGS_DIR, fname)
        if os.path.exists(path):
            try:
                os.remove(path)
                deleted_count += 1
            except Exception as e:
                errors.append(f"{fname}: {str(e)}")
        else:
            errors.append(f"{fname}: Not found")
    
    return {"status": "success", "deleted": deleted_count, "errors": errors}

@app.post("/api/upload")
async def upload_logs(files: List[UploadFile] = File(...)):
    """Uploads new log files."""
    saved_count = 0
    for file in files:
        try:
            # Basic validation
            if ".." in file.filename or "/" in file.filename:
                continue
                
            path = os.path.join(LOGS_DIR, file.filename)
            with open(path, "wb") as buffer:
                content = await file.read()
                buffer.write(content)
            saved_count += 1
        except Exception as e:
            print(f"Failed to upload {file.filename}: {e}")
            
    return {"status": "success", "saved": saved_count}

def get_app_name_from_file(filepath):
    """Reads the beginning of the file to find SparkListenerApplicationStart."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for _ in range(100): # Check first 100 lines
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

@app.post("/api/extract-names")
async def extract_names(request: NameRequest):
    """Returns app names for requested files."""
    results = {}
    for fname in request.files:
        path = os.path.join(LOGS_DIR, fname)
        if os.path.exists(path):
            results[fname] = get_app_name_from_file(path)
        else:
            results[fname] = "File Not Found"
    return results

from spark_log_parser import analyze_stage_details

@app.post("/api/analyze")
async def analyze_logs(request: AnalyzeRequest):
    """Triggers the python analysis script."""
    if not request.files:
        raise HTTPException(status_code=400, detail="No files selected")
    
    # Resolve absolute paths
    target_files = [os.path.join(LOGS_DIR, f) for f in request.files]
    
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
        path = os.path.join(LOGS_DIR, fname)
        if os.path.exists(path):
            input_files.append(path)
    
    if not input_files:
        raise HTTPException(status_code=400, detail="No valid files selected")
        
    try:
        # Assuming all files belong to one app or we just analyze what is given
        details = analyze_stage_details(input_files)
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
