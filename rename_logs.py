import os
import json
import re
import subprocess
import tempfile
import shutil

LOG_DIR = '/Users/user/NaverCorp/oss/DnA/spark_analyzer/event_logs'
DATE_REGEX = re.compile(r'(\d{4}-\d{2}-\d{2})')

def get_new_app_name(old_name):
    match = DATE_REGEX.search(old_name)
    if match:
        date_str = match.group(1)
        return f'my-spark-job-{date_str}'
    return None

def process_file(filepath):
    print(f"Processing {filepath}...")
    is_zstd = filepath.endswith('.zstd')
    
    try:
        if is_zstd:
            # Decompress to temp file
            with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmp_in:
                subprocess.run(['zstd', '-dc', filepath], stdout=tmp_in, check=True)
                tmp_in_path = tmp_in.name
            read_path = tmp_in_path
        else:
            read_path = filepath

        # Pass 1: Find the old App Name (or verify if we need to clean up)
        old_name_pattern = re.compile(r'(abt-temporary_expvar_coll-(\d{4}-\d{2}-\d{2})-batch---period-daily---measure_unit-area)')
        
        target_old_name = None
        date_str = None
        
        with open(read_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = old_name_pattern.search(line)
                if match:
                    target_old_name = match.group(1)
                    date_str = match.group(2)
                    break
        
        if not target_old_name:
            print(f"  No old App Name pattern found in {filepath}. Skipping.")
            if is_zstd: os.remove(read_path)
            return

        new_name = f'my-spark-job-{date_str}'
        print(f"  Refactoring: {target_old_name} -> {new_name} (Global Replace)")

        # Pass 2: Read, Replace, Write
        with open(read_path, 'r', encoding='utf-8') as f_in, \
             tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as tmp_out:
            
            for line in f_in:
                if target_old_name in line:
                    line = line.replace(target_old_name, new_name)
                    # Use a regex to also catch any JSON encoded versions if standard replace misses? 
                    # Usually standard replace is enough if the string is exact.
                tmp_out.write(line)
            
            tmp_out_path = tmp_out.name

        # If zstd, compress back
        if is_zstd:
            subprocess.run(['zstd', '-f', tmp_out_path, '-o', filepath], check=True)
            os.remove(tmp_out_path) # remove the modified uncompressed temp
            os.remove(read_path)    # remove the decompressed read temp
        else:
            shutil.move(tmp_out_path, filepath)

    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        # Clean up any generic temp files if they exist and are defined
        if 'tmp_in_path' in locals() and os.path.exists(tmp_in_path):
             os.remove(tmp_in_path)
        if 'tmp_out_path' in locals() and os.path.exists(tmp_out_path):
             os.remove(tmp_out_path)

def main():
    for filename in os.listdir(LOG_DIR):
        filepath = os.path.join(LOG_DIR, filename)
        if not os.path.isfile(filepath):
            continue
        
        # Filter for valid log files
        if filename.startswith('application_') or (filename.startswith('events_1') and filename.endswith('.zstd')):
             process_file(filepath)

if __name__ == "__main__":
    main()
