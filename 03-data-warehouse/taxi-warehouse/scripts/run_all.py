#!/usr/bin/env python3
"""
All-in-one script to download data, setup tables, and answer homework questions.
"""

import subprocess
import sys

def run_script(name: str):
    print(f"\n{'='*70}")
    print(f"Running: {name}")
    print('='*70 + "\n")
    result = subprocess.run([sys.executable, f"/app/scripts/{name}"])
    return result.returncode == 0

def main():
    print("="*70)
    print("DATA ENGINEERING ZOOMCAMP - MODULE 3 HOMEWORK")
    print("Local Data Warehouse Setup with DuckDB")
    print("="*70)
    
    steps = [
        ("download_data.py", "Downloading Yellow Taxi Parquet files (Jan-Jun 2024)"),
        ("setup_tables.py", "Creating DuckDB tables"),
        ("answer_homework.py", "Running homework queries"),
    ]
    
    for script, description in steps:
        print(f"\n>>> {description}...")
        if not run_script(script):
            print(f"Failed at step: {script}")
            sys.exit(1)
    
    print("\n" + "="*70)
    print("ALL DONE!")
    print("="*70)
    print("""
You can now:
1. Run individual queries: python scripts/query.py "YOUR SQL HERE"
2. Enter interactive mode: python scripts/query.py
3. Re-run homework answers: python scripts/answer_homework.py
""")

if __name__ == "__main__":
    main()
