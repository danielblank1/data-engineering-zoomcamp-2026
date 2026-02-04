#!/usr/bin/env python3
"""
Download NYC Yellow Taxi Trip Records (January - June 2024)
These are the Parquet files needed for the Module 3 homework.
"""

import os
import requests
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

DATA_DIR = Path("/app/data")
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]  # 01 through 06

def download_file(month: str) -> str:
    """Download a single parquet file."""
    url = f"{BASE_URL}{month}.parquet"
    filename = f"yellow_tripdata_2024-{month}.parquet"
    filepath = DATA_DIR / filename
    
    if filepath.exists():
        print(f"✓ Already exists: {filename}")
        return str(filepath)
    
    print(f"⬇ Downloading {filename}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    pct = (downloaded / total_size) * 100
                    print(f"\r  {filename}: {pct:.1f}%", end="", flush=True)
        
        print(f"\n✓ Downloaded: {filename} ({total_size / 1024 / 1024:.1f} MB)")
        return str(filepath)
    except Exception as e:
        print(f"\n✗ Failed to download {filename}: {e}")
        return None

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("NYC Yellow Taxi Data Downloader (Jan-Jun 2024)")
    print("=" * 60)
    print()
    
    # Download files (use 2 workers to be nice to the server)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(download_file, MONTHS))
    
    successful = [r for r in results if r]
    print()
    print("=" * 60)
    print(f"Downloaded {len(successful)}/{len(MONTHS)} files")
    print("=" * 60)
    
    # List files
    print("\nFiles in data directory:")
    for f in sorted(DATA_DIR.glob("*.parquet")):
        size_mb = f.stat().st_size / 1024 / 1024
        print(f"  - {f.name} ({size_mb:.1f} MB)")

if __name__ == "__main__":
    main()
