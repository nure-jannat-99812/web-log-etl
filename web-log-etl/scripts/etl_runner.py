#!/usr/bin/env python3
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.etl_pipeline import LogETL

if __name__ == "__main__":
    print("🚀 Running ETL Pipeline (JSONL only)...")
    etl = LogETL()
    count = etl.run()
    print(f"✨ ETL completed. Inserted {count} new records.")
