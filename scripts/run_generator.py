#!/usr/bin/env python3
import argparse
from pathlib import Path
from datetime import datetime
import sys
import random

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from generator.core import LogGenerator
from generator.formats import LogExporter

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic web logs")
    parser.add_argument("--date", type=str, help="Date (YYYY-MM-DD)", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", type=str, default="./data/logs")
    parser.add_argument("--format", type=str, choices=["jsonl", "csv", "nginx", "all"], default="all")
    parser.add_argument("--logs-per-hour", type=int, default=100)
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
    
    date = datetime.strptime(args.date, "%Y-%m-%d")
    output_dir = Path(args.output_dir)
    
    print(f"\n🚀 Generating logs for {date.strftime('%Y-%m-%d')}")
    print(f"   Base rate: {args.logs_per_hour} logs/hour")
    print(f"   Output: {output_dir}")
    
    # Initialize generator
    generator = LogGenerator(config_path="generator/config.yaml")
    
    # Generate logs
    log_stream = generator.generate_logs_for_day(date, args.logs_per_hour)
    
    # Create output directory
    day_dir = output_dir / date.strftime("%Y/%m/%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    
    # Export based on format
    if args.format in ["jsonl", "all"]:
        output_file = day_dir / f"logs_{date.strftime('%Y%m%d')}.jsonl"
        print(f"\n📝 Writing JSONL: {output_file}")
        count = LogExporter.to_jsonl(log_stream, output_file)
        print(f"✅ Wrote {count} records to JSONL")
    
    if args.format in ["csv", "all"]:
        # Need to regenerate for different format
        log_stream2 = generator.generate_logs_for_day(date, args.logs_per_hour)
        output_file = day_dir / f"logs_{date.strftime('%Y%m%d')}.csv"
        print(f"\n📝 Writing CSV: {output_file}")
        count = LogExporter.to_csv(log_stream2, output_file)
        print(f"✅ Wrote {count} records to CSV")
    
    if args.format in ["nginx", "all"]:
        log_stream3 = generator.generate_logs_for_day(date, args.logs_per_hour)
        output_file = day_dir / f"access_{date.strftime('%Y%m%d')}.log"
        print(f"\n📝 Writing Nginx: {output_file}")
        count = LogExporter.to_nginx_format(log_stream3, output_file)
        print(f"✅ Wrote {count} records to Nginx format")
    
    print(f"\n✨ Done! Logs saved to {day_dir}")

if __name__ == "__main__":
    main()
