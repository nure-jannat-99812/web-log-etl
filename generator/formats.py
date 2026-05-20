import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Generator, Dict

class LogExporter:
    @staticmethod
    def to_jsonl(logs: Generator[Dict, None, None], output_path: Path) -> int:
        """Export as JSON Lines format"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        
        with open(output_path, 'w') as f:
            for log in logs:
                f.write(json.dumps(log) + '\n')
                count += 1
                if count % 1000 == 0:
                    print(f"  Wrote {count} records...")
        
        return count
    
    @staticmethod
    def to_csv(logs: Generator[Dict, None, None], output_path: Path) -> int:
        """Export as CSV format"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Get first log to get fieldnames
        first = next(logs)
        fieldnames = list(first.keys())
        
        count = 1
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(first)
            
            for log in logs:
                writer.writerow(log)
                count += 1
                if count % 1000 == 0:
                    print(f"  Wrote {count} records...")
        
        return count
    
    @staticmethod
    def to_nginx_format(logs: Generator[Dict, None, None], output_path: Path) -> int:
        """Export as Nginx/Apache log format"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        
        with open(output_path, 'w') as f:
            for log in logs:
                dt = datetime.fromisoformat(log['timestamp'])
                nginx_time = dt.strftime('%d/%b/%Y:%H:%M:%S %z')
                
                line = f'{log["ip"]} - - [{nginx_time}] "{log["method"]} {log["endpoint"]} HTTP/1.1" {log["status_code"]} {log["response_size"]} "-" "{log["user_agent"]}"\n'
                
                f.write(line)
                count += 1
                if count % 1000 == 0:
                    print(f"  Wrote {count} records...")
        
        return count
