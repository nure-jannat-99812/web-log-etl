import os
import json
import glob
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LogETL:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'log_analytics'),
            user=os.getenv('DB_USER', 'etl_user'),
            password=os.getenv('DB_PASSWORD', 'etl_password')
        )
        self.logs_dir = Path(os.getenv('LOGS_DIR', '/data/logs'))
        self.processed_file = Path('/var/log/etl/processed_files.txt')
        self._load_processed_files()
    
    def _load_processed_files(self):
        """Track which files have been processed"""
        self.processed = set()
        if self.processed_file.exists():
            with open(self.processed_file, 'r') as f:
                self.processed = set(line.strip() for line in f)
    
    def _mark_processed(self, filepath):
        """Mark file as processed"""
        self.processed.add(str(filepath))
        with open(self.processed_file, 'a') as f:
            f.write(f"{filepath}\n")
    
    def _create_tables(self):
        """Create log tables if not exists"""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS web_logs (
                    id SERIAL PRIMARY KEY,
                    log_id VARCHAR(36) UNIQUE,
                    timestamp TIMESTAMP,
                    ip INET,
                    country VARCHAR(2),
                    method VARCHAR(10),
                    endpoint TEXT,
                    status_code INT,
                    response_size INT,
                    user_agent TEXT,
                    referer TEXT,
                    is_bot BOOLEAN,
                    processed_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE INDEX IF NOT EXISTS idx_timestamp ON web_logs(timestamp);
                CREATE INDEX IF NOT EXISTS idx_status ON web_logs(status_code);
                CREATE INDEX IF NOT EXISTS idx_endpoint ON web_logs(endpoint);
            """)
            self.conn.commit()
            logger.info("Tables created/verified")
    
    def process_jsonl_file(self, filepath):
        """Process JSONL file and insert into database"""
        logger.info(f"Processing JSONL: {filepath}")
        records = []
        
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    records.append((
                        data.get('log_id'),
                        data.get('timestamp'),
                        data.get('ip'),
                        data.get('country'),
                        data.get('method'),
                        data.get('endpoint'),
                        data.get('status_code'),
                        data.get('response_size'),
                        data.get('user_agent'),
                        data.get('referer'),
                        data.get('is_bot', False)
                    ))
                except Exception as e:
                    logger.error(f"Error parsing line {line_num}: {e}")
        
        if records:
            with self.conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO web_logs 
                    (log_id, timestamp, ip, country, method, endpoint, 
                     status_code, response_size, user_agent, referer, is_bot)
                    VALUES %s
                    ON CONFLICT (log_id) DO NOTHING
                """, records)
                self.conn.commit()
                logger.info(f"Inserted {len(records)} records from {Path(filepath).name}")
        
        return len(records)
    
    def run(self):
        """Main ETL process - ONLY JSONL files"""
        logger.info("=" * 50)
        logger.info("Starting ETL pipeline (JSONL only)...")
        
        # Create tables if not exists
        self._create_tables()
        
        # Find ONLY JSONL files
        jsonl_files = glob.glob(str(self.logs_dir / "**" / "*.jsonl"), recursive=True)
        
        logger.info(f"Found {len(jsonl_files)} JSONL files")
        
        total_records = 0
        
        # Process only JSONL files
        for filepath in jsonl_files:
            if filepath not in self.processed:
                count = self.process_jsonl_file(filepath)
                total_records += count
                self._mark_processed(filepath)
        
        # Show database statistics
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM web_logs")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT DATE(timestamp)) FROM web_logs")
            days = cur.fetchone()[0]
            
            logger.info("=" * 50)
            logger.info(f"✅ ETL Complete!")
            logger.info(f"   New records inserted: {total_records}")
            logger.info(f"   Total records in DB: {total}")
            logger.info(f"   Days of data: {days}")
        
        return total_records

if __name__ == "__main__":
    etl = LogETL()
    etl.run()
