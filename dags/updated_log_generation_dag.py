"""
DAG: Log Generation with Intelligent Backfill

FEATURES:
✅ Automatic backfilling of ALL missing dates from start date
✅ Loops through missing dates - skips if none found
✅ Proper FAILED status when file generation fails
✅ Modular architecture for easy future changes
✅ Comprehensive error handling and logging
✅ SQL query logging for debugging

ARCHITECTURE:
- Config class: Centralized configuration
- Helper functions: Reusable database and file operations
- Main functions: Clear separation of concerns
- Error handling: Proper propagation and status updates
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import os
import subprocess
from typing import Tuple, List, Optional, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - Easy to modify for future changes
# ============================================================================

class Config:
    """Centralized configuration for easy maintenance"""
    
    # Database
    DB_HOST = 'postgres'
    DB_NAME = 'log_analytics'
    DB_USER = 'etl_user'
    DB_PASSWORD = 'etl_password'
    
    # Paths - Modify these if your setup changes
    GENERATOR_SCRIPT = '/opt/airflow/scripts/run_generator.py'
    WORKING_DIR = '/opt/airflow'  # Where to run generator from
    
    # File paths to check (in order of preference)
    FILE_PATHS = [
        '/opt/airflow/data/logs',  # Primary location
        '/data/logs',              # Alternative location
    ]
    
    # Generation settings
    LOGS_PER_HOUR = 100
    START_DATE = '2026-05-13'  # Backfill from this date
    
    # Timeouts
    GENERATOR_TIMEOUT = 300  # 5 minutes per date
    TOTAL_TIMEOUT_HOURS = 2  # For processing all missing dates

# ============================================================================
# HELPER FUNCTIONS - Reusable database and file operations
# ============================================================================

def get_db_connection():
    """Create and return database connection"""
    return psycopg2.connect(
        host=Config.DB_HOST,
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD
    )

def execute_sql_with_logging(cursor, query: str, params: tuple = None, 
                             description: str = "SQL Query", 
                             fetch_result: bool = False):
    """
    Execute SQL with comprehensive logging
    
    Args:
        cursor: Database cursor
        query: SQL query string
        params: Query parameters
        description: Human-readable description
        fetch_result: Whether to fetch and return result
    
    Returns:
        Result if fetch_result=True, otherwise cursor
    """
    logger.info("="*70)
    logger.info(f"📊 {description}")
    logger.info("="*70)
    logger.info("SQL Query:")
    logger.info(query.strip())
    if params:
        logger.info(f"Parameters: {params}")
    logger.info("-"*70)
    
    cursor.execute(query, params)
    
    result = None
    if fetch_result and query.strip().upper().startswith('SELECT'):
        result = cursor.fetchone()
        logger.info(f"Result: {result}")
    
    logger.info("="*70)
    
    return result if fetch_result else cursor

def find_generated_file(date_str: str) -> Optional[str]:
    """
    Search for generated log file across all configured paths
    
    Args:
        date_str: Date in YYYY-MM-DD format
    
    Returns:
        Full path to file if found, None otherwise
    """
    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
    date_formatted = date_obj.strftime('%Y%m%d')
    
    # Build possible paths
    possible_paths = []
    for base_path in Config.FILE_PATHS:
        file_path = f"{base_path}/{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}/logs_{date_formatted}.jsonl"
        possible_paths.append(file_path)
    
    # Check each path
    for path in possible_paths:
        if os.path.exists(path):
            logger.info(f"✅ Found file at: {path}")
            return path
    
    # If not found in expected locations, search
    logger.warning(f"⚠️ File not found in expected locations. Searching...")
    for base_path in ['/opt/airflow', '/data']:
        search_cmd = f"find {base_path} -name '*{date_formatted}*' -type f 2>/dev/null"
        result = subprocess.run(search_cmd, shell=True, capture_output=True, text=True)
        
        if result.stdout.strip():
            files = result.stdout.strip().split('\n')
            for file in files:
                if file.endswith('.jsonl'):
                    logger.info(f"✅ Found file at: {file}")
                    return file
    
    logger.error(f"❌ File not found anywhere for date: {date_str}")
    return None

def get_file_stats(file_path: str) -> Tuple[int, int]:
    """
    Get file statistics
    
    Args:
        file_path: Path to file
    
    Returns:
        Tuple of (record_count, file_size_bytes)
    """
    if not os.path.exists(file_path):
        return (0, 0)
    
    # Get file size
    file_size = os.path.getsize(file_path)
    
    # Count lines
    with open(file_path, 'r') as f:
        record_count = sum(1 for _ in f)
    
    return (record_count, file_size)

# ============================================================================
# CORE BUSINESS LOGIC
# ============================================================================

def find_missing_dates_from_start() -> List[str]:
    """
    Find all missing dates from START_DATE to today
    
    Returns:
        List of missing dates in YYYY-MM-DD format
    """
    logger.info("="*70)
    logger.info(f"🔍 FINDING MISSING DATES FROM {Config.START_DATE}")
    logger.info("="*70)
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                date_series::date AS missing_date
            FROM 
                generate_series(
                    %s::date,
                    CURRENT_DATE,
                    '1 day'::interval
                ) AS date_series
            WHERE date_series::date NOT IN (
                SELECT generation_date 
                FROM log_gen_tracker 
                WHERE status = 'SUCCESS'
            )
            ORDER BY date_series;
        """
        
        execute_sql_with_logging(cur, query, (Config.START_DATE,), 
                                "Find missing dates from start")
        
        missing_dates = cur.fetchall()
        missing_dates_list = [str(row['missing_date']) for row in missing_dates]
        
        cur.close()
        conn.close()
        
        return missing_dates_list
        
    except Exception as e:
        logger.error(f"❌ Error finding missing dates: {e}")
        return []

def mark_date_status(date_str: str, status: str, 
                     records: Optional[int] = None, 
                     file_size: Optional[int] = None,
                     error_msg: Optional[str] = None) -> bool:
    """
    Update tracker with generation status
    
    Args:
        date_str: Date in YYYY-MM-DD format
        status: 'RUNNING', 'SUCCESS', or 'FAILED'
        records: Number of records generated (for SUCCESS)
        file_size: File size in bytes (for SUCCESS)
        error_msg: Error message (for FAILED)
    
    Returns:
        True if update successful, False otherwise
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        if status == 'RUNNING':
            # Check if record exists
            check_query = "SELECT 1 FROM log_gen_tracker WHERE generation_date = %s"
            cur.execute(check_query, (date_obj,))
            exists = cur.fetchone()
            
            if exists:
                query = """
                    UPDATE log_gen_tracker 
                    SET status = 'RUNNING', 
                        retry_count = retry_count + 1, 
                        updated_at = NOW()
                    WHERE generation_date = %s
                """
            else:
                query = """
                    INSERT INTO log_gen_tracker (generation_date, status, retry_count)
                    VALUES (%s, 'RUNNING', 0)
                """
            
            execute_sql_with_logging(cur, query, (date_obj,), 
                                    f"Mark {date_str} as RUNNING")
        
        elif status == 'SUCCESS':
            query = """
                UPDATE log_gen_tracker 
                SET status = 'SUCCESS',
                    records_generated = %s,
                    file_size_bytes = %s,
                    generated_at = NOW(),
                    updated_at = NOW(),
                    error_message = NULL
                WHERE generation_date = %s
            """
            execute_sql_with_logging(cur, query, (records, file_size, date_obj),
                                    f"Mark {date_str} as SUCCESS")
        
        elif status == 'FAILED':
            query = """
                UPDATE log_gen_tracker 
                SET status = 'FAILED',
                    error_message = %s,
                    generated_at = NOW(),
                    updated_at = NOW()
                WHERE generation_date = %s
            """
            execute_sql_with_logging(cur, query, (error_msg, date_obj),
                                    f"Mark {date_str} as FAILED")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating tracker for {date_str}: {e}")
        return False

def generate_logs_for_single_date(date_str: str) -> Tuple[bool, Optional[str]]:
    """
    Generate logs for a single date with comprehensive error handling
    
    Args:
        date_str: Date in YYYY-MM-DD format
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    logger.info("")
    logger.info("="*70)
    logger.info(f"🚀 GENERATING LOGS FOR {date_str}")
    logger.info("="*70)
    
    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
    
    try:
        # Step 1: Mark as RUNNING
        if not mark_date_status(date_str, 'RUNNING'):
            return (False, "Failed to mark status as RUNNING")
        
        # Step 2: Create directory
        for base_path in Config.FILE_PATHS:
            target_dir = f"{base_path}/{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}"
            try:
                os.makedirs(target_dir, exist_ok=True)
                logger.info(f"📁 Created directory: {target_dir}")
                break  # Success, use first available path
            except Exception as e:
                logger.warning(f"⚠️ Could not create {target_dir}: {e}")
                continue
        
        # Step 3: Run generator
        original_dir = os.getcwd()
        os.chdir(Config.WORKING_DIR)
        
        logger.info(f"📝 Running generator from {Config.WORKING_DIR}...")
        
        result = subprocess.run(
            ['python', Config.GENERATOR_SCRIPT,
             '--date', date_str,
             '--format', 'jsonl',
             '--logs-per-hour', str(Config.LOGS_PER_HOUR)],
            capture_output=True,
            text=True,
            timeout=Config.GENERATOR_TIMEOUT
        )
        
        os.chdir(original_dir)
        
        # Log generator output
        if result.stdout:
            logger.info("Generator output:")
            logger.info(result.stdout)
        
        # Step 4: Check if generator succeeded
        if result.returncode != 0:
            error_msg = f"Generator exited with code {result.returncode}"
            if result.stderr:
                error_msg += f": {result.stderr}"
            logger.error(f"❌ {error_msg}")
            mark_date_status(date_str, 'FAILED', error_msg=error_msg)
            return (False, error_msg)
        
        # Step 5: Verify file was created
        file_path = find_generated_file(date_str)
        
        if not file_path:
            error_msg = f"Generator completed but file not found for {date_str}"
            logger.error(f"❌ {error_msg}")
            mark_date_status(date_str, 'FAILED', error_msg=error_msg)
            return (False, error_msg)
        
        # Step 6: Get file stats
        records, file_size = get_file_stats(file_path)
        
        if records == 0:
            error_msg = f"File created but contains 0 records: {file_path}"
            logger.error(f"❌ {error_msg}")
            mark_date_status(date_str, 'FAILED', error_msg=error_msg)
            return (False, error_msg)
        
        # Step 7: Mark as SUCCESS
        logger.info(f"✅ File created successfully:")
        logger.info(f"   📄 Path: {file_path}")
        logger.info(f"   📊 Records: {records:,}")
        logger.info(f"   💾 Size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
        
        mark_date_status(date_str, 'SUCCESS', records=records, file_size=file_size)
        
        return (True, None)
        
    except subprocess.TimeoutExpired:
        error_msg = f"Generator timeout after {Config.GENERATOR_TIMEOUT} seconds"
        logger.error(f"❌ {error_msg}")
        mark_date_status(date_str, 'FAILED', error_msg=error_msg)
        return (False, error_msg)
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"❌ {error_msg}")
        mark_date_status(date_str, 'FAILED', error_msg=error_msg)
        return (False, error_msg)

# ============================================================================
# AIRFLOW TASK FUNCTIONS
# ============================================================================

def check_for_missing_dates(**context) -> bool:
    """
    Task 1: Check if there are missing dates to process
    Returns True to continue, False to skip generation
    """
    missing_dates = find_missing_dates_from_start()
    
    if missing_dates:
        logger.info("")
        logger.info(f"📋 Found {len(missing_dates)} missing dates:")
        for date in missing_dates:
            logger.info(f"   ❌ {date}")
        logger.info("")
        logger.info(f"🔄 Will generate logs for all {len(missing_dates)} dates")
        
        # Store for next task
        context['ti'].xcom_push(key='missing_dates', value=missing_dates)
        context['ti'].xcom_push(key='missing_count', value=len(missing_dates))
        
        return True  # Continue to generation
    else:
        logger.info("")
        logger.info("✅ No missing dates found! All dates have been generated.")
        logger.info("")
        
        context['ti'].xcom_push(key='missing_dates', value=[])
        context['ti'].xcom_push(key='missing_count', value=0)
        
        return False  # Skip generation tasks

def backfill_all_missing_dates(**context):
    """
    Task 2: Loop through all missing dates and generate logs
    """
    missing_dates = context['ti'].xcom_pull(task_ids='check_missing_dates', key='missing_dates')
    
    if not missing_dates:
        logger.info("⏭️ No missing dates to process")
        return
    
    logger.info("="*70)
    logger.info(f"🔄 BACKFILLING {len(missing_dates)} MISSING DATES")
    logger.info("="*70)
    
    summary = {
        'total': len(missing_dates),
        'successful': 0,
        'failed': 0,
        'failed_dates': []
    }
    
    for i, date_str in enumerate(missing_dates, 1):
        logger.info(f"\n📅 Processing date {i}/{len(missing_dates)}: {date_str}")
        
        success, error = generate_logs_for_single_date(date_str)
        
        if success:
            summary['successful'] += 1
        else:
            summary['failed'] += 1
            summary['failed_dates'].append({'date': date_str, 'error': error})
    
    # Print summary
    logger.info("")
    logger.info("="*70)
    logger.info("📊 BACKFILL SUMMARY")
    logger.info("="*70)
    logger.info(f"Total dates processed: {summary['total']}")
    logger.info(f"✅ Successful: {summary['successful']}")
    logger.info(f"❌ Failed: {summary['failed']}")
    
    if summary['failed_dates']:
        logger.info("")
        logger.info("Failed dates:")
        for failed in summary['failed_dates']:
            logger.info(f"   ❌ {failed['date']}: {failed['error']}")
    
    logger.info("="*70)
    
    # Store summary
    context['ti'].xcom_push(key='summary', value=summary)
    
    # Raise exception if any failed
    if summary['failed'] > 0:
        raise Exception(f"Backfill completed with {summary['failed']} failures")

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime.strptime(Config.START_DATE, '%Y-%m-%d'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'log_generation_dag_V3',
    default_args=default_args,
    description='Log generation with intelligent backfill',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    catchup=True,
    max_active_runs=1,
    tags=['logs', 'generation', 'backfill'],
)

# START TASK
# =========================================================
start_pipeline = EmptyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Task 1: Check for missing dates (ShortCircuitOperator skips downstream if False)
check_missing_dates = ShortCircuitOperator(
    task_id='check_missing_dates',
    python_callable=check_for_missing_dates,
    provide_context=True,
    dag=dag,
)

# Task 2: Generate logs for all missing dates
backfill_missing_dates = PythonOperator(
    task_id='backfill_missing_dates',
    python_callable=backfill_all_missing_dates,
    provide_context=True,
    execution_timeout=timedelta(hours=Config.TOTAL_TIMEOUT_HOURS),
    dag=dag,
)

# Task 3: Show final summary
show_summary = BashOperator(
    task_id='show_summary',
    bash_command='''
    echo ""
    echo "========================================"
    echo "📊 FINAL SUMMARY"
    echo "========================================"
    echo ""
    
    # Show backfill results
    echo "Backfill completed for missing dates"
    echo ""
    
    # Show last 20 records
    echo "========================================"
    echo "📋 LAST 20 GENERATION RECORDS"
    echo "========================================"
    
    PGPASSWORD=etl_password psql \
        -h postgres \
        -U etl_user \
        -d log_analytics \
        -c "
        SELECT
            generation_date as date,
            status,
            records_generated as records,
            ROUND(file_size_bytes/1024.0/1024.0, 2) || ' MB' as size,
            retry_count as retries,
            TO_CHAR(updated_at, 'MM-DD HH24:MI') as updated
        FROM log_gen_tracker
        ORDER BY generation_date DESC
        LIMIT 20;
        "
    
    # Show statistics
    echo ""
    echo "========================================"
    echo "📈 OVERALL STATISTICS"
    echo "========================================"
    
    PGPASSWORD=etl_password psql \
        -h postgres \
        -U etl_user \
        -d log_analytics \
        -t -c "
        SELECT
            'Total Days Generated: ' || COUNT(*) 
        FROM log_gen_tracker
        WHERE status = 'SUCCESS'
        UNION ALL
        SELECT
            'Total Failed: ' || COUNT(*)
        FROM log_gen_tracker
        WHERE status = 'FAILED'
        UNION ALL
        SELECT
            'Total Records: ' || TO_CHAR(SUM(records_generated), 'FM999,999,999')
        FROM log_gen_tracker
        WHERE status = 'SUCCESS'
        UNION ALL
        SELECT
            'Total Size: ' || ROUND(SUM(file_size_bytes)/1024.0/1024.0, 2) || ' MB'
        FROM log_gen_tracker
        WHERE status = 'SUCCESS';
        "
    
    # Check for remaining missing dates
    echo ""
    echo "========================================"
    echo "⚠️  STATUS CHECK"
    echo "========================================"
    
    MISSING=$(PGPASSWORD=etl_password psql \
        -h postgres \
        -U etl_user \
        -d log_analytics \
        -t -c "
        SELECT COUNT(*)
        FROM generate_series(
            '2026-05-13'::date,
            CURRENT_DATE,
            '1 day'::interval
        ) AS date_series
        WHERE date_series::date NOT IN (
            SELECT generation_date 
            FROM log_gen_tracker 
            WHERE status = 'SUCCESS'
        );
        " | tr -d ' ')
    
    if [ "$MISSING" -eq 0 ]; then
        echo "✅ All dates from 2026-05-13 to today have been generated!"
    else
        echo "⚠️  Still missing: $MISSING dates"
        echo "    Run DAG again to retry failed dates"
    fi
    
    echo ""
    ''',
    dag=dag,
)

# END TASK
# =========================================================
end_pipeline = EmptyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Set dependencies
start_pipeline >> check_missing_dates >> backfill_missing_dates >> show_summary >> end_pipeline