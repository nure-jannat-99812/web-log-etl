"""
DAG 1: Log Generation with Duplicate Prevention - FINAL CORRECTED VERSION

✅ FIXES:
- Fixed cursor fetch bug (was consuming rows in logging function)
- Added updated_at column support
- Correct path: /data/logs/{YEAR}/{MONTH}/{DAY}/logs_{YYYYMMDD}.jsonl

FEATURES:
1. ✅ Prints all SQL queries before execution
2. ✅ Finds and displays missing dates after May 13
3. ✅ Discovers and prints actual file location
4. ✅ Comprehensive summary with statistics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.state import State
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import os
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'log_generation_dag',
    default_args=default_args,
    description='Generate daily web logs with backfill capability',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    catchup=True,
    max_active_runs=1,
    tags=['logs', 'generation'],
)

def execute_sql_with_logging(cursor, query, params=None, query_description="SQL Query", fetch_result=False):
    """
    Execute SQL query with full logging
    FIXED: Option to fetch and return result without consuming cursor
    """
    logger.info("="*70)
    logger.info(f"📊 {query_description}")
    logger.info("="*70)
    logger.info("SQL Query:")
    logger.info(query.strip())
    if params:
        logger.info(f"Parameters: {params}")
    logger.info("-"*70)
    
    cursor.execute(query, params)
    
    result = None
    # Only fetch for logging if requested (for SELECT queries)
    if fetch_result and query.strip().upper().startswith('SELECT'):
        result = cursor.fetchone()
        logger.info(f"Result: {result}")
    
    logger.info("="*70)
    
    return result if fetch_result else cursor

def should_generate_logs(**context):
    """
    Check if logs should be generated for this date
    Returns True only if not already generated successfully
    FIXED: Cursor fetch bug - now properly returns result from logging function
    """
    execution_date = context['execution_date'].date()
    
    try:
        conn = psycopg2.connect(
            host='postgres',
            database='log_analytics',
            user='etl_user',
            password='etl_password'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if already generated successfully
        query = """
            SELECT status, records_generated, generated_at 
            FROM log_gen_tracker 
            WHERE generation_date = %s
        """
        # FIXED: Use fetch_result=True to get the result from logging function
        result = execute_sql_with_logging(cur, query, (execution_date,), 
                                         "Check if logs already generated",
                                         fetch_result=True)
        
        if result:
            if result['status'] == 'SUCCESS':
                logger.info(f"✅ Logs for {execution_date} already generated successfully on {result['generated_at']}")
                logger.info(f"   Records generated: {result['records_generated']}")
                context['ti'].xcom_push(key='skip_generation', value=True)
                cur.close()
                conn.close()
                return False
            elif result['status'] == 'FAILED':
                logger.info(f"⚠️ Previous generation for {execution_date} failed. Retrying...")
                
                query = """
                    UPDATE log_gen_tracker 
                    SET retry_count = retry_count + 1,
                        status = 'RUNNING',
                        updated_at = NOW()
                    WHERE generation_date = %s
                """
                execute_sql_with_logging(cur, query, (execution_date,),
                                        "Update retry count for failed generation")
                conn.commit()
                context['ti'].xcom_push(key='skip_generation', value=False)
                cur.close()
                conn.close()
                return True
        else:
            # No record exists - first time generating
            logger.info(f"🆕 First time generating logs for {execution_date}")
            
            query = """
                INSERT INTO log_gen_tracker (generation_date, status, retry_count)
                VALUES (%s, 'RUNNING', 0)
            """
            execute_sql_with_logging(cur, query, (execution_date,),
                                    "Insert new tracker record")
            conn.commit()
            context['ti'].xcom_push(key='skip_generation', value=False)
            cur.close()
            conn.close()
            return True
        
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        # On error, allow generation to proceed
        context['ti'].xcom_push(key='skip_generation', value=False)
        return True

def find_missing_dates(**context):
    """
    REQUIREMENT #2: Find all missing dates after May 13 and display them
    """
    logger.info("="*70)
    logger.info("🔍 FINDING MISSING DATES AFTER MAY 13, 2026")
    logger.info("="*70)
    
    try:
        conn = psycopg2.connect(
            host='postgres',
            database='log_analytics',
            user='etl_user',
            password='etl_password'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Find dates between May 13 and today that don't have SUCCESS status
        query = """
            SELECT 
                date_series::date AS missing_date
            FROM 
                generate_series(
                    '2026-05-13'::date,
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
        
        execute_sql_with_logging(cur, query, None, "Find missing dates after May 13")
        
        missing_dates = cur.fetchall()
        
        if missing_dates:
            logger.info(f"📋 Found {len(missing_dates)} missing dates that need generation:")
            for row in missing_dates:
                logger.info(f"   ❌ {row['missing_date']}")
            
            # Push to XCom for tracking
            missing_dates_list = [str(row['missing_date']) for row in missing_dates]
            context['ti'].xcom_push(key='missing_dates', value=missing_dates_list)
            context['ti'].xcom_push(key='missing_dates_count', value=len(missing_dates))
        else:
            logger.info("✅ No missing dates found! All dates have been generated.")
            context['ti'].xcom_push(key='missing_dates', value=[])
            context['ti'].xcom_push(key='missing_dates_count', value=0)
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error finding missing dates: {e}")
        context['ti'].xcom_push(key='missing_dates', value=[])

def discover_file_location(**context):
    """
    REQUIREMENT #3: Find and print where the generator actually wrote the file
    """
    execution_date = context['execution_date'].date()
    
    logger.info("="*70)
    logger.info("🔍 DISCOVERING ACTUAL FILE LOCATION")
    logger.info("="*70)
    
    # CORRECT path pattern: /data/logs/YYYY/MM/DD/logs_YYYYMMDD.jsonl
    expected_path = f"/opt/airflow/data/logs/{execution_date.year}/{execution_date.month:02d}/{execution_date.day:02d}/logs_{execution_date.strftime('%Y%m%d')}.jsonl"
    
    logger.info(f"Expected path: {expected_path}")
    
    # Check if file exists at expected location
    if os.path.exists(expected_path):
        size = os.path.getsize(expected_path)
        with open(expected_path, 'r') as f:
            lines = sum(1 for _ in f)
        
        logger.info(f"✅ FILE FOUND AT EXPECTED LOCATION:")
        logger.info(f"   📄 Path: {expected_path}")
        logger.info(f"   📊 Records: {lines:,}")
        logger.info(f"   💾 Size: {size:,} bytes ({size/1024/1024:.2f} MB)")
        logger.info("="*70)
        
        context['ti'].xcom_push(key='actual_file_path', value=expected_path)
        context['ti'].xcom_push(key='file_records', value=lines)
        context['ti'].xcom_push(key='file_size', value=size)
        return expected_path
    
    # If not found, search for it
    logger.warning(f"⚠️ File not found at expected location!")
    logger.info(f"🔍 Searching for file...")
    
    date_pattern = execution_date.strftime('%Y%m%d')
    search_commands = [
        f"find /data -name '*{date_pattern}*' -type f 2>/dev/null",
        f"find /opt/airflow/data -name '*{date_pattern}*' -type f 2>/dev/null",
        "find /data/logs -type f -name '*.jsonl' -mmin -10 2>/dev/null",
    ]
    
    found_files = []
    
    for cmd in search_commands:
        logger.info(f"   Running: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.stdout.strip():
            files = result.stdout.strip().split('\n')
            found_files.extend(files)
            for f in files:
                if os.path.exists(f):
                    size = os.path.getsize(f)
                    logger.info(f"   📄 Found: {f} ({size:,} bytes)")
    
    if found_files:
        # Use the first found file
        actual_path = found_files[0]
        logger.info(f"✅ Using file: {actual_path}")
        logger.info("="*70)
        context['ti'].xcom_push(key='actual_file_path', value=actual_path)
        return actual_path
    
    logger.error("❌ No files found anywhere!")
    logger.info("="*70)
    return None

def finalize_generation_tracking(**context):
    """
    Update tracker with generation results (SUCCESS or FAILED)
    UPDATED: Now includes updated_at column
    """
    execution_date = context['execution_date'].date()
    task_instance = context['ti']
    
    # Check if generation was skipped
    skip_generation = task_instance.xcom_pull(task_ids='check_and_mark_start', key='skip_generation')
    if skip_generation:
        logger.info(f"⏭️ Skipping finalize for {execution_date} - already generated")
        return
    
    # Check actual task state
    dag_run = context['dag_run']
    generate_task_instance = dag_run.get_task_instance('generate_logs')
    task_succeeded = generate_task_instance and generate_task_instance.state == State.SUCCESS
    
    # Try to get actual file path from discovery task
    actual_file_path = task_instance.xcom_pull(task_ids='discover_file_location', key='actual_file_path')
    
    # CORRECT expected path: /data/logs/YYYY/MM/DD/logs_YYYYMMDD.jsonl
    expected_file_path = f"/opt/airflow/data/logs/{execution_date.year}/{execution_date.month:02d}/{execution_date.day:02d}/logs_{execution_date.strftime('%Y%m%d')}.jsonl"
    
    logger.info("="*70)
    logger.info("📊 FINALIZING GENERATION TRACKING")
    logger.info("="*70)
    logger.info(f"Expected path: {expected_file_path}")
    logger.info(f"Discovered path: {actual_file_path}")
    logger.info(f"Task state: {generate_task_instance.state if generate_task_instance else 'None'}")
    
    # Determine which file to use
    file_to_use = None
    if actual_file_path and os.path.exists(actual_file_path):
        file_to_use = actual_file_path
        logger.info(f"✅ Using discovered file: {file_to_use}")
    elif os.path.exists(expected_file_path):
        file_to_use = expected_file_path
        logger.info(f"✅ Using expected file: {file_to_use}")
    
    try:
        conn = psycopg2.connect(
            host='postgres',
            database='log_analytics',
            user='etl_user',
            password='etl_password'
        )
        cur = conn.cursor()
        
        if task_succeeded and file_to_use:
            # Get file info
            result = subprocess.run(['wc', '-l', file_to_use], capture_output=True, text=True)
            record_count = int(result.stdout.split()[0]) if result.stdout else 0
            file_size = os.path.getsize(file_to_use)
            
            logger.info(f"📄 File statistics:")
            logger.info(f"   Records: {record_count:,}")
            logger.info(f"   Size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
            logger.info(f"   Location: {file_to_use}")
            
            # UPDATED: Include updated_at column
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
            execute_sql_with_logging(cur, query, (record_count, file_size, execution_date),
                                    "Update tracker with SUCCESS")
            
            logger.info(f"✅ SUCCESS: {execution_date} - {record_count:,} records generated")
            logger.info("="*70)
        else:
            # Generation failed
            if not task_succeeded:
                error_msg = f"Task generate_logs failed with state: {generate_task_instance.state if generate_task_instance else 'Unknown'}"
            else:
                error_msg = f"Task succeeded but file not found. Expected: {expected_file_path}"
            
            # UPDATED: Include updated_at column
            query = """
                UPDATE log_gen_tracker 
                SET status = 'FAILED',
                    error_message = %s,
                    generated_at = NOW(),
                    updated_at = NOW()
                WHERE generation_date = %s
            """
            execute_sql_with_logging(cur, query, (error_msg, execution_date),
                                    "Update tracker with FAILED")
            
            logger.error(f"❌ FAILED: {execution_date}")
            logger.error(f"   Error: {error_msg}")
            logger.error("="*70)
        
        conn.commit()
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error updating tracker: {e}")
        raise

# Task 1: Find missing dates (runs first to show backlog)
find_missing_dates_task = PythonOperator(
    task_id='find_missing_dates',
    python_callable=find_missing_dates,
    provide_context=True,
    dag=dag,
)

# Task 2: Check if generation needed and mark start
check_and_mark_start = PythonOperator(
    task_id='check_and_mark_start',
    python_callable=should_generate_logs,
    provide_context=True,
    dag=dag,
)

# Task 3: Create directory structure with CORRECT path
create_directories = BashOperator(
    task_id='create_directories',
    bash_command='''
    if [ "{{ ti.xcom_pull(task_ids='check_and_mark_start', key='skip_generation') }}" = "True" ]; then
        echo "⏭️ Skipping directory creation - already generated"
        exit 0
    fi
    
    # CORRECT path: /data/logs/YYYY/MM/DD/
    TARGET_DIR="/opt/airflow/data/logs/{{ execution_date.strftime('%Y') }}/{{ execution_date.strftime('%m') }}/{{ execution_date.strftime('%d') }}"
    
    echo "========================================"
    echo "📁 Creating directory structure"
    echo "========================================"
    echo "Target directory: $TARGET_DIR"
    
    mkdir -p "$TARGET_DIR"
    
    if [ -d "$TARGET_DIR" ]; then
        echo "✅ Directory created successfully"
        ls -la "$TARGET_DIR" 2>/dev/null || true
        echo "========================================"
        exit 0
    else
        echo "❌ Failed to create directory"
        exit 1
    fi
    ''',
    dag=dag,
)

# Task 4: Generate logs
generate_logs = BashOperator(
    task_id='generate_logs',
    bash_command='''
    set -e
    
    if [ "{{ ti.xcom_pull(task_ids='check_and_mark_start', key='skip_generation') }}" = "True" ]; then
        echo "⏭️ Skipping log generation - already generated"
        exit 0
    fi
    
    echo "========================================"
    echo "🚀 GENERATING LOGS FOR {{ ds }}"
    echo "========================================"
    echo "Current working directory: $(pwd)"
    echo "Target date: {{ ds }}"
    echo ""
    
    # CRITICAL FIX: Change to /opt/airflow directory so relative path "data/logs/" resolves correctly
    cd /opt/airflow
    echo "Changed to directory: $(pwd)"
    echo ""
    
    # Run the generator from /opt/airflow so "data/logs/" resolves to "/opt/airflow/data/logs/"
    # which should be mounted to /data/logs/
    python /opt/airflow/scripts/run_generator.py \
        --date {{ ds }} \
        --format jsonl \
        --logs-per-hour 100
    
    GENERATOR_EXIT_CODE=$?
    
    if [ $GENERATOR_EXIT_CODE -ne 0 ]; then
        echo ""
        echo "❌ Generator script failed with exit code: $GENERATOR_EXIT_CODE"
        echo "========================================"
        exit $GENERATOR_EXIT_CODE
    fi
    
    echo ""
    echo "✅ Generator completed successfully"
    
    # Show what was created - check BOTH possible locations
    echo ""
    echo "📁 Searching for created files..."
    
    # Check the mounted volume
    if find /data/logs -name "*{{ ds_nodash }}*" -type f 2>/dev/null | grep -q .; then
        echo "Found in /data/logs:"
        find /data/logs -name "*{{ ds_nodash }}*" -type f -exec ls -lh {} \;
    fi
    
    # Check /opt/airflow/data/logs (in case it's there)
    if find /opt/airflow/data/logs -name "*{{ ds_nodash }}*" -type f 2>/dev/null | grep -q .; then
        echo "Found in /opt/airflow/data/logs:"
        find /opt/airflow/data/logs -name "*{{ ds_nodash }}*" -type f -exec ls -lh {} \;
    fi
    
    # Show where generator actually wrote
    echo ""
    echo "📂 Checking what generator created:"
    find /opt/airflow -name "*{{ ds_nodash }}*" -type f -mmin -5 2>/dev/null | head -5 || echo "No recent files found"
    
    echo "========================================"
    exit 0
    ''',
    dag=dag,
)

# Task 5: Discover actual file location
discover_file_location_task = PythonOperator(
    task_id='discover_file_location',
    python_callable=discover_file_location,
    provide_context=True,
    dag=dag,
)

# Task 6: Finalize tracking
finalize_tracking = PythonOperator(
    task_id='finalize_tracking',
    python_callable=finalize_generation_tracking,
    provide_context=True,
    dag=dag,
)

# Task 7: Show comprehensive summary
show_summary = BashOperator(
    task_id='show_summary',
    bash_command='''
    echo ""
    echo "========================================"
    echo "📊 GENERATION SUMMARY - LAST 15 DAYS"
    echo "========================================"
    
    PGPASSWORD=etl_password psql \
        -h postgres \
        -U etl_user \
        -d log_analytics \
        -c "
        SELECT
            generation_date,
            status,
            records_generated as records,
            ROUND(file_size_bytes/1024.0/1024.0, 2) || ' MB' as file_size,
            retry_count as retries,
            TO_CHAR(generated_at, 'YYYY-MM-DD HH24:MI') as generated,
            TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI') as updated
        FROM log_gen_tracker
        ORDER BY generation_date DESC
        LIMIT 15;
        "
    
    echo ""
    echo "========================================"
    echo "📈 STATISTICS"
    echo "========================================"
    
    PGPASSWORD=etl_password psql \
        -h postgres \
        -U etl_user \
        -d log_analytics \
        -t -c "
        SELECT
            'Total Successful: ' || COUNT(*) || ' days'
        FROM log_gen_tracker
        WHERE status = 'SUCCESS'
        UNION ALL
        SELECT
            'Total Failed: ' || COUNT(*) || ' days'
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
    
    echo ""
    echo "========================================"
    echo "📁 ACTUAL FILES ON DISK (LAST 10)"
    echo "========================================"
    
    find /data/logs -name "*.jsonl" -type f -exec ls -lh {} \; 2>/dev/null | tail -10 || echo "No files found"
    
    echo ""
    echo "========================================"
    echo "⚠️  MISSING DATES"
    echo "========================================"
    
    MISSING_COUNT="{{ ti.xcom_pull(task_ids='find_missing_dates', key='missing_dates_count') }}"
    
    if [ "$MISSING_COUNT" = "0" ] || [ -z "$MISSING_COUNT" ]; then
        echo "✅ No missing dates! All dates from May 13 onwards have been generated."
    else
        echo "❌ Found $MISSING_COUNT missing dates"
        echo "   See 'find_missing_dates' task log for details"
        echo "   Run catchup to generate missing dates"
    fi
    
    echo ""
    ''',
    dag=dag,
)

# Set dependencies
find_missing_dates_task >> check_and_mark_start >> create_directories >> generate_logs >> discover_file_location_task >> finalize_tracking >> show_summary