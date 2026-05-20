#!/bin/bash
set -e

echo "🚀 Starting Log Generator Service"
echo "================================"
echo "Output directory: ${OUTPUT_DIR:-/data/logs}"
echo "Logs per hour: ${LOGS_PER_HOUR:-100}"
echo "Run on startup: ${RUN_ON_STARTUP:-true}"

# Create output directory
mkdir -p ${OUTPUT_DIR:-/data/logs}
mkdir -p /var/log/generator

# Function to generate logs
generate_logs() {
    local date=$(date +%Y-%m-%d)
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Generating logs for $date"
    
    python /app/scripts/run_generator.py \
        --date "$date" \
        --output-dir "${OUTPUT_DIR:-/data/logs}" \
        --format all \
        --logs-per-hour ${LOGS_PER_HOUR:-100}
    
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ✅ Log generation complete"
}

# Generate on startup if enabled
if [ "${RUN_ON_STARTUP:-true}" = "true" ]; then
    echo "Running initial log generation..."
    generate_logs
fi

# Start cron daemon for scheduled runs
echo "Starting cron daemon for daily generation at 1 AM..."
cron -f
