--- Table-1: log_gen_tracker
CREATE TABLE IF NOT EXISTS public.log_gen_tracker (
    id SERIAL PRIMARY KEY,
    generation_date DATE UNIQUE,
    generated_at TIMESTAMP DEFAULT NOW(),
    records_generated INTEGER,
    file_size_bytes BIGINT,
    status VARCHAR(10),  -- NULL = not attempted, 'SUCCESS', or 'FAILED'
    error_message TEXT,
    retry_count INTEGER DEFAULT 0
);
CREATE INDEX idx_gen_date ON log_gen_tracker(generation_date);
CREATE INDEX idx_gen_status ON log_gen_tracker(status);


--- Table -2: web_logs
CREATE TABLE IF NOT EXISTS public.web_logs (
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


-- --- Table - 2: file_etl_tracker
CREATE TABLE IF NOT EXISTS public.file_etl_tracker (
    id SERIAL PRIMARY KEY,
    file_path TEXT UNIQUE NOT NULL,
    file_name VARCHAR(255),
    generation_date DATE,
    file_size_bytes BIGINT,
    record_count INTEGER,
    records_inserted INTEGER,
    records_skipped INTEGER,
    status VARCHAR(10),  -- NULL, 'SUCCESS', 'FAILED'
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_file_date ON file_etl_tracker(generation_date);
CREATE INDEX IF NOT EXISTS idx_file_path ON file_etl_tracker(file_path);



