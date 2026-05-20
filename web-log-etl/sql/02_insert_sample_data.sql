-- --- insert after manual etl script: DAG 1
INSERT INTO public.log_gen_tracker (generation_date, status, records_generated, file_size_bytes, generated_at)
VALUES 
    ('2024-01-15', 'SUCCESS', 1748, 571392, NOW()),
    ('2026-05-13', 'SUCCESS', 3934, 1363148, NOW())
ON CONFLICT (generation_date) DO UPDATE 
SET status = 'SUCCESS',
    records_generated = EXCLUDED.records_generated,
    file_size_bytes = EXCLUDED.file_size_bytes,
    generated_at = NOW();

-- Verify
SELECT *
FROM log_gen_tracker 
ORDER BY generation_date;


--- insert after manual etl script: DAG 2
INSERT INTO public.file_etl_tracker 
(file_path, file_name, generation_date, file_size_bytes, record_count, records_inserted, records_skipped, status)
VALUES 
    ('/data/logs/2024/01/15/logs_20240115.jsonl', 'logs_20240115.jsonl', '2024-01-15', 571392, 1748, 1748, 0, 'SUCCESS'),
    ('/data/logs/2026/05/13/logs_20260513.jsonl', 'logs_20260513.jsonl', '2026-05-13', 1363148, 3934, 3934, 0, 'SUCCESS')
ON CONFLICT (file_path) DO NOTHING;

-- Verify insertion
SELECT 
    file_name,
    generation_date,
    record_count,
    records_inserted,
    status
FROM public.file_etl_tracker
ORDER BY generation_date;

