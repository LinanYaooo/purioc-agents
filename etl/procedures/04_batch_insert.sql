-- =====================================================
-- DWS 存储过程: 批量插入优化
-- 描述: 使用批量插入和并行处理优化大数据量加载性能
-- 适用场景: 全量数据加载、历史数据迁移
-- 作者: purioc-agent-etl-engineering
-- 创建日期: 2025-03-18
-- =====================================================

DROP PROCEDURE IF EXISTS sp_batch_insert_optimized;
DROP PROCEDURE IF EXISTS sp_parallel_load;
DROP FUNCTION IF EXISTS fn_estimate_row_count;

-- 辅助函数: 估算源表行数
CREATE OR REPLACE FUNCTION fn_estimate_row_count(
    p_schema_name VARCHAR(100),
    p_table_name VARCHAR(100)
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_count BIGINT;
BEGIN
    -- 使用统计信息快速估算
    SELECT n_live_tup INTO v_count
    FROM pg_stat_user_tables
    WHERE schemaname = p_schema_name AND relname = p_table_name;
    
    -- 如果没有统计信息，执行精确计数
    IF v_count IS NULL OR v_count = 0 THEN
        EXECUTE format(
            'SELECT COUNT(*) FROM %I.%I',
            p_schema_name, p_table_name
        ) INTO v_count;
    END IF;
    
    RETURN COALESCE(v_count, 0);
END;
$$;

-- 存储过程: 优化的批量插入
CREATE OR REPLACE PROCEDURE sp_batch_insert_optimized(
    p_source_schema VARCHAR(100),
    p_source_table VARCHAR(100),
    p_target_schema VARCHAR(100),
    p_target_table VARCHAR(100),
    p_batch_size INT DEFAULT 50000,         -- 每批次行数
    p_commit_frequency INT DEFAULT 10,      -- 每多少批次提交一次
    p_truncate_target BOOLEAN DEFAULT FALSE -- 是否清空目标表
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_rows BIGINT := 0;
    v_processed_rows BIGINT := 0;
    v_batch_count INT := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_last_id BIGINT := 0;
    v_batch_start TIMESTAMP;
    v_batch_inserted INT;
    v_pk_column VARCHAR(100);
    v_sql TEXT;
    v_columns TEXT;
BEGIN
    v_start_time := clock_timestamp();
    
    -- 获取主键列名（假设第一个整数列是主键）
    SELECT a.attname INTO v_pk_column
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = format('%I.%I', p_source_schema, p_source_table)::regclass
    AND i.indisprimary
    LIMIT 1;
    
    IF v_pk_column IS NULL THEN
        -- 如果没有主键，尝试找第一个整数列
        SELECT column_name INTO v_pk_column
        FROM information_schema.columns
        WHERE table_schema = p_source_schema
        AND table_name = p_source_table
        AND data_type IN ('integer', 'bigint', 'smallint')
        ORDER BY ordinal_position
        LIMIT 1;
    END IF;
    
    RAISE NOTICE '使用主键/ID列: %', v_pk_column;
    
    -- 获取列列表
    SELECT string_agg(format('%I', column_name), ', ' ORDER BY ordinal_position)
    INTO v_columns
    FROM information_schema.columns
    WHERE table_schema = p_source_schema
    AND table_name = p_source_table;
    
    -- 估算总行数
    v_total_rows := fn_estimate_row_count(p_source_schema, p_source_table);
    RAISE NOTICE '估算源表行数: %', v_total_rows;
    
    -- 如果需要，清空目标表
    IF p_truncate_target THEN
        EXECUTE format('TRUNCATE TABLE %I.%I', p_target_schema, p_target_table);
        RAISE NOTICE '已清空目标表';
    END IF;
    
    RAISE NOTICE '开始批量插入，批次大小: %', p_batch_size;
    
    -- 循环处理批次
    LOOP
        v_batch_start := clock_timestamp();
        
        -- 构建批量插入 SQL
        IF v_pk_column IS NOT NULL THEN
            v_sql := format(
                'INSERT INTO %I.%I (%s)
                 SELECT %s FROM %I.%I
                 WHERE %I > %s
                 ORDER BY %I
                 LIMIT %s',
                p_target_schema, p_target_table, v_columns,
                v_columns, p_source_schema, p_source_table,
                v_pk_column, v_last_id,
                v_pk_column,
                p_batch_size
            );
        ELSE
            -- 如果没有合适的列，使用 OFFSET/LIMIT（性能较差）
            v_sql := format(
                'INSERT INTO %I.%I (%s)
                 SELECT %s FROM %I.%I
                 OFFSET %s LIMIT %s',
                p_target_schema, p_target_table, v_columns,
                v_columns, p_source_schema, p_source_table,
                v_processed_rows, p_batch_size
            );
        END IF;
        
        -- 执行插入
        EXECUTE v_sql;
        GET DIAGNOSTICS v_batch_inserted = ROW_COUNT;
        
        -- 如果没有数据了，退出循环
        IF v_batch_inserted = 0 THEN
            EXIT;
        END IF;
        
        v_processed_rows := v_processed_rows + v_batch_inserted;
        v_batch_count := v_batch_count + 1;
        
        -- 更新 last_id
        IF v_pk_column IS NOT NULL THEN
            EXECUTE format(
                'SELECT MAX(%I) FROM %I.%I',
                v_pk_column, p_target_schema, p_target_table
            ) INTO v_last_id;
        END IF;
        
        -- 定期提交
        IF v_batch_count % p_commit_frequency = 0 THEN
            COMMIT;
            RAISE NOTICE '批次 %: 已处理 % / % 行 (%.1f%%), 本批次耗时: % ms',
                v_batch_count,
                v_processed_rows,
                v_total_rows,
                CASE WHEN v_total_rows > 0 
                     THEN v_processed_rows * 100.0 / v_total_rows 
                     ELSE 0 
                END,
                EXTRACT(MILLISECOND FROM (clock_timestamp() - v_batch_start))::INT;
        END IF;
        
        -- 如果本批次未达到批次大小，说明已经处理完毕
        IF v_batch_inserted < p_batch_size THEN
            EXIT;
        END IF;
    END LOOP;
    
    -- 最终提交
    COMMIT;
    
    v_end_time := clock_timestamp();
    
    -- 输出结果
    RAISE NOTICE '============================================';
    RAISE NOTICE '批量插入完成!';
    RAISE NOTICE '源表: %.%', p_source_schema, p_source_table;
    RAISE NOTICE '目标表: %.%', p_target_schema, p_target_table;
    RAISE NOTICE '总处理行数: %', v_processed_rows;
    RAISE NOTICE '总批次: %', v_batch_count;
    RAISE NOTICE '总耗时: % 秒', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '平均速度: % 行/秒', 
        ROUND(v_processed_rows / NULLIF(EXTRACT(EPOCH FROM (v_end_time - v_start_time)), 0));
    RAISE NOTICE '============================================';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '批量插入失败: % - %', SQLSTATE, SQLERRM;
END;
$$;

-- 存储过程: 并行加载（使用 DWS 的并行能力）
CREATE OR REPLACE PROCEDURE sp_parallel_load(
    p_source_schema VARCHAR(100),
    p_source_table VARCHAR(100),
    p_target_schema VARCHAR(100),
    p_target_table VARCHAR(100),
    p_num_partitions INT DEFAULT 4     -- 并行分区数
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql TEXT;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_pk_column VARCHAR(100);
    v_min_id BIGINT;
    v_max_id BIGINT;
    v_range_size BIGINT;
    i INT;
BEGIN
    v_start_time := clock_timestamp();
    
    -- 获取主键列
    SELECT a.attname INTO v_pk_column
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = format('%I.%I', p_source_schema, p_source_table)::regclass
    AND i.indisprimary
    LIMIT 1;
    
    IF v_pk_column IS NULL THEN
        RAISE EXCEPTION '并行加载需要主键列';
    END IF;
    
    -- 获取 ID 范围
    EXECUTE format(
        'SELECT MIN(%I), MAX(%I) FROM %I.%I',
        v_pk_column, v_pk_column, p_source_schema, p_source_table
    ) INTO v_min_id, v_max_id;
    
    v_range_size := (v_max_id - v_min_id + 1) / p_num_partitions;
    
    RAISE NOTICE '并行加载配置:';
    RAISE NOTICE '  分区数: %', p_num_partitions;
    RAISE NOTICE '  ID范围: % ~ %', v_min_id, v_max_id;
    RAISE NOTICE '  每分区范围: %', v_range_size;
    
    -- 清空目标表
    EXECUTE format('TRUNCATE TABLE %I.%I', p_target_schema, p_target_table);
    
    -- 为每个分区创建并发的加载任务
    -- 注意：在实际环境中，这可能需要使用 DWS 的并行查询或外部调度
    FOR i IN 0..p_num_partitions-1 LOOP
        v_sql := format(
            'INSERT INTO %I.%I 
             SELECT * FROM %I.%I
             WHERE %I >= %s AND %I < %s',
            p_target_schema, p_target_table,
            p_source_schema, p_source_table,
            v_pk_column, v_min_id + (i * v_range_size),
            v_pk_column, CASE WHEN i = p_num_partitions-1 
                              THEN v_max_id + 1 
                              ELSE v_min_id + ((i + 1) * v_range_size) 
                         END
        );
        
        RAISE NOTICE '执行分区 %: %', i, v_sql;
        EXECUTE v_sql;
    END LOOP;
    
    v_end_time := clock_timestamp();
    
    RAISE NOTICE '============================================';
    RAISE NOTICE '并行加载完成!';
    RAISE NOTICE '总耗时: % 秒', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '============================================';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '并行加载失败: % - %', SQLSTATE, SQLERRM;
END;
$$;

COMMENT ON PROCEDURE sp_batch_insert_optimized IS 
'优化的批量插入存储过程，支持分批提交和进度跟踪';

COMMENT ON PROCEDURE sp_parallel_load IS 
'并行数据加载存储过程，利用 DWS 分布式特性提高性能';

-- =====================================================
-- 使用示例
-- =====================================================

/*
-- 示例 1: 创建测试表
CREATE TABLE large_source_table (
    id BIGSERIAL PRIMARY KEY,
    data VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(10,2)
) DISTRIBUTE BY HASH (id);

-- 插入大量测试数据
INSERT INTO large_source_table (data, amount)
SELECT 
    'Data_' || generate_series,
    random() * 1000
FROM generate_series(1, 1000000);

-- 创建目标表（相同结构）
CREATE TABLE large_target_table (LIKE large_source_table INCLUDING ALL)
DISTRIBUTE BY HASH (id);

-- 示例 2: 使用优化的批量插入
CALL sp_batch_insert_optimized(
    'public',                 -- source_schema
    'large_source_table',     -- source_table
    'public',                 -- target_schema
    'large_target_table',     -- target_table
    50000,                    -- batch_size
    5,                        -- commit_frequency
    TRUE                      -- truncate_target
);

-- 示例 3: 使用并行加载（如果有主键）
CALL sp_parallel_load(
    'public',
    'large_source_table',
    'public',
    'large_target_table',
    4                         -- num_partitions
);

-- 示例 4: 验证数据
SELECT 
    'Source' as table_name,
    COUNT(*) as row_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM large_source_table
UNION ALL
SELECT 
    'Target',
    COUNT(*),
    MIN(id),
    MAX(id)
FROM large_target_table;

-- 示例 5: 性能对比
-- 直接插入（用于对比）
EXPLAIN ANALYZE
INSERT INTO large_target_table 
SELECT * FROM large_source_table;

-- 批量插入（应该更快）
-- 使用 sp_batch_insert_optimized 存储过程

-- 清理
DROP TABLE IF EXISTS large_source_table;
DROP TABLE IF EXISTS large_target_table;
*/
