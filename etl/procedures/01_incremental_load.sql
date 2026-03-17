-- =====================================================
-- DWS 存储过程: 增量数据加载
-- 描述: 从源表增量抽取数据到目标表，支持水印机制
-- 适用场景: 每日/每小时增量同步
-- 作者: purioc-agent-etl-engineering
-- 创建日期: 2025-03-18
-- =====================================================

-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS sp_incremental_load;

-- 创建存储过程
CREATE OR REPLACE PROCEDURE sp_incremental_load(
    p_source_schema VARCHAR(100),      -- 源表 schema
    p_source_table VARCHAR(100),        -- 源表名
    p_target_schema VARCHAR(100),       -- 目标表 schema
    p_target_table VARCHAR(100),        -- 目标表名
    p_watermark_column VARCHAR(100),    -- 水印字段（通常是更新时间戳）
    p_batch_size INT DEFAULT 10000      -- 每批次处理行数
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_watermark TIMESTAMP;         -- 上次加载时间戳
    v_current_watermark TIMESTAMP;      -- 当前最大时间戳
    v_inserted_rows INT := 0;           -- 插入行数
    v_start_time TIMESTAMP;             -- 开始时间
    v_end_time TIMESTAMP;               -- 结束时间
    v_sql TEXT;                         -- 动态 SQL
BEGIN
    -- 记录开始时间
    v_start_time := clock_timestamp();
    
    -- 创建水印表（如果不存在）
    CREATE TABLE IF NOT EXISTS etl_watermark (
        source_table VARCHAR(200) PRIMARY KEY,
        last_watermark TIMESTAMP NOT NULL DEFAULT '1900-01-01'::TIMESTAMP,
        last_load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        row_count INT DEFAULT 0
    ) DISTRIBUTE BY REPLICATION;  -- 复制表，每个节点都有完整数据
    
    -- 获取上次加载的水印值
    SELECT last_watermark INTO v_last_watermark
    FROM etl_watermark
    WHERE source_table = p_source_schema || '.' || p_source_table;
    
    -- 如果没有记录，使用最小时间
    IF v_last_watermark IS NULL THEN
        v_last_watermark := '1900-01-01'::TIMESTAMP;
        INSERT INTO etl_watermark (source_table, last_watermark)
        VALUES (p_source_schema || '.' || p_source_table, v_last_watermark);
    END IF;
    
    -- 获取当前最大时间戳
    v_sql := format(
        'SELECT MAX(%I) FROM %I.%I WHERE %I > %L',
        p_watermark_column, p_source_schema, p_source_table, 
        p_watermark_column, v_last_watermark
    );
    EXECUTE v_sql INTO v_current_watermark;
    
    -- 如果没有新数据，直接返回
    IF v_current_watermark IS NULL THEN
        RAISE NOTICE '没有新数据需要加载。上次加载时间: %', v_last_watermark;
        RETURN;
    END IF;
    
    RAISE NOTICE '开始增量加载: 从 % 到 %', v_last_watermark, v_current_watermark;
    
    -- 使用批量插入优化性能
    v_sql := format(
        'INSERT INTO %I.%I 
         SELECT * FROM %I.%I 
         WHERE %I > %L AND %I <= %L',
        p_target_schema, p_target_table,
        p_source_schema, p_source_table,
        p_watermark_column, v_last_watermark,
        p_watermark_column, v_current_watermark
    );
    
    EXECUTE v_sql;
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;
    
    -- 更新水印
    UPDATE etl_watermark
    SET last_watermark = v_current_watermark,
        last_load_time = CURRENT_TIMESTAMP,
        row_count = v_inserted_rows
    WHERE source_table = p_source_schema || '.' || p_source_table;
    
    -- 记录结束时间并计算耗时
    v_end_time := clock_timestamp();
    
    -- 输出结果
    RAISE NOTICE '============================================';
    RAISE NOTICE '增量加载完成!';
    RAISE NOTICE '源表: %.%', p_source_schema, p_source_table;
    RAISE NOTICE '目标表: %.%', p_target_schema, p_target_table;
    RAISE NOTICE '加载行数: %', v_inserted_rows;
    RAISE NOTICE '时间范围: % ~ %', v_last_watermark, v_current_watermark;
    RAISE NOTICE '耗时: % 秒', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '============================================';
    
EXCEPTION
    WHEN OTHERS THEN
        -- 错误处理
        RAISE EXCEPTION '增量加载失败: % - %', SQLSTATE, SQLERRM;
END;
$$;

-- 添加注释
COMMENT ON PROCEDURE sp_incremental_load IS 
'增量数据加载存储过程，支持水印机制，适用于定时 ETL 任务';

-- =====================================================
-- 使用示例
-- =====================================================

/*
-- 示例 1: 基础用法
CALL sp_incremental_load(
    'source_schema',      -- 源 schema
    'orders',             -- 源表
    'dw_schema',          -- 目标 schema
    'fact_orders',        -- 目标表
    'update_time',        -- 水印字段
    10000                 -- 批次大小
);

-- 示例 2: 查看水印表状态
SELECT * FROM etl_watermark;

-- 示例 3: 重置水印（重新全量加载）
UPDATE etl_watermark 
SET last_watermark = '1900-01-01'::TIMESTAMP
WHERE source_table = 'source_schema.orders';

-- 示例 4: 创建定时任务（每小时执行）
SELECT cron.schedule(
    'incremental-load-orders',  -- 任务名
    '0 * * * *',               -- cron 表达式（每小时）
    $$
    CALL sp_incremental_load('source_schema', 'orders', 'dw_schema', 'fact_orders', 'update_time');
    $$
);
*/
