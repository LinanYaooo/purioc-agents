-- =====================================================
-- DWS 存储过程: 慢变化维 Type 2 (SCD Type 2)
-- 描述: 实现维度表的慢变化维 Type 2，保留历史记录
-- 适用场景: 客户维度、产品维度等需要追踪历史变化的场景
-- 作者: purioc-agent-etl-engineering
-- 创建日期: 2025-03-18
-- =====================================================

-- 删除已存在的存储过程
DROP PROCEDURE IF EXISTS sp_scd_type2_merge;

-- 创建 SCD Type 2 合并存储过程
CREATE OR REPLACE PROCEDURE sp_scd_type2_merge(
    p_source_schema VARCHAR(100),       -- 源表 schema
    p_source_table VARCHAR(100),         -- 源表名（staging 表）
    p_target_schema VARCHAR(100),        -- 目标表 schema
    p_target_table VARCHAR(100),         -- 目标表名（维度表）
    p_key_columns TEXT,                  -- 主键列，逗号分隔（如：'customer_id'）
    p_compare_columns TEXT,              -- 需要比较的列，逗号分隔（如：'name,address,phone'）
    p_effective_date DATE DEFAULT CURRENT_DATE  -- 生效日期
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql TEXT;
    v_inserted_rows INT := 0;
    v_updated_rows INT := 0;
    v_closed_rows INT := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_key_array TEXT[];
    v_compare_array TEXT[];
BEGIN
    v_start_time := clock_timestamp();
    
    -- 解析主键列和比较列
    v_key_array := string_to_array(p_key_columns, ',');
    v_compare_array := string_to_array(p_compare_columns, ',');
    
    RAISE NOTICE '开始 SCD Type 2 处理...';
    RAISE NOTICE '主键列: %', p_key_columns;
    RAISE NOTICE '比较列: %', p_compare_columns;
    
    -- =====================================================
    -- 步骤 1: 关闭已有记录的过期记录
    -- =====================================================
    v_sql := format(
        'UPDATE %I.%I dim
         SET expiry_date = %L::DATE - 1,
             is_current = FALSE,
             last_updated = CURRENT_TIMESTAMP
         FROM %I.%I src
         WHERE %s
           AND dim.is_current = TRUE
           AND (%s)',
        p_target_schema, p_target_table,
        p_effective_date,
        p_source_schema, p_source_table,
        -- 主键匹配条件
        array_to_string(
            ARRAY(
                SELECT format('dim.%I = src.%I', col, col)
                FROM unnest(v_key_array) AS col
            ),
            ' AND '
        ),
        -- 属性变化检测条件
        array_to_string(
            ARRAY(
                SELECT format('dim.%I IS DISTINCT FROM src.%I', col, col)
                FROM unnest(v_compare_array) AS col
            ),
            ' OR '
        )
    );
    
    EXECUTE v_sql;
    GET DIAGNOSTICS v_closed_rows = ROW_COUNT;
    RAISE NOTICE '已关闭 % 条过期记录', v_closed_rows;
    
    -- =====================================================
    -- 步骤 2: 插入新记录和变化记录
    -- =====================================================
    v_sql := format(
        'INSERT INTO %I.%I (
            %s,
            effective_date,
            expiry_date,
            is_current,
            created_at,
            last_updated
        )
        SELECT 
            %s,
            %L::DATE as effective_date,
            ''9999-12-31''::DATE as expiry_date,
            TRUE as is_current,
            CURRENT_TIMESTAMP as created_at,
            CURRENT_TIMESTAMP as last_updated
        FROM %I.%I src
        WHERE NOT EXISTS (
            SELECT 1 FROM %I.%I dim
            WHERE %s
            AND dim.is_current = TRUE
            AND %s
        )',
        p_target_schema, p_target_table,
        -- 所有列（除了SCD专用列）
        p_key_columns || ',' || p_compare_columns,
        p_key_columns || ',' || p_compare_columns,
        p_effective_date,
        p_source_schema, p_source_table,
        p_target_schema, p_target_table,
        -- 主键匹配
        array_to_string(
            ARRAY(
                SELECT format('dim.%I = src.%I', col, col)
                FROM unnest(v_key_array) AS col
            ),
            ' AND '
        ),
        -- 属性相同（排除已存在的记录）
        array_to_string(
            ARRAY(
                SELECT format('dim.%I IS NOT DISTINCT FROM src.%I', col, col)
                FROM unnest(v_compare_array) AS col
            ),
            ' AND '
        )
    );
    
    EXECUTE v_sql;
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;
    RAISE NOTICE '已插入 % 条新记录', v_inserted_rows;
    
    -- 记录结束时间
    v_end_time := clock_timestamp();
    
    -- 输出结果
    RAISE NOTICE '============================================';
    RAISE NOTICE 'SCD Type 2 处理完成!';
    RAISE NOTICE '源表: %.%', p_source_schema, p_source_table;
    RAISE NOTICE '目标表: %.%', p_target_schema, p_target_table;
    RAISE NOTICE '关闭过期记录: %', v_closed_rows;
    RAISE NOTICE '插入新记录: %', v_inserted_rows;
    RAISE NOTICE '总耗时: % 秒', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '============================================';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'SCD Type 2 处理失败: % - %', SQLSTATE, SQLERRM;
END;
$$;

COMMENT ON PROCEDURE sp_scd_type2_merge IS 
'实现维度表的慢变化维 Type 2，自动处理记录变更并保留历史';

-- =====================================================
-- 辅助函数: 创建 SCD Type 2 目标表
-- =====================================================

CREATE OR REPLACE FUNCTION fn_create_scd2_table(
    p_table_name VARCHAR(100),
    p_columns_definition TEXT,      -- 列定义，如：customer_id INT, name VARCHAR(100)
    p_distribution_column VARCHAR(100) DEFAULT NULL
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql TEXT;
    v_dist_clause TEXT;
BEGIN
    -- 构建分布子句
    IF p_distribution_column IS NOT NULL THEN
        v_dist_clause := format('DISTRIBUTE BY HASH (%I)', p_distribution_column);
    ELSE
        v_dist_clause := 'DISTRIBUTE BY REPLICATION';
    END IF;
    
    -- 构建创建表 SQL
    v_sql := format(
        'CREATE TABLE IF NOT EXISTS %I (
            %s,
            effective_date DATE NOT NULL,
            expiry_date DATE NOT NULL DEFAULT ''9999-12-31'',
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (%s, effective_date)
        ) %s PARTITION BY RANGE (effective_date)',
        p_table_name,
        p_columns_definition,
        CASE 
            WHEN p_columns_definition LIKE '%customer_id%' THEN 'customer_id'
            WHEN p_columns_definition LIKE '%id%' THEN 'id'
            ELSE split_part(p_columns_definition, ' ', 1)
        END,
        v_dist_clause
    );
    
    EXECUTE v_sql;
    
    -- 创建初始分区
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I_p2025 PARTITION OF %I
         FOR VALUES FROM (''2025-01-01'') TO (''2026-01-01'')',
        p_table_name, p_table_name
    );
    
    RETURN format('表 %I 创建成功', p_table_name);
END;
$$;

-- =====================================================
-- 使用示例
-- =====================================================

/*
-- 示例 1: 创建客户维度表
SELECT fn_create_scd2_table(
    'dim_customer',
    'customer_id INT, customer_name VARCHAR(100), address VARCHAR(200), phone VARCHAR(20), email VARCHAR(100)',
    'customer_id'
);

-- 示例 2: 创建临时 staging 表
CREATE TABLE staging_customer (
    customer_id INT,
    customer_name VARCHAR(100),
    address VARCHAR(200),
    phone VARCHAR(20),
    email VARCHAR(100)
) DISTRIBUTE BY HASH (customer_id);

-- 示例 3: 插入测试数据到 staging
INSERT INTO staging_customer VALUES 
(1, '张三', '北京', '13800138000', 'zhangsan@example.com'),
(2, '李四', '上海', '13900139000', 'lisi@example.com');

-- 示例 4: 执行 SCD Type 2 合并
CALL sp_scd_type2_merge(
    'public',              -- source_schema
    'staging_customer',    -- source_table
    'public',              -- target_schema
    'dim_customer',        -- target_table
    'customer_id',         -- key_columns
    'customer_name,address,phone,email',  -- compare_columns
    CURRENT_DATE           -- effective_date
);

-- 示例 5: 查询当前有效记录
SELECT * FROM dim_customer WHERE is_current = TRUE;

-- 示例 6: 查询历史记录（包括过期记录）
SELECT * FROM dim_customer WHERE customer_id = 1 ORDER BY effective_date;

-- 示例 7: 更新 staging 数据并再次执行（模拟数据变更）
UPDATE staging_customer SET address = '深圳' WHERE customer_id = 1;
CALL sp_scd_type2_merge('public', 'staging_customer', 'public', 'dim_customer', 'customer_id', 'customer_name,address,phone,email');

-- 验证：应该能看到两条 customer_id=1 的记录，一条过期，一条当前
SELECT * FROM dim_customer WHERE customer_id = 1 ORDER BY effective_date;
*/
