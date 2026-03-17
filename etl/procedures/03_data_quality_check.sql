-- =====================================================
-- DWS 存储过程: 数据质量校验
-- 描述: 对数据进行全面的质量检查，包括完整性、准确性、一致性
-- 适用场景: ETL 流程前的数据验证、数据质量监控
-- 作者: purioc-agent-etl-engineering
-- 创建日期: 2025-03-18
-- =====================================================

-- 删除已存在的对象
DROP TABLE IF EXISTS data_quality_results CASCADE;
DROP TABLE IF EXISTS data_quality_rules CASCADE;
DROP PROCEDURE IF EXISTS sp_data_quality_check;
DROP PROCEDURE IF EXISTS sp_create_quality_rule;

-- 创建数据质量规则表
CREATE TABLE IF NOT EXISTS data_quality_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,     -- NOT_NULL, UNIQUE, RANGE, REGEX, CUSTOM
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    rule_config JSONB,                   -- 规则配置（范围、正则等）
    error_threshold DECIMAL(5,2) DEFAULT 5.0,  -- 错误率阈值（百分比）
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
) DISTRIBUTE BY REPLICATION;

-- 创建数据质量结果表（分区表，按月分区）
CREATE TABLE IF NOT EXISTS data_quality_results (
    result_id BIGSERIAL,
    check_date DATE NOT NULL,
    rule_id INT NOT NULL,
    schema_name VARCHAR(100),
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    rule_type VARCHAR(50),
    total_rows BIGINT,
    failed_rows BIGINT,
    failed_percentage DECIMAL(5,2),
    check_status VARCHAR(20),           -- PASSED, FAILED, WARNING
    error_details JSONB,                -- 错误详情（样本）
    execution_time_ms INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTE BY HASH (rule_id)
PARTITION BY RANGE (check_date);

-- 创建分区（2025年）
CREATE TABLE IF NOT EXISTS data_quality_results_202501 PARTITION OF data_quality_results
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE IF NOT EXISTS data_quality_results_202502 PARTITION OF data_quality_results
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE IF NOT EXISTS data_quality_results_202503 PARTITION OF data_quality_results
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

-- 创建存储过程: 添加数据质量规则
CREATE OR REPLACE PROCEDURE sp_create_quality_rule(
    p_rule_name VARCHAR(100),
    p_rule_type VARCHAR(50),
    p_schema_name VARCHAR(100),
    p_table_name VARCHAR(100),
    p_column_name VARCHAR(100),
    p_rule_config JSONB,
    p_error_threshold DECIMAL(5,2) DEFAULT 5.0,
    p_description TEXT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO data_quality_rules (
        rule_name, rule_type, schema_name, table_name, column_name,
        rule_config, error_threshold, description
    ) VALUES (
        p_rule_name, p_rule_type, p_schema_name, p_table_name, p_column_name,
        p_rule_config, p_error_threshold, p_description
    );
    
    RAISE NOTICE '数据质量规则 "%" 创建成功', p_rule_name;
END;
$$;

-- 创建主存储过程: 执行数据质量检查
CREATE OR REPLACE PROCEDURE sp_data_quality_check(
    p_schema_name VARCHAR(100) DEFAULT NULL,    -- NULL 表示检查所有 schema
    p_table_name VARCHAR(100) DEFAULT NULL,     -- NULL 表示检查所有表
    p_rule_id INT DEFAULT NULL,                 -- NULL 表示检查所有规则
    p_sample_size INT DEFAULT 100               -- 错误样本数量
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rule RECORD;
    v_sql TEXT;
    v_check_sql TEXT;
    v_count_sql TEXT;
    v_total_rows BIGINT;
    v_failed_rows BIGINT;
    v_failed_pct DECIMAL(5,2);
    v_status VARCHAR(20);
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration_ms INT;
    v_error_samples JSONB;
BEGIN
    v_start_time := clock_timestamp();
    
    RAISE NOTICE '开始数据质量检查...';
    RAISE NOTICE '时间: %', CURRENT_TIMESTAMP;
    
    -- 遍历所有激活的规则
    FOR v_rule IN 
        SELECT * FROM data_quality_rules 
        WHERE is_active = TRUE
        AND (p_rule_id IS NULL OR rule_id = p_rule_id)
        AND (p_schema_name IS NULL OR schema_name = p_schema_name)
        AND (p_table_name IS NULL OR table_name = p_table_name)
    LOOP
        RAISE NOTICE '检查规则: % (%.%.%)', 
            v_rule.rule_name, v_rule.schema_name, v_rule.table_name, COALESCE(v_rule.column_name, '*');
        
        -- 获取总行数
        v_count_sql := format(
            'SELECT COUNT(*) FROM %I.%I',
            v_rule.schema_name, v_rule.table_name
        );
        EXECUTE v_count_sql INTO v_total_rows;
        
        -- 构建检查 SQL
        v_check_sql := build_quality_check_sql(v_rule);
        
        -- 执行检查并获取失败行数
        EXECUTE v_check_sql INTO v_failed_rows;
        
        -- 计算失败率
        IF v_total_rows > 0 THEN
            v_failed_pct := ROUND(v_failed_rows * 100.0 / v_total_rows, 2);
        ELSE
            v_failed_pct := 0;
        END IF;
        
        -- 确定状态
        IF v_failed_pct = 0 THEN
            v_status := 'PASSED';
        ELSIF v_failed_pct <= v_rule.error_threshold THEN
            v_status := 'WARNING';
        ELSE
            v_status := 'FAILED';
        END IF;
        
        -- 获取错误样本
        IF v_failed_rows > 0 THEN
            v_error_samples := get_error_samples(v_rule, p_sample_size);
        ELSE
            v_error_samples := '[]'::JSONB;
        END IF;
        
        -- 插入结果
        INSERT INTO data_quality_results (
            check_date, rule_id, schema_name, table_name, column_name,
            rule_type, total_rows, failed_rows, failed_percentage,
            check_status, error_details, execution_time_ms
        ) VALUES (
            CURRENT_DATE, v_rule.rule_id, v_rule.schema_name, v_rule.table_name, 
            v_rule.column_name, v_rule.rule_type, v_total_rows, v_failed_rows,
            v_failed_pct, v_status, v_error_samples, 0
        );
        
        RAISE NOTICE '  总行数: %, 失败: % (%) - %', 
            v_total_rows, v_failed_rows, v_failed_pct || '%', v_status;
    END LOOP;
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECOND FROM (v_end_time - v_start_time))::INT;
    
    -- 输出汇总报告
    RAISE NOTICE '============================================';
    RAISE NOTICE '数据质量检查完成!';
    RAISE NOTICE '总耗时: % 毫秒', v_duration_ms;
    RAISE NOTICE '============================================';
    
    -- 输出检查摘要
    PERFORM fn_print_quality_summary(CURRENT_DATE);
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '数据质量检查失败: % - %', SQLSTATE, SQLERRM;
END;
$$;

-- 辅助函数: 构建质量检查 SQL
CREATE OR REPLACE FUNCTION build_quality_check_sql(p_rule data_quality_rules)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql TEXT;
    v_column_ref TEXT;
BEGIN
    -- 构建列引用
    IF p_rule.column_name IS NOT NULL THEN
        v_column_ref := format('%I', p_rule.column_name);
    ELSE
        v_column_ref := '1';
    END IF;
    
    CASE p_rule.rule_type
        WHEN 'NOT_NULL' THEN
            v_sql := format(
                'SELECT COUNT(*) FROM %I.%I WHERE %s IS NULL',
                p_rule.schema_name, p_rule.table_name, v_column_ref
            );
            
        WHEN 'UNIQUE' THEN
            v_sql := format(
                'SELECT COUNT(*) FROM (
                    SELECT %s FROM %I.%I 
                    WHERE %s IS NOT NULL
                    GROUP BY %s HAVING COUNT(*) > 1
                ) t',
                v_column_ref, p_rule.schema_name, p_rule.table_name,
                v_column_ref, v_column_ref
            );
            
        WHEN 'RANGE' THEN
            v_sql := format(
                'SELECT COUNT(*) FROM %I.%I 
                 WHERE %s < %s OR %s > %s',
                p_rule.schema_name, p_rule.table_name,
                v_column_ref, p_rule.rule_config->>'min',
                v_column_ref, p_rule.rule_config->>'max'
            );
            
        WHEN 'REGEX' THEN
            v_sql := format(
                'SELECT COUNT(*) FROM %I.%I 
                 WHERE %s !~ %L',
                p_rule.schema_name, p_rule.table_name,
                v_column_ref, p_rule.rule_config->>'pattern'
            );
            
        WHEN 'CUSTOM' THEN
            v_sql := format(
                'SELECT COUNT(*) FROM %I.%I WHERE %s',
                p_rule.schema_name, p_rule.table_name,
                p_rule.rule_config->>'condition'
            );
            
        ELSE
            v_sql := 'SELECT 0';
    END CASE;
    
    RETURN v_sql;
END;
$$;

-- 辅助函数: 获取错误样本
CREATE OR REPLACE FUNCTION get_error_samples(p_rule data_quality_rules, p_limit INT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql TEXT;
    v_result JSONB;
BEGIN
    v_sql := format(
        'SELECT jsonb_agg(row_to_json(t)) FROM (
            SELECT * FROM %I.%I 
            WHERE %s 
            LIMIT %s
        ) t',
        p_rule.schema_name, p_rule.table_name,
        CASE p_rule.rule_type
            WHEN 'NOT_NULL' THEN format('%I IS NULL', p_rule.column_name)
            WHEN 'RANGE' THEN format('%I < %s OR %I > %s', 
                p_rule.column_name, p_rule.rule_config->>'min',
                p_rule.column_name, p_rule.rule_config->>'max')
            ELSE 'FALSE'
        END,
        p_limit
    );
    
    EXECUTE v_sql INTO v_result;
    RETURN COALESCE(v_result, '[]'::JSONB);
END;
$$;

-- 辅助函数: 打印质量检查摘要
CREATE OR REPLACE FUNCTION fn_print_quality_summary(p_check_date DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_rules INT;
    v_passed INT;
    v_warnings INT;
    v_failed INT;
BEGIN
    SELECT COUNT(*) INTO v_total_rules
    FROM data_quality_results WHERE check_date = p_check_date;
    
    SELECT COUNT(*) INTO v_passed
    FROM data_quality_results WHERE check_date = p_check_date AND check_status = 'PASSED';
    
    SELECT COUNT(*) INTO v_warnings
    FROM data_quality_results WHERE check_date = p_check_date AND check_status = 'WARNING';
    
    SELECT COUNT(*) INTO v_failed
    FROM data_quality_results WHERE check_date = p_check_date AND check_status = 'FAILED';
    
    RAISE NOTICE '检查规则总数: %', v_total_rules;
    RAISE NOTICE '通过: %, 警告: %, 失败: %', v_passed, v_warnings, v_failed;
END;
$$;

COMMENT ON PROCEDURE sp_data_quality_check IS 
'执行数据质量检查，支持多种校验规则和阈值设置';

-- =====================================================
-- 使用示例
-- =====================================================

/*
-- 示例 1: 创建测试表
CREATE TABLE test_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_amount DECIMAL(10,2),
    order_date DATE,
    email VARCHAR(100),
    status VARCHAR(20)
) DISTRIBUTE BY HASH (order_id);

-- 插入测试数据（包含一些错误数据）
INSERT INTO test_orders VALUES
(1, 101, 100.00, '2025-03-01', 'customer1@example.com', 'COMPLETED'),
(2, 102, NULL, '2025-03-02', 'customer2@example.com', 'PENDING'),
(3, 103, 200.00, '2025-03-03', 'invalid-email', 'COMPLETED'),
(4, NULL, 150.00, '2025-03-04', 'customer4@example.com', 'COMPLETED'),
(5, 101, 100.00, '2025-03-05', 'customer5@example.com', 'COMPLETED'),
(6, 106, -50.00, '2025-03-06', 'customer6@example.com', 'CANCELLED');

-- 示例 2: 创建数据质量规则

-- 规则 1: customer_id 不能为空
CALL sp_create_quality_rule(
    'customer_id_not_null',
    'NOT_NULL',
    'public',
    'test_orders',
    'customer_id',
    '{}',
    0.0,
    '客户ID不能为空'
);

-- 规则 2: order_amount 必须大于 0
CALL sp_create_quality_rule(
    'order_amount_positive',
    'RANGE',
    'public',
    'test_orders',
    'order_amount',
    '{"min": 0.01, "max": 999999.99}',
    5.0,
    '订单金额必须大于0'
);

-- 规则 3: email 格式校验
CALL sp_create_quality_rule(
    'email_format_valid',
    'REGEX',
    'public',
    'test_orders',
    'email',
    '{"pattern": "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"}',
    2.0,
    '邮箱格式必须正确'
);

-- 规则 4: order_id 唯一性
CALL sp_create_quality_rule(
    'order_id_unique',
    'UNIQUE',
    'public',
    'test_orders',
    'order_id',
    '{}',
    0.0,
    '订单ID必须唯一'
);

-- 示例 3: 执行数据质量检查
CALL sp_data_quality_check('public', 'test_orders');

-- 示例 4: 查看检查结果
SELECT 
    r.rule_name,
    r.table_name,
    r.column_name,
    res.total_rows,
    res.failed_rows,
    res.failed_percentage,
    res.check_status,
    res.created_at
FROM data_quality_results res
JOIN data_quality_rules r ON res.rule_id = r.rule_id
WHERE res.check_date = CURRENT_DATE
ORDER BY res.check_status, res.failed_percentage DESC;

-- 示例 5: 查看失败样本
SELECT rule_id, jsonb_pretty(error_details) 
FROM data_quality_results 
WHERE check_status = 'FAILED' AND check_date = CURRENT_DATE;

-- 示例 6: 查看历史趋势
SELECT 
    check_date,
    COUNT(*) as total_checks,
    SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN check_status = 'FAILED' THEN 1 ELSE 0 END) as failed
FROM data_quality_results
GROUP BY check_date
ORDER BY check_date DESC;
*/
