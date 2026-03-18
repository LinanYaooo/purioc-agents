-- ============================================
-- 存储过程名称: SP_LOAD_DWS_TRADE_ORDERS_SUMMARY_DAY
-- 目标表: dws.dws_trade_orders_summary_day
-- Mapping ID: M_DWS_ORDERS_SUM_001
-- 加载策略: INCREMENTAL_MERGE
-- 生成时间: 2026-03-18 14:30:00
-- 生成工具: ETL Automation Platform v2.0
-- ============================================

CREATE OR REPLACE PROCEDURE dws.SP_LOAD_DWS_TRADE_ORDERS_SUMMARY_DAY(
    IN  IN_BATCH_DATE         DATE,                    -- 批次日期
    IN  IN_FORCE_FULL         BOOLEAN DEFAULT FALSE,   -- 强制全量标识
    OUT OUT_RET_CODE          INTEGER,                 -- 返回码: 0成功, 非0失败
    OUT OUT_RET_MSG           VARCHAR(1000),           -- 返回信息
    OUT OUT_ROW_COUNT         BIGINT,                  -- 处理行数
    OUT OUT_INSERT_COUNT      BIGINT,                  -- 插入行数
    OUT OUT_UPDATE_COUNT      BIGINT,                  -- 更新行数
    OUT OUT_DELETE_COUNT      BIGINT,                  -- 删除行数
    OUT OUT_ERROR_COUNT       BIGINT                   -- 错误行数
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    -- 变量声明
    V_PROC_NAME             VARCHAR(100) := 'SP_LOAD_DWS_TRADE_ORDERS_SUMMARY_DAY';
    V_MAPPING_ID            VARCHAR(50)  := 'M_DWS_ORDERS_SUM_001';
    V_START_TIME            TIMESTAMP    := CURRENT_TIMESTAMP;
    V_END_TIME              TIMESTAMP;
    V_STEP                  VARCHAR(50);
    V_STEP_START_TIME       TIMESTAMP;
    
    -- 加载策略变量
    V_LOADING_STRATEGY      VARCHAR(20)  := 'INCREMENTAL_MERGE';
    V_IS_FULL_LOAD          BOOLEAN;
    
    -- 统计变量
    V_SOURCE_COUNT          BIGINT := 0;
    V_DUPLICATE_COUNT       BIGINT := 0;
    V_VALIDATION_ERROR_COUNT BIGINT := 0;
    
    -- 游标变量（用于批量处理）
    V_BATCH_SIZE            INTEGER := 10000;
    V_BATCH_COUNT           INTEGER := 0;
    
    -- 异常处理变量
    V_ERROR_CODE            VARCHAR(10);
    V_ERROR_MESSAGE         VARCHAR(4000);
    
    -- 依赖检查变量
    V_DEP_CHECK_RESULT      BOOLEAN;
    V_MISSING_DEPS          TEXT;

BEGIN
    -- ==========================================
    -- 步骤1: 初始化与参数校验
    -- ==========================================
    V_STEP := 'INIT';
    V_STEP_START_TIME := CURRENT_TIMESTAMP;
    
    -- 初始化输出参数
    OUT_RET_CODE     := 0;
    OUT_RET_MSG      := 'SUCCESS';
    OUT_ROW_COUNT    := 0;
    OUT_INSERT_COUNT := 0;
    OUT_UPDATE_COUNT := 0;
    OUT_DELETE_COUNT := 0;
    OUT_ERROR_COUNT  := 0;
    
    -- 记录开始日志
    INSERT INTO etl_log.proc_execution_log (
        proc_name, mapping_id, batch_date, step, status, 
        start_time, message
    ) VALUES (
        V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, V_STEP, 'RUNNING',
        V_START_TIME, 
        FORMAT('存储过程开始执行。批次日期: %s, 加载策略: %s, 强制全量: %s',
               IN_BATCH_DATE, V_LOADING_STRATEGY, IN_FORCE_FULL)
    );
    
    -- 参数校验
    IF IN_BATCH_DATE IS NULL THEN
        RAISE EXCEPTION '批次日期(IN_BATCH_DATE)不能为空';
    END IF;
    
    -- 确定加载方式
    V_IS_FULL_LOAD := V_LOADING_STRATEGY = 'FULL' OR IN_FORCE_FULL;
    
    RAISE NOTICE '[%] % - 初始化完成, 全量加载: %', 
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS'), 
        V_STEP, V_IS_FULL_LOAD;

    -- ==========================================
    -- 步骤2: 上游依赖检查
    -- ==========================================
    V_STEP := 'DEPENDENCY_CHECK';
    V_STEP_START_TIME := CURRENT_TIMESTAMP;
    
    -- 检查依赖任务: dwd_trade_orders_detail_day, dim_channel_info
    SELECT COUNT(*) INTO V_SOURCE_COUNT
    FROM etl_log.proc_execution_log
    WHERE proc_name IN ('SP_LOAD_DWD_TRADE_ORDERS_DETAIL_DAY', 'SP_LOAD_DIM_CHANNEL_INFO')
      AND batch_date = IN_BATCH_DATE
      AND status = 'SUCCESS'
      AND end_time >= IN_BATCH_DATE - INTERVAL '7 days';
    
    IF V_SOURCE_COUNT < 2 THEN
        -- 查询具体缺失的依赖
        SELECT STRING_AGG(DISTINCT proc_name, ', ')
        INTO V_MISSING_DEPS
        FROM (VALUES 
            ('SP_LOAD_DWD_TRADE_ORDERS_DETAIL_DAY'),
            ('SP_LOAD_DIM_CHANNEL_INFO')
        ) AS deps(proc_name)
        WHERE NOT EXISTS (
            SELECT 1 FROM etl_log.proc_execution_log
            WHERE proc_execution_log.proc_name = deps.proc_name
              AND batch_date = IN_BATCH_DATE
              AND status = 'SUCCESS'
        );
        
        RAISE EXCEPTION '上游依赖任务未完成。缺失: %', V_MISSING_DEPS;
    END IF;
    
    -- 记录依赖检查成功
    INSERT INTO etl_log.proc_execution_log (
        proc_name, mapping_id, batch_date, step, status, 
        start_time, end_time, duration_seconds, message
    ) VALUES (
        V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, V_STEP, 'SUCCESS',
        V_STEP_START_TIME, CURRENT_TIMESTAMP,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - V_STEP_START_TIME)),
        '上游依赖检查通过: dwd_trade_orders_detail_day, dim_channel_info'
    );

    -- ==========================================
    -- 步骤3: 数据预处理
    -- ==========================================
    V_STEP := 'PREPROCESS';
    V_STEP_START_TIME := CURRENT_TIMESTAMP;
    
    IF V_IS_FULL_LOAD THEN
        -- 全量加载: 清空目标表
        TRUNCATE TABLE dws.dws_trade_orders_summary_day;
        
        INSERT INTO etl_log.proc_execution_log (
            proc_name, mapping_id, batch_date, step, status, 
            start_time, message
        ) VALUES (
            V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, V_STEP, 'SUCCESS',
            V_STEP_START_TIME, '全量加载: 已清空目标表'
        );
    ELSE
        -- 增量加载: 幂等处理 - 先删除当日已有数据
        DELETE FROM dws.dws_trade_orders_summary_day
        WHERE stat_date = IN_BATCH_DATE;
        
        GET DIAGNOSTICS OUT_DELETE_COUNT = ROW_COUNT;
        
        INSERT INTO etl_log.proc_execution_log (
            proc_name, mapping_id, batch_date, step, status, 
            start_time, message
        ) VALUES (
            V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, V_STEP, 'SUCCESS',
            V_STEP_START_TIME, 
            FORMAT('增量加载: 已删除 %s 条当日数据', OUT_DELETE_COUNT)
        );
    END IF;

    -- ==========================================
    -- 步骤4: 数据加载
    -- ==========================================
    V_STEP := 'DATA_LOAD';
    V_STEP_START_TIME := CURRENT_TIMESTAMP;
    
    -- 插入数据（包含JOIN、WHERE、GROUP BY、字段计算）
    INSERT INTO dws.dws_trade_orders_summary_day (
        summary_id,
        stat_date,
        channel_code,
        channel_name,
        total_orders,
        total_amount,
        total_customers,
        avg_order_amount,
        amount_level,
        etl_load_time
    )
    WITH source_data AS (
        -- 第一步: 关联查询和过滤
        SELECT 
            a.stat_date,
            a.channel_code,
            COALESCE(b.channel_name, '未知渠道') AS channel_name,
            a.order_id,
            a.customer_id,
            a.order_amount
        FROM dwd.dwd_trade_orders_detail_day a
        LEFT JOIN dim.dim_channel_info b 
            ON a.channel_code = b.channel_code 
            AND b.is_valid = 'Y'
        WHERE 1=1
          -- WHERE条件: 订单状态检查
          AND a.order_status IN ('COMPLETED', 'PAID')
          -- WHERE条件: 删除标识检查
          AND a.is_deleted = 'N'
          -- 增量条件
          AND (
              V_IS_FULL_LOAD
              OR (
                  a.etl_load_time >= IN_BATCH_DATE 
                  AND a.etl_load_time < IN_BATCH_DATE + INTERVAL '1 day'
              )
          )
    ),
    aggregated AS (
        -- 第二步: 聚合计算
        SELECT 
            stat_date,
            channel_code,
            channel_name,
            -- total_orders: 订单总数 (COUNT)
            COUNT(order_id) AS total_orders,
            -- total_amount: 订单总金额 (SUM)
            SUM(order_amount) AS total_amount,
            -- total_customers: 客户总数 (COUNT DISTINCT)
            COUNT(DISTINCT customer_id) FILTER (WHERE customer_id IS NOT NULL) AS total_customers
        FROM source_data
        -- GROUP BY条件
        GROUP BY stat_date, channel_code, channel_name
    ),
    calculated AS (
        -- 第三步: 计算派生字段
        SELECT 
            stat_date,
            channel_code,
            channel_name,
            total_orders,
            total_amount,
            total_customers,
            -- avg_order_amount: 平均订单金额 (公式计算)
            CASE 
                WHEN total_orders > 0 THEN ROUND(total_amount / total_orders, 2)
                ELSE 0
            END AS avg_order_amount,
            -- amount_level: 金额等级 (CASE WHEN条件判断)
            CASE 
                WHEN total_amount >= 1000000 THEN 'HIGH'
                WHEN total_amount >= 100000 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS amount_level
        FROM aggregated
    )
    SELECT 
        -- summary_id: 主键派生 (MD5)
        MD5(stat_date::TEXT || '_' || channel_code) AS summary_id,
        stat_date,
        channel_code,
        channel_name,
        total_orders,
        total_amount,
        total_customers,
        avg_order_amount,
        amount_level,
        -- etl_load_time: 系统当前时间
        CURRENT_TIMESTAMP AS etl_load_time
    FROM calculated
    -- ORDER BY: 按日期降序、金额降序
    ORDER BY stat_date DESC, total_amount DESC;
    
    GET DIAGNOSTICS OUT_INSERT_COUNT = ROW_COUNT;
    OUT_ROW_COUNT := OUT_INSERT_COUNT;
    
    -- 记录数据加载成功
    INSERT INTO etl_log.proc_execution_log (
        proc_name, mapping_id, batch_date, step, status, 
        start_time, end_time, duration_seconds, row_count, message
    ) VALUES (
        V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, V_STEP, 'SUCCESS',
        V_STEP_START_TIME, CURRENT_TIMESTAMP,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - V_STEP_START_TIME)),
        OUT_ROW_COUNT,
        FORMAT('数据加载完成。插入: %s 条', OUT_INSERT_COUNT)
    );

    -- ==========================================
    -- 步骤5: 数据质量校验
    -- ==========================================
    V_STEP := 'DATA_QUALITY';
    V_STEP_START_TIME := CURRENT_TIMESTAMP;
    
    -- 校验规则1: 主键非空检查 (summary_id)
    SELECT COUNT(*) INTO V_VALIDATION_ERROR_COUNT
    FROM dws.dws_trade_orders_summary_day
    WHERE stat_date = IN_BATCH_DATE
      AND (summary_id IS NULL OR summary_id = '');
    
    IF V_VALIDATION_ERROR_COUNT > 0 THEN
        -- 记录错误日志
        INSERT INTO etl_log.data_quality_log (
            proc_name, mapping_id, batch_date, rule_id, rule_name, 
            error_count, error_sample, check_time
        )
        SELECT 
            V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, 
            'R001', '主键非空检查',
            COUNT(*),
            STRING_AGG(DISTINCT COALESCE(summary_id, 'NULL'), ', ' ORDER BY COALESCE(summary_id, 'NULL')) FILTER (WHERE summary_id IS NOT NULL),
            CURRENT_TIMESTAMP
        FROM dws.dws_trade_orders_summary_day
        WHERE stat_date = IN_BATCH_DATE
          AND (summary_id IS NULL OR summary_id = '');
        
        -- 根据exception_strategy处理: LOG模式继续执行
        OUT_ERROR_COUNT := OUT_ERROR_COUNT + V_VALIDATION_ERROR_COUNT;
        
        RAISE NOTICE '数据质量警告: 主键非空检查发现 % 条异常数据', V_VALIDATION_ERROR_COUNT;
    END IF;
    
    -- 校验规则2: 订单金额范围检查 (total_amount >= 0)
    SELECT COUNT(*) INTO V_VALIDATION_ERROR_COUNT
    FROM dws.dws_trade_orders_summary_day
    WHERE stat_date = IN_BATCH_DATE
      AND (total_amount IS NULL OR total_amount < 0 OR total_amount > 999999999.99);
    
    IF V_VALIDATION_ERROR_COUNT > 0 THEN
        INSERT INTO etl_log.data_quality_log (
            proc_name, mapping_id, batch_date, rule_id, rule_name, 
            error_count, check_time
        )
        VALUES (
            V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE,
            'R002', '订单金额范围检查',
            V_VALIDATION_ERROR_COUNT,
            CURRENT_TIMESTAMP
        );
        
        OUT_ERROR_COUNT := OUT_ERROR_COUNT + V_VALIDATION_ERROR_COUNT;
        
        RAISE NOTICE '数据质量警告: 订单金额范围检查发现 % 条异常数据', V_VALIDATION_ERROR_COUNT;
    END IF;
    
    -- 校验规则3: 渠道代码有效性检查（外键检查）
    SELECT COUNT(*) INTO V_VALIDATION_ERROR_COUNT
    FROM dws.dws_trade_orders_summary_day t
    WHERE t.stat_date = IN_BATCH_DATE
      AND t.channel_code IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM dim.dim_channel_info c
          WHERE c.channel_code = t.channel_code
            AND c.is_valid = 'Y'
      );
    
    IF V_VALIDATION_ERROR_COUNT > 0 THEN
        INSERT INTO etl_log.data_quality_log (
            proc_name, mapping_id, batch_date, rule_id, rule_name, 
            error_count, check_time
        )
        VALUES (
            V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE,
            'R003', '渠道代码有效性检查',
            V_VALIDATION_ERROR_COUNT,
            CURRENT_TIMESTAMP
        );
        
        OUT_ERROR_COUNT := OUT_ERROR_COUNT + V_VALIDATION_ERROR_COUNT;
        
        RAISE NOTICE '数据质量警告: 渠道代码有效性检查发现 % 条异常数据', V_VALIDATION_ERROR_COUNT;
    END IF;
    
    -- 记录数据质量检查完成
    INSERT INTO etl_log.proc_execution_log (
        proc_name, mapping_id, batch_date, step, status, 
        start_time, end_time, duration_seconds, message
    ) VALUES (
        V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, V_STEP, 'SUCCESS',
        V_STEP_START_TIME, CURRENT_TIMESTAMP,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - V_STEP_START_TIME)),
        FORMAT('数据质量校验完成。发现 %s 条异常数据', OUT_ERROR_COUNT)
    );

    -- ==========================================
    -- 步骤6: 统计信息更新
    -- ==========================================
    V_STEP := 'STATISTICS';
    V_STEP_START_TIME := CURRENT_TIMESTAMP;
    
    -- 更新表统计信息
    ANALYZE dws.dws_trade_orders_summary_day;
    
    -- 如果是全量加载，执行VACUUM
    IF V_IS_FULL_LOAD THEN
        VACUUM ANALYZE dws.dws_trade_orders_summary_day;
    END IF;
    
    INSERT INTO etl_log.proc_execution_log (
        proc_name, mapping_id, batch_date, step, status, 
        start_time, end_time, duration_seconds, message
    ) VALUES (
        V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, V_STEP, 'SUCCESS',
        V_STEP_START_TIME, CURRENT_TIMESTAMP,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - V_STEP_START_TIME)),
        '统计信息更新完成'
    );

    -- ==========================================
    -- 步骤7: 完成记录
    -- ==========================================
    V_STEP := 'COMPLETE';
    V_END_TIME := CURRENT_TIMESTAMP;
    
    OUT_RET_CODE := 0;
    OUT_RET_MSG := FORMAT('处理成功。插入: %s, 删除: %s, 错误: %s, 总耗时: %s秒',
                         OUT_INSERT_COUNT, OUT_DELETE_COUNT, OUT_ERROR_COUNT,
                         ROUND(EXTRACT(EPOCH FROM (V_END_TIME - V_START_TIME)), 2));
    
    -- 记录完成日志
    INSERT INTO etl_log.proc_execution_log (
        proc_name, mapping_id, batch_date, step, status, 
        start_time, end_time, duration_seconds, row_count, message
    ) VALUES (
        V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, V_STEP, 'SUCCESS',
        V_START_TIME, V_END_TIME,
        EXTRACT(EPOCH FROM (V_END_TIME - V_START_TIME)),
        OUT_ROW_COUNT,
        OUT_RET_MSG
    );
    
    RAISE NOTICE '[%] % - %', 
        TO_CHAR(V_END_TIME, 'YYYY-MM-DD HH24:MI:SS'),
        V_STEP, OUT_RET_MSG;

EXCEPTION
    WHEN OTHERS THEN
        -- 错误处理
        V_END_TIME := CURRENT_TIMESTAMP;
        V_ERROR_CODE := SQLSTATE;
        V_ERROR_MESSAGE := SQLERRM;
        
        OUT_RET_CODE := -1;
        OUT_RET_MSG := FORMAT('错误[%s]: %s', V_ERROR_CODE, V_ERROR_MESSAGE);
        
        -- 记录错误日志
        INSERT INTO etl_log.proc_execution_log (
            proc_name, mapping_id, batch_date, step, status, 
            start_time, end_time, duration_seconds, 
            error_code, error_message, message
        ) VALUES (
            V_PROC_NAME, V_MAPPING_ID, IN_BATCH_DATE, V_STEP, 'FAILED',
            V_STEP_START_TIME, V_END_TIME,
            EXTRACT(EPOCH FROM (V_END_TIME - V_STEP_START_TIME)),
            V_ERROR_CODE, V_ERROR_MESSAGE, OUT_RET_MSG
        );
        
        -- 回滚事务
        ROLLBACK;
        
        -- 重新抛出异常
        RAISE;
END;
$$;

-- 添加存储过程注释
COMMENT ON PROCEDURE dws.SP_LOAD_DWS_TRADE_ORDERS_SUMMARY_DAY(
    DATE, BOOLEAN, INTEGER, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT
) IS 
'交易订单日汇总表加载存储过程
Mapping ID: M_DWS_ORDERS_SUM_001
加载策略: INCREMENTAL_MERGE
输入参数: 
  IN_BATCH_DATE - 批次日期
  IN_FORCE_FULL - 是否强制全量加载
输出参数:
  OUT_RET_CODE - 返回码 (0成功, 非0失败)
  OUT_RET_MSG - 返回信息
  OUT_ROW_COUNT - 处理总行数
  OUT_INSERT_COUNT - 插入行数
  OUT_UPDATE_COUNT - 更新行数
  OUT_DELETE_COUNT - 删除行数
  OUT_ERROR_COUNT - 数据质量错误行数
依赖表:
  dwd.dwd_trade_orders_detail_day (源表)
  dim.dim_channel_info (维度表)
目标表:
  dws.dws_trade_orders_summary_day
作者: ETL Automation Platform v2.0
创建时间: 2026-03-18';

-- ============================================
-- 建表语句（如果表不存在）
-- ============================================

CREATE TABLE IF NOT EXISTS dws.dws_trade_orders_summary_day (
    -- 主键
    summary_id VARCHAR(32) NOT NULL,
    -- 维度字段
    stat_date DATE NOT NULL,
    channel_code VARCHAR(20) NOT NULL,
    channel_name VARCHAR(100),
    -- 度量字段
    total_orders BIGINT NOT NULL DEFAULT 0,
    total_amount DECIMAL(18,2) NOT NULL DEFAULT 0.00,
    total_customers BIGINT NOT NULL DEFAULT 0,
    avg_order_amount DECIMAL(18,2),
    amount_level VARCHAR(20),
    -- ETL审计字段
    etl_load_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- 主键约束
    CONSTRAINT pk_dws_trade_orders_summary_day PRIMARY KEY (summary_id)
)
-- 分布键配置: HASH(stat_date, channel_code)
DISTRIBUTE BY HASH(stat_date, channel_code)
-- 分区键配置: RANGE(stat_date) 按天分区
PARTITION BY RANGE(stat_date) (
    PARTITION p202401 VALUES LESS THAN ('2024-02-01'),
    PARTITION p202402 VALUES LESS THAN ('2024-03-01'),
    PARTITION p202403 VALUES LESS THAN ('2024-04-01'),
    PARTITION p202404 VALUES LESS THAN ('2024-05-01'),
    PARTITION p202405 VALUES LESS THAN ('2024-06-01'),
    PARTITION p202406 VALUES LESS THAN ('2024-07-01'),
    PARTITION p202407 VALUES LESS THAN ('2024-08-01'),
    PARTITION p202408 VALUES LESS THAN ('2024-09-01'),
    PARTITION p202409 VALUES LESS THAN ('2024-10-01'),
    PARTITION p202410 VALUES LESS THAN ('2024-11-01'),
    PARTITION p202411 VALUES LESS THAN ('2024-12-01'),
    PARTITION p202412 VALUES LESS THAN ('2025-01-01'),
    PARTITION p_max VALUES LESS THAN (MAXVALUE)
);

-- 添加表注释
COMMENT ON TABLE dws.dws_trade_orders_summary_day IS 
'交易订单日汇总表 - 按渠道统计每日订单金额、数量、客户数
Mapping ID: M_DWS_ORDERS_SUM_001
数据层级: DWS
业务域: TRADE
更新频率: DAILY';

-- 添加字段注释
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.summary_id IS '主键：MD5(stat_date || channel_code)';
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.stat_date IS '统计日期';
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.channel_code IS '渠道代码';
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.channel_name IS '渠道名称';
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.total_orders IS '订单总数';
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.total_amount IS '订单总金额';
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.total_customers IS '客户总数（去重）';
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.avg_order_amount IS '平均订单金额';
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.amount_level IS '金额等级: HIGH/MEDIUM/LOW';
COMMENT ON COLUMN dws.dws_trade_orders_summary_day.etl_load_time IS 'ETL加载时间';

-- ============================================
-- 使用示例
-- ============================================

/*
-- 示例1: 增量加载（日常调度）
CALL dws.SP_LOAD_DWS_TRADE_ORDERS_SUMMARY_DAY(
    IN_BATCH_DATE := '2026-03-18',
    IN_FORCE_FULL := FALSE,
    OUT_RET_CODE := 0,
    OUT_RET_MSG := '',
    OUT_ROW_COUNT := 0,
    OUT_INSERT_COUNT := 0,
    OUT_UPDATE_COUNT := 0,
    OUT_DELETE_COUNT := 0,
    OUT_ERROR_COUNT := 0
);

-- 示例2: 强制全量加载（数据修复）
CALL dws.SP_LOAD_DWS_TRADE_ORDERS_SUMMARY_DAY(
    IN_BATCH_DATE := '2026-03-18',
    IN_FORCE_FULL := TRUE,
    OUT_RET_CODE := 0,
    OUT_RET_MSG := '',
    OUT_ROW_COUNT := 0,
    OUT_INSERT_COUNT := 0,
    OUT_UPDATE_COUNT := 0,
    OUT_DELETE_COUNT := 0,
    OUT_ERROR_COUNT := 0
);

-- 查看执行日志
SELECT * FROM etl_log.proc_execution_log
WHERE proc_name = 'SP_LOAD_DWS_TRADE_ORDERS_SUMMARY_DAY'
  AND batch_date = '2026-03-18'
ORDER BY start_time DESC;

-- 查看数据质量日志
SELECT * FROM etl_log.data_quality_log
WHERE proc_name = 'SP_LOAD_DWS_TRADE_ORDERS_SUMMARY_DAY'
  AND batch_date = '2026-03-18'
ORDER BY check_time DESC;

-- 验证数据
SELECT 
    stat_date,
    channel_code,
    channel_name,
    total_orders,
    total_amount,
    total_customers,
    avg_order_amount,
    amount_level
FROM dws.dws_trade_orders_summary_day
WHERE stat_date = '2026-03-18'
ORDER BY total_amount DESC;
*/
