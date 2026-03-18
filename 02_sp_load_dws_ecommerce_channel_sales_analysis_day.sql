-- ============================================
-- 存储过程名称: sp_load_dws_ecommerce_channel_sales_analysis_day
-- 目标表: dws.dws_ecommerce_channel_sales_analysis_day
-- 作者: 自动生成 (DWS ETL自动化平台 v1.0)
-- 创建时间: 2026-03-18
-- 版本: v1.0.0
-- 说明: 电商渠道销售分析日汇总表数据加载
-- ============================================

CREATE OR REPLACE PROCEDURE dws.sp_load_dws_ecommerce_channel_sales_analysis_day(
    IN p_batch_date DATE,              -- 批次日期
    IN p_force_full BOOLEAN DEFAULT FALSE  -- 是否强制全量
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- 变量声明
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_end_time TIMESTAMP;
    v_row_count INTEGER := 0;
    v_error_count INTEGER := 0;
    v_proc_name VARCHAR(100) := 'sp_load_dws_ecommerce_channel_sales_analysis_day';
    v_step VARCHAR(100);
    
    -- 加载策略
    v_loading_strategy VARCHAR(20) := 'INCREMENTAL_MERGE';  -- FULL/INCREMENTAL/SCD2
    v_is_full BOOLEAN;
    
    -- 统计信息
    v_source_count INTEGER := 0;
    v_insert_count INTEGER := 0;
    v_update_count INTEGER := 0;
    v_delete_count INTEGER := 0;

BEGIN
    -- ==========================================
    -- 步骤1: 初始化与参数校验
    -- ==========================================
    v_step := 'INIT';
    
    -- 记录开始日志
    INSERT INTO etl_log.proc_execution_log (
        proc_name, batch_date, start_time, status, message
    ) VALUES (
        v_proc_name, p_batch_date, v_start_time, 'RUNNING', 
        '存储过程开始执行，加载策略: ' || v_loading_strategy
    );
    
    -- 确定加载方式
    v_is_full := v_loading_strategy = 'FULL' OR p_force_full;
    
    -- 参数校验
    IF p_batch_date IS NULL THEN
        RAISE EXCEPTION '批次日期不能为空';
    END IF;

    -- ==========================================
    -- 步骤2: 依赖检查
    -- ==========================================
    v_step := 'DEPENDENCY_CHECK';
    
    -- 检查上游任务是否完成
    PERFORM etl_check.check_dependencies(
        ARRAY[
            'dwd_trade_orders_detail_day',
            'dwd_order_items_detail_day',
            'dwd_payments_detail_day',
            'dwd_refunds_detail_day',
            'dwd_inventory_detail_day',
            'dim_channel_info',
            'dim_product_info',
            'dim_customer_info',
            'dim_region_info',
            'dim_promotion_info',
            'dim_store_info',
            'dim_category_info'
        ],
        p_batch_date
    );

    -- ==========================================
    -- 步骤3: 数据预处理（清空/删除历史）
    -- ==========================================
    v_step := 'PREPROCESS';
    
    IF v_is_full THEN
        -- 全量加载：清空目标表
        TRUNCATE TABLE dws.dws_ecommerce_channel_sales_analysis_day;
        
        INSERT INTO etl_log.proc_execution_log (proc_name, batch_date, step, message)
        VALUES (v_proc_name, p_batch_date, v_step, '全量加载：已清空目标表');
    ELSE
        -- 增量加载：删除当日已有数据（幂等处理）
        DELETE FROM dws.dws_ecommerce_channel_sales_analysis_day
        WHERE etl_batch_date = p_batch_date;
        
        GET DIAGNOSTICS v_delete_count = ROW_COUNT;
        
        INSERT INTO etl_log.proc_execution_log (proc_name, batch_date, step, message)
        VALUES (v_proc_name, p_batch_date, v_step, 
                '增量加载：已删除 ' || v_delete_count || ' 条当日数据');
    END IF;

    -- ==========================================
    -- 步骤4: 数据加载
    -- ==========================================
    v_step := 'DATA_LOAD';
    
    -- 使用CTE进行复杂的数据加工，包括去重、关联、聚合
    INSERT INTO dws.dws_ecommerce_channel_sales_analysis_day (
        analysis_id,
        stat_date,
        channel_code,
        channel_name,
        channel_type,
        region_name,
        category_name,
        total_orders,
        total_order_amount,
        total_refund_amount,
        total_net_amount,
        total_customers,
        member_customers,
        non_member_customers,
        first_time_customers,
        repurchase_rate,
        total_products,
        total_quantity,
        avg_inventory_turnover,
        avg_order_amount,
        avg_customer_value,
        channel_value_level,
        promotion_order_count,
        promotion_order_rate,
        etl_batch_date,
        etl_load_time
    )
    WITH order_dedup AS (
        -- 第一层CTE：订单明细去重（取每个订单最新的一条记录）
        SELECT 
            ord.order_id,
            ord.stat_date,
            ord.channel_code,
            ord.region_code,
            ord.customer_id,
            ord.order_amount,
            ord.order_status,
            ord.is_deleted,
            ord.order_type,
            ord.promotion_id,
            ord.store_code,
            ord.is_first_order,
            ord.etl_load_time,
            ROW_NUMBER() OVER (
                PARTITION BY ord.order_id 
                ORDER BY ord.etl_load_time DESC
            ) AS rn
        FROM dwd.dwd_trade_orders_detail_day ord
        WHERE ord.stat_date = p_batch_date
          AND ord.order_status IN ('COMPLETED', 'PAID', 'SHIPPED')
          AND ord.is_deleted = 'N'
          AND ord.order_type != 'TEST'
    ),
    item_agg AS (
        -- 第二层CTE：订单商品聚合
        SELECT 
            itm.order_id,
            itm.product_id,
            SUM(itm.quantity) AS quantity,
            ROW_NUMBER() OVER (
                PARTITION BY itm.order_id, itm.product_id 
                ORDER BY itm.etl_load_time DESC
            ) AS rn
        FROM dwd.dwd_order_items_detail_day itm
        WHERE itm.is_valid = 'Y'
        GROUP BY itm.order_id, itm.product_id, itm.etl_load_time
    ),
    refund_agg AS (
        -- 第三层CTE：退款金额聚合
        SELECT 
            ref.order_id,
            COALESCE(SUM(ref.refund_amount), 0) AS refund_amount
        FROM dwd.dwd_refunds_detail_day ref
        WHERE ref.refund_status = 'COMPLETED'
          AND ref.stat_date = p_batch_date
        GROUP BY ref.order_id
    ),
    inventory_agg AS (
        -- 第四层CTE：库存数据聚合
        SELECT 
            inv.product_id,
            inv.stat_date,
            AVG(inv.inventory_qty) AS avg_inventory_qty
        FROM dwd.dwd_inventory_detail_day inv
        WHERE inv.stat_date = p_batch_date
        GROUP BY inv.product_id, inv.stat_date
    ),
    base_data AS (
        -- 第五层CTE：基础数据关联
        SELECT 
            d.order_id,
            d.stat_date,
            d.channel_code,
            d.region_code,
            d.customer_id,
            d.order_amount,
            d.promotion_id,
            d.store_code,
            d.is_first_order,
            COALESCE(rf.refund_amount, 0) AS refund_amount,
            ia.product_id,
            ia.quantity,
            COALESCE(inv.avg_inventory_qty, 0) AS avg_inventory_qty
        FROM order_dedup d
        LEFT JOIN refund_agg rf ON d.order_id = rf.order_id
        LEFT JOIN item_agg ia ON d.order_id = ia.order_id AND ia.rn = 1
        LEFT JOIN inventory_agg inv ON ia.product_id = inv.product_id AND inv.stat_date = d.stat_date
        WHERE d.rn = 1
    ),
    channel_agg AS (
        -- 第六层CTE：渠道维度聚合计算
        SELECT 
            b.stat_date,
            b.channel_code,
            COALESCE(chn.channel_name, '未知渠道') AS channel_name,
            COALESCE(chn.channel_type, 'OTHER') AS channel_type,
            COALESCE(reg.region_name, '未知地区') AS region_name,
            COALESCE(cat.category_name, '未知类目') AS category_name,
            
            -- 订单相关指标
            COUNT(DISTINCT b.order_id) AS total_orders,
            SUM(b.order_amount) AS total_order_amount,
            SUM(b.refund_amount) AS total_refund_amount,
            SUM(b.order_amount) - SUM(b.refund_amount) AS total_net_amount,
            
            -- 客户相关指标
            COUNT(DISTINCT b.customer_id) AS total_customers,
            COUNT(DISTINCT CASE WHEN cst.customer_type = 'MEMBER' THEN b.customer_id END) AS member_customers,
            COUNT(DISTINCT CASE WHEN b.is_first_order = 'Y' THEN b.customer_id END) AS first_time_customers,
            
            -- 商品相关指标
            COUNT(DISTINCT b.product_id) AS total_products,
            SUM(b.quantity) AS total_quantity,
            CASE WHEN SUM(b.avg_inventory_qty) > 0 
                 THEN SUM(b.quantity) / SUM(b.avg_inventory_qty) 
                 ELSE 0 
            END AS avg_inventory_turnover,
            
            -- 促销相关指标
            COUNT(DISTINCT CASE WHEN b.promotion_id IS NOT NULL THEN b.order_id END) AS promotion_order_count
            
        FROM base_data b
        LEFT JOIN dim.dim_channel_info chn ON b.channel_code = chn.channel_code AND chn.is_valid = 'Y'
        LEFT JOIN dim.dim_customer_info cst ON b.customer_id = cst.customer_id
        LEFT JOIN dim.dim_region_info reg ON b.region_code = reg.region_code AND reg.is_valid = 'Y'
        LEFT JOIN dim.dim_product_info prd ON b.product_id = prd.product_id AND prd.is_valid = 'Y'
        LEFT JOIN dim.dim_category_info cat ON prd.category_id = cat.category_id AND cat.is_valid = 'Y'
        GROUP BY 
            b.stat_date,
            b.channel_code,
            COALESCE(chn.channel_name, '未知渠道'),
            COALESCE(chn.channel_type, 'OTHER'),
            COALESCE(reg.region_name, '未知地区'),
            COALESCE(cat.category_name, '未知类目')
        HAVING COUNT(DISTINCT b.order_id) > 0
    )
    SELECT 
        -- 主键
        MD5(ca.stat_date::TEXT || '_' || ca.channel_code) AS analysis_id,
        
        -- 维度字段
        ca.stat_date,
        ca.channel_code,
        ca.channel_name,
        ca.channel_type,
        ca.region_name,
        ca.category_name,
        
        -- 订单指标
        ca.total_orders,
        ca.total_order_amount,
        ca.total_refund_amount,
        ca.total_net_amount,
        
        -- 客户指标
        ca.total_customers,
        ca.member_customers,
        ca.total_customers - ca.member_customers AS non_member_customers,
        ca.first_time_customers,
        CASE WHEN ca.total_customers > 0 
             THEN (ca.total_customers - ca.first_time_customers) * 100.0 / ca.total_customers 
             ELSE 0 
        END AS repurchase_rate,
        
        -- 商品指标
        ca.total_products,
        ca.total_quantity,
        ca.avg_inventory_turnover,
        
        -- 金额指标
        CASE WHEN ca.total_orders > 0 
             THEN ca.total_net_amount / ca.total_orders 
             ELSE 0 
        END AS avg_order_amount,
        CASE WHEN ca.total_customers > 0 
             THEN ca.total_net_amount / ca.total_customers 
             ELSE 0 
        END AS avg_customer_value,
        
        -- 等级划分
        CASE 
            WHEN ca.total_net_amount >= 1000000 THEN 'HIGH'
            WHEN ca.total_net_amount >= 100000 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS channel_value_level,
        
        -- 促销指标
        ca.promotion_order_count,
        CASE WHEN ca.total_orders > 0 
             THEN ca.promotion_order_count * 100.0 / ca.total_orders 
             ELSE 0 
        END AS promotion_order_rate,
        
        -- ETL字段
        p_batch_date AS etl_batch_date,
        CURRENT_TIMESTAMP AS etl_load_time
        
    FROM channel_agg ca
    ORDER BY ca.stat_date DESC, ca.total_net_amount DESC;
    
    GET DIAGNOSTICS v_insert_count = ROW_COUNT;
    v_row_count := v_insert_count;

    -- ==========================================
    -- 步骤5: 数据质量校验
    -- ==========================================
    v_step := 'DATA_QUALITY';
    
    -- 校验1: 主键非空检查
    SELECT COUNT(*) INTO v_error_count
    FROM dws.dws_ecommerce_channel_sales_analysis_day
    WHERE etl_batch_date = p_batch_date
      AND (analysis_id IS NULL OR analysis_id = '');
    
    IF v_error_count > 0 THEN
        INSERT INTO etl_log.data_quality_log (
            table_name, batch_date, rule_name, error_count, error_sample
        )
        SELECT 
            'dws_ecommerce_channel_sales_analysis_day', p_batch_date, '主键非空检查', v_error_count,
            string_agg(DISTINCT analysis_id, ', ' ORDER BY analysis_id)
        FROM dws.dws_ecommerce_channel_sales_analysis_day
        WHERE etl_batch_date = p_batch_date
          AND (analysis_id IS NULL OR analysis_id = '')
        LIMIT 10;
    END IF;
    
    -- 校验2: 订单金额非负检查
    SELECT COUNT(*) INTO v_error_count
    FROM dws.dws_ecommerce_channel_sales_analysis_day
    WHERE etl_batch_date = p_batch_date
      AND total_order_amount < 0;
    
    IF v_error_count > 0 THEN
        INSERT INTO etl_log.data_quality_log (
            table_name, batch_date, rule_name, error_count, error_sample
        )
        SELECT 
            'dws_ecommerce_channel_sales_analysis_day', p_batch_date, '订单金额非负检查', v_error_count,
            string_agg(DISTINCT channel_code, ', ' ORDER BY channel_code)
        FROM dws.dws_ecommerce_channel_sales_analysis_day
        WHERE etl_batch_date = p_batch_date
          AND total_order_amount < 0
        LIMIT 10;
    END IF;
    
    -- 校验3: 净销售额计算正确性检查
    SELECT COUNT(*) INTO v_error_count
    FROM dws.dws_ecommerce_channel_sales_analysis_day
    WHERE etl_batch_date = p_batch_date
      AND ABS(total_net_amount - (total_order_amount - total_refund_amount)) > 0.01;
    
    IF v_error_count > 0 THEN
        RAISE NOTICE '警告：发现 % 条记录的净销售额计算可能有误', v_error_count;
    END IF;

    -- ==========================================
    -- 步骤6: 统计与VACUUM
    -- ==========================================
    v_step := 'STATISTICS';
    
    -- 更新统计信息
    ANALYZE dws.dws_ecommerce_channel_sales_analysis_day;
    
    -- 如果是全量加载，执行VACUUM
    IF v_is_full THEN
        VACUUM dws.dws_ecommerce_channel_sales_analysis_day;
    END IF;

    -- ==========================================
    -- 步骤7: 完成记录
    -- ==========================================
    v_end_time := CURRENT_TIMESTAMP;
    
    INSERT INTO etl_log.proc_execution_log (
        proc_name, batch_date, step, status, end_time, 
        duration_seconds, row_count, message
    ) VALUES (
        v_proc_name, p_batch_date, 'COMPLETE', 'SUCCESS', v_end_time,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time)),
        v_row_count,
        format('处理完成：插入 %s 条，更新 %s 条，耗时 %s 秒',
               v_insert_count, v_update_count, 
               EXTRACT(EPOCH FROM (v_end_time - v_start_time)))
    );

EXCEPTION
    WHEN OTHERS THEN
        -- 错误处理
        v_end_time := CURRENT_TIMESTAMP;
        
        INSERT INTO etl_log.proc_execution_log (
            proc_name, batch_date, step, status, end_time,
            error_code, error_message, duration_seconds
        ) VALUES (
            v_proc_name, p_batch_date, v_step, 'FAILED', v_end_time,
            SQLSTATE, SQLERRM,
            EXTRACT(EPOCH FROM (v_end_time - v_start_time))
        );
        
        -- 重新抛出异常
        RAISE;
END;
$$;

-- 注释说明
COMMENT ON PROCEDURE dws.sp_load_dws_ecommerce_channel_sales_analysis_day(DATE, BOOLEAN) IS 
'电商渠道销售分析日汇总表数据加载存储过程：支持全量和增量加载，包含复杂的多表JOIN关联、聚合计算、数据质量校验';
