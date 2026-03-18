-- ============================================
-- 表名称: dws_ecommerce_channel_sales_analysis_day
-- 表中文名: 电商渠道销售分析日汇总表
-- 说明: 按天汇总各销售渠道的销售业绩指标
-- 作者: 自动生成 (DWS ETL自动化平台 v1.0)
-- 创建时间: 2026-03-18
-- 版本: v1.0.0
-- ============================================

CREATE TABLE IF NOT EXISTS dws.dws_ecommerce_channel_sales_analysis_day (
    -- 主键和基础维度
    analysis_id                     VARCHAR(50)         NOT NULL,
    stat_date                       DATE                NOT NULL,
    channel_code                    VARCHAR(20)         NOT NULL,
    channel_name                    VARCHAR(100)        NULL,
    channel_type                    VARCHAR(20)         NULL,
    region_name                     VARCHAR(100)        NULL,
    category_name                   VARCHAR(100)        NULL,
    
    -- 订单相关指标
    total_orders                    BIGINT              NOT NULL DEFAULT 0,
    total_order_amount              DECIMAL(18,2)       NOT NULL DEFAULT 0.00,
    total_refund_amount             DECIMAL(18,2)       NOT NULL DEFAULT 0.00,
    total_net_amount                DECIMAL(18,2)       NOT NULL DEFAULT 0.00,
    
    -- 客户相关指标
    total_customers                 BIGINT              NOT NULL DEFAULT 0,
    member_customers                BIGINT              NOT NULL DEFAULT 0,
    non_member_customers            BIGINT              NOT NULL DEFAULT 0,
    first_time_customers            BIGINT              NOT NULL DEFAULT 0,
    repurchase_rate                 DECIMAL(5,2)        NULL,
    
    -- 商品相关指标
    total_products                  BIGINT              NOT NULL DEFAULT 0,
    total_quantity                  BIGINT              NOT NULL DEFAULT 0,
    avg_inventory_turnover          DECIMAL(10,2)       NULL,
    
    -- 金额相关指标
    avg_order_amount                DECIMAL(18,2)       NULL,
    avg_customer_value              DECIMAL(18,2)       NULL,
    
    -- 等级和分类
    channel_value_level             VARCHAR(10)         NULL,
    
    -- 促销相关指标
    promotion_order_count           BIGINT              NOT NULL DEFAULT 0,
    promotion_order_rate            DECIMAL(5,2)        NULL,
    
    -- ETL审计字段
    etl_batch_date                  DATE                NOT NULL,
    etl_load_time                   TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- 主键约束
    CONSTRAINT pk_dws_ecommerce_channel_sales_analysis_day PRIMARY KEY (analysis_id)
)
DISTRIBUTE BY HASH(analysis_id)
PARTITION BY RANGE(stat_date)
(
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
    PARTITION p202501 VALUES LESS THAN ('2025-02-01'),
    PARTITION p202502 VALUES LESS THAN ('2025-03-01'),
    PARTITION p202503 VALUES LESS THAN ('2025-04-01'),
    PARTITION p202504 VALUES LESS THAN ('2025-05-01'),
    PARTITION p202505 VALUES LESS THAN ('2025-06-01'),
    PARTITION p202506 VALUES LESS THAN ('2025-07-01'),
    PARTITION p_max VALUES LESS THAN (MAXVALUE)
);

-- 表注释
COMMENT ON TABLE dws.dws_ecommerce_channel_sales_analysis_day IS '电商渠道销售分析日汇总表：按天汇总各销售渠道的销售业绩，包含订单金额、退款金额、净销售额、客户数、商品数等多维度指标';

-- 字段注释
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.analysis_id IS '分析记录主键：MD5(stat_date || '_' || channel_code)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.stat_date IS '统计日期';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.channel_code IS '渠道代码';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.channel_name IS '渠道名称：关联dim_channel_info获取';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.channel_type IS '渠道类型：APP/小程序/H5/线下/OTHER';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.region_name IS '地区名称：关联dim_region_info获取';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.category_name IS '类目名称：关联dim_category_info获取';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.total_orders IS '订单总数：COUNT(DISTINCT ord.order_id)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.total_order_amount IS '订单总金额：SUM(ord.order_amount)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.total_refund_amount IS '退款总金额：COALESCE(SUM(ref.refund_amount), 0)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.total_net_amount IS '净销售额：total_order_amount - total_refund_amount';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.total_customers IS '客户总数：COUNT(DISTINCT ord.customer_id)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.member_customers IS '会员客户数：COUNT(DISTINCT CASE WHEN cst.customer_type = MEMBER THEN ord.customer_id END)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.non_member_customers IS '非会员客户数：total_customers - member_customers';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.first_time_customers IS '首单客户数：COUNT(DISTINCT CASE WHEN ord.is_first_order = Y THEN ord.customer_id END)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.repurchase_rate IS '复购率：(total_customers - first_time_customers) / total_customers * 100';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.total_products IS '商品总数：COUNT(DISTINCT itm.product_id)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.total_quantity IS '商品总数量：SUM(itm.quantity)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.avg_inventory_turnover IS '平均库存周转率';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.avg_order_amount IS '平均订单金额：total_net_amount / total_orders';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.avg_customer_value IS '客户平均消费金额：total_net_amount / total_customers';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.channel_value_level IS '渠道价值等级：HIGH(>=100万)/MEDIUM(>=10万)/LOW';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.promotion_order_count IS '促销订单数：COUNT(DISTINCT CASE WHEN ord.promotion_id IS NOT NULL THEN ord.order_id END)';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.promotion_order_rate IS '促销订单占比：promotion_order_count / total_orders * 100';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.etl_batch_date IS 'ETL批次日期';
COMMENT ON COLUMN dws.dws_ecommerce_channel_sales_analysis_day.etl_load_time IS 'ETL加载时间';

-- 创建索引
CREATE INDEX idx_dws_ecommerce_channel_sales_analysis_day_01 ON dws.dws_ecommerce_channel_sales_analysis_day(stat_date);
CREATE INDEX idx_dws_ecommerce_channel_sales_analysis_day_02 ON dws.dws_ecommerce_channel_sales_analysis_day(channel_code);
CREATE INDEX idx_dws_ecommerce_channel_sales_analysis_day_03 ON dws.dws_ecommerce_channel_sales_analysis_day(channel_type);
CREATE INDEX idx_dws_ecommerce_channel_sales_analysis_day_04 ON dws.dws_ecommerce_channel_sales_analysis_day(etl_batch_date);
