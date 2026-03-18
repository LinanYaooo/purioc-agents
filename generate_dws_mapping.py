#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWS ETL Mapping 文档生成器
生成电商全渠道销售分析场景的完整设计文档和存储过程代码
"""

import json
from datetime import datetime

def generate_entity_mapping():
    """生成实体级Mapping配置"""
    
    entity_config = {
        "mapping_id": "M_DWS_ECOM_CHNL_SALES_001",
        "target_schema": "dws",
        "target_table": "dws_ecommerce_channel_sales_analysis_day",
        "target_table_cn": "电商渠道销售分析日汇总表",
        "table_description": "按天汇总各销售渠道的销售业绩，包含订单金额、退款金额、净销售额、客户数、商品数等多维度指标，支持渠道价值等级划分",
        "data_layer": "DWS",
        "business_domain": "ECOMMERCE",
        "responsible_person": "数据开发工程师",
        "version": "v1.0.0",
        "loading_strategy": "INCREMENTAL_MERGE",
        "schedule_type": "DAILY",
        "schedule_cron": "0 3 * * *",
        "dependency_tasks": [
            "dwd_trade_orders_detail_day",
            "dwd_order_items_detail_day", 
            "dwd_payments_detail_day",
            "dwd_refunds_detail_day",
            "dwd_inventory_detail_day",
            "dim_channel_info",
            "dim_product_info",
            "dim_customer_info",
            "dim_region_info",
            "dim_promotion_info",
            "dim_store_info",
            "dim_category_info"
        ],
        "execution_priority": 100,
        "timeout_minutes": 120,
        "retry_count": 3,
        "retry_interval_minutes": 10,
        "parallel_enabled": False,
        
        "incremental_config": {
            "watermark_field": "etl_load_time",
            "watermark_value": "${BATCH_DATE} 00:00:00",
            "incremental_condition": "etl_load_time >= '${BATCH_DATE}' AND etl_load_time < '${BATCH_DATE}'::DATE + INTERVAL '1 day'",
            "delete_before_insert": True,
            "idempotent": True
        },
        
        "distribution_key": "channel_code, stat_date",
        "distribution_type": "HASH",
        "partition_key": "stat_date",
        "partition_type": "RANGE",
        "partition_granularity": "DAY",
        "partition_retention_days": 730,
        
        "exception_strategy": "LOG",
        "null_check_enabled": True,
        "duplicate_check_enabled": True,
        "error_threshold_percent": 5.0,
        
        "validation_rules": [
            {
                "rule_id": "R001",
                "rule_name": "主键非空检查",
                "field": "analysis_id",
                "rule_type": "NOT_NULL",
                "error_action": "REJECT"
            },
            {
                "rule_id": "R002",
                "rule_name": "统计日期非空检查",
                "field": "stat_date",
                "rule_type": "NOT_NULL",
                "error_action": "REJECT"
            },
            {
                "rule_id": "R003",
                "rule_name": "渠道代码有效性检查",
                "field": "channel_code",
                "rule_type": "REFERENTIAL",
                "ref_table": "dim_channel_info",
                "ref_field": "channel_code",
                "error_action": "LOG"
            },
            {
                "rule_id": "R004",
                "rule_name": "订单金额非负检查",
                "field": "total_order_amount",
                "rule_type": "RANGE",
                "min_value": 0,
                "max_value": 9999999999.99,
                "error_action": "LOG"
            }
        ],
        
        "source_tables": [
            {"source_id": "S001", "schema": "dwd", "table_name": "dwd_trade_orders_detail_day", "alias": "ord", "table_type": "PRIMARY", "description": "订单明细事实表"},
            {"source_id": "S002", "schema": "dwd", "table_name": "dwd_order_items_detail_day", "alias": "itm", "table_type": "DETAIL", "description": "订单商品明细表"},
            {"source_id": "S003", "schema": "dwd", "table_name": "dwd_payments_detail_day", "alias": "pay", "table_type": "DETAIL", "description": "支付明细表"},
            {"source_id": "S004", "schema": "dwd", "table_name": "dwd_refunds_detail_day", "alias": "ref", "table_type": "DETAIL", "description": "退款明细表"},
            {"source_id": "S005", "schema": "dwd", "table_name": "dwd_inventory_detail_day", "alias": "inv", "table_type": "DETAIL", "description": "库存明细表"},
            {"source_id": "S006", "schema": "dim", "table_name": "dim_channel_info", "alias": "chn", "table_type": "LOOKUP", "description": "渠道维度表"},
            {"source_id": "S007", "schema": "dim", "table_name": "dim_product_info", "alias": "prd", "table_type": "LOOKUP", "description": "商品维度表"},
            {"source_id": "S008", "schema": "dim", "table_name": "dim_customer_info", "alias": "cst", "table_type": "LOOKUP", "description": "客户维度表"},
            {"source_id": "S009", "schema": "dim", "table_name": "dim_region_info", "alias": "reg", "table_type": "LOOKUP", "description": "地区维度表"},
            {"source_id": "S010", "schema": "dim", "table_name": "dim_promotion_info", "alias": "prm", "table_type": "LOOKUP", "description": "促销维度表"},
            {"source_id": "S011", "schema": "dim", "table_name": "dim_store_info", "alias": "sto", "table_type": "LOOKUP", "description": "门店维度表"},
            {"source_id": "S012", "schema": "dim", "table_name": "dim_category_info", "alias": "cat", "table_type": "LOOKUP", "description": "类目维度表"}
        ],
        
        "sql_conditions": {
            "joins": [
                {
                    "join_sequence": 1,
                    "join_type": "LEFT",
                    "table_alias": "itm",
                    "join_condition": "ord.order_id = itm.order_id AND itm.is_valid = 'Y'",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 2,
                    "join_type": "LEFT",
                    "table_alias": "pay",
                    "join_condition": "ord.order_id = pay.order_id AND pay.pay_status = 'SUCCESS'",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 3,
                    "join_type": "LEFT",
                    "table_alias": "ref",
                    "join_condition": "ord.order_id = ref.order_id AND ref.refund_status = 'COMPLETED'",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 4,
                    "join_type": "LEFT",
                    "table_alias": "inv",
                    "join_condition": "itm.product_id = inv.product_id AND inv.stat_date = ord.stat_date",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 5,
                    "join_type": "LEFT",
                    "table_alias": "chn",
                    "join_condition": "ord.channel_code = chn.channel_code AND chn.is_valid = 'Y'",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 6,
                    "join_type": "LEFT",
                    "table_alias": "prd",
                    "join_condition": "itm.product_id = prd.product_id AND prd.is_valid = 'Y'",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 7,
                    "join_type": "LEFT",
                    "table_alias": "cst",
                    "join_condition": "ord.customer_id = cst.customer_id",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 8,
                    "join_type": "LEFT",
                    "table_alias": "reg",
                    "join_condition": "ord.region_code = reg.region_code AND reg.is_valid = 'Y'",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 9,
                    "join_type": "LEFT",
                    "table_alias": "prm",
                    "join_condition": "ord.promotion_id = prm.promotion_id AND prm.is_valid = 'Y'",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 10,
                    "join_type": "LEFT",
                    "table_alias": "sto",
                    "join_condition": "ord.store_code = sto.store_code AND sto.is_valid = 'Y'",
                    "join_hint": "USE_HASH"
                },
                {
                    "join_sequence": 11,
                    "join_type": "LEFT",
                    "table_alias": "cat",
                    "join_condition": "prd.category_id = cat.category_id AND cat.is_valid = 'Y'",
                    "join_hint": "USE_HASH"
                }
            ],
            "where": [
                {
                    "sequence": 1,
                    "condition_group": 1,
                    "logic": "AND",
                    "field": "ord.order_status",
                    "operator": "IN",
                    "value": ["COMPLETED", "PAID", "SHIPPED"],
                    "value_type": "ARRAY"
                },
                {
                    "sequence": 2,
                    "condition_group": 1,
                    "logic": "AND",
                    "field": "ord.is_deleted",
                    "operator": "=",
                    "value": "N",
                    "value_type": "STRING"
                },
                {
                    "sequence": 3,
                    "condition_group": 1,
                    "logic": "AND",
                    "field": "ord.order_type",
                    "operator": "!=",
                    "value": "TEST",
                    "value_type": "STRING"
                }
            ],
            "group_by": [
                {"sequence": 1, "expression": "ord.stat_date", "alias": None},
                {"sequence": 2, "expression": "ord.channel_code", "alias": None},
                {"sequence": 3, "expression": "chn.channel_name", "alias": None},
                {"sequence": 4, "expression": "chn.channel_type", "alias": None},
                {"sequence": 5, "expression": "reg.region_name", "alias": None},
                {"sequence": 6, "expression": "cat.category_name", "alias": None}
            ],
            "having": [
                {
                    "sequence": 1,
                    "logic": "AND",
                    "condition": "COUNT(DISTINCT ord.order_id) > 0",
                    "description": "只保留有订单记录的渠道"
                }
            ],
            "order_by": [
                {"sequence": 1, "expression": "ord.stat_date", "direction": "DESC"},
                {"sequence": 2, "expression": "total_net_amount", "direction": "DESC"}
            ]
        }
    }
    
    return entity_config

def generate_attribute_mapping():
    """生成属性级Mapping配置"""
    
    attributes = [
        {
            "seq": 1,
            "target_field": "analysis_id",
            "target_type": "VARCHAR",
            "target_precision": "50",
            "nullable": "N",
            "is_pk": "Y",
            "gen_type": "DERIVED",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "MD5(stat_date || '_' || channel_code)",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "STRING",
            "field_length": "50",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "主键：日期+渠道MD5"
        },
        {
            "seq": 2,
            "target_field": "stat_date",
            "target_type": "DATE",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "DIRECT",
            "source_schema": "dwd",
            "source_table": "dwd_trade_orders_detail_day",
            "source_alias": "ord",
            "source_field": "stat_date",
            "process_logic": "直接映射：ord.stat_date",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "DATE",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "统计日期"
        },
        {
            "seq": 3,
            "target_field": "channel_code",
            "target_type": "VARCHAR",
            "target_precision": "20",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "DIRECT",
            "source_schema": "dwd",
            "source_table": "dwd_trade_orders_detail_day",
            "source_alias": "ord",
            "source_field": "channel_code",
            "process_logic": "直接映射：ord.channel_code",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "STRING",
            "field_length": "20",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "渠道代码"
        },
        {
            "seq": 4,
            "target_field": "channel_name",
            "target_type": "VARCHAR",
            "target_precision": "100",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "LOOKUP",
            "source_schema": "dim",
            "source_table": "dim_channel_info",
            "source_alias": "chn",
            "source_field": "channel_name",
            "process_logic": "COALESCE(chn.channel_name, '未知渠道')",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "STRING",
            "field_length": "100",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "渠道名称"
        },
        {
            "seq": 5,
            "target_field": "channel_type",
            "target_type": "VARCHAR",
            "target_precision": "20",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "LOOKUP",
            "source_schema": "dim",
            "source_table": "dim_channel_info",
            "source_alias": "chn",
            "source_field": "channel_type",
            "process_logic": "COALESCE(chn.channel_type, 'OTHER')",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "STRING",
            "field_length": "20",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "渠道类型：APP/小程序/H5/线下"
        },
        {
            "seq": 6,
            "target_field": "region_name",
            "target_type": "VARCHAR",
            "target_precision": "100",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "LOOKUP",
            "source_schema": "dim",
            "source_table": "dim_region_info",
            "source_alias": "reg",
            "source_field": "region_name",
            "process_logic": "COALESCE(reg.region_name, '未知地区')",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "STRING",
            "field_length": "100",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "地区名称"
        },
        {
            "seq": 7,
            "target_field": "category_name",
            "target_type": "VARCHAR",
            "target_precision": "100",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "LOOKUP",
            "source_schema": "dim",
            "source_table": "dim_category_info",
            "source_alias": "cat",
            "source_field": "category_name",
            "process_logic": "COALESCE(cat.category_name, '未知类目')",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "STRING",
            "field_length": "100",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "类目名称"
        },
        {
            "seq": 8,
            "target_field": "total_orders",
            "target_type": "BIGINT",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "AGGREGATE",
            "source_schema": "dwd",
            "source_table": "dwd_trade_orders_detail_day",
            "source_alias": "ord",
            "source_field": "order_id",
            "process_logic": "COUNT(DISTINCT ord.order_id)",
            "is_dedup": "Y",
            "owner": "数据开发工程师",
            "field_type": "BIGINT",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "订单总数（去重）"
        },
        {
            "seq": 9,
            "target_field": "total_order_amount",
            "target_type": "DECIMAL",
            "target_precision": "18,2",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "AGGREGATE",
            "source_schema": "dwd",
            "source_table": "dwd_trade_orders_detail_day",
            "source_alias": "ord",
            "source_field": "order_amount",
            "process_logic": "SUM(ord.order_amount)",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "DECIMAL",
            "field_length": "18,2",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "订单总金额"
        },
        {
            "seq": 10,
            "target_field": "total_refund_amount",
            "target_type": "DECIMAL",
            "target_precision": "18,2",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "AGGREGATE",
            "source_schema": "dwd",
            "source_table": "dwd_refunds_detail_day",
            "source_alias": "ref",
            "source_field": "refund_amount",
            "process_logic": "COALESCE(SUM(ref.refund_amount), 0)",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "DECIMAL",
            "field_length": "18,2",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "退款总金额"
        },
        {
            "seq": 11,
            "target_field": "total_net_amount",
            "target_type": "DECIMAL",
            "target_precision": "18,2",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "CALCULATE",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "total_order_amount - total_refund_amount",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "DECIMAL",
            "field_length": "18,2",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "净销售额"
        },
        {
            "seq": 12,
            "target_field": "total_customers",
            "target_type": "BIGINT",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "AGGREGATE",
            "source_schema": "dwd",
            "source_table": "dwd_trade_orders_detail_day",
            "source_alias": "ord",
            "source_field": "customer_id",
            "process_logic": "COUNT(DISTINCT ord.customer_id)",
            "is_dedup": "Y",
            "owner": "数据开发工程师",
            "field_type": "BIGINT",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "客户总数（去重）"
        },
        {
            "seq": 13,
            "target_field": "total_products",
            "target_type": "BIGINT",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "AGGREGATE",
            "source_schema": "dwd",
            "source_table": "dwd_order_items_detail_day",
            "source_alias": "itm",
            "source_field": "product_id",
            "process_logic": "COUNT(DISTINCT itm.product_id)",
            "is_dedup": "Y",
            "owner": "数据开发工程师",
            "field_type": "BIGINT",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "商品总数（去重）"
        },
        {
            "seq": 14,
            "target_field": "total_quantity",
            "target_type": "BIGINT",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "AGGREGATE",
            "source_schema": "dwd",
            "source_table": "dwd_order_items_detail_day",
            "source_alias": "itm",
            "source_field": "quantity",
            "process_logic": "SUM(itm.quantity)",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "BIGINT",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "商品总数量"
        },
        {
            "seq": 15,
            "target_field": "member_customers",
            "target_type": "BIGINT",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "AGGREGATE",
            "source_schema": "dwd",
            "source_table": "dwd_trade_orders_detail_day",
            "source_alias": "ord",
            "source_field": "customer_id",
            "process_logic": "COUNT(DISTINCT CASE WHEN cst.customer_type = 'MEMBER' THEN ord.customer_id END)",
            "is_dedup": "Y",
            "owner": "数据开发工程师",
            "field_type": "BIGINT",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "会员客户数"
        },
        {
            "seq": 16,
            "target_field": "non_member_customers",
            "target_type": "BIGINT",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "CALCULATE",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "total_customers - member_customers",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "BIGINT",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "非会员客户数"
        },
        {
            "seq": 17,
            "target_field": "avg_order_amount",
            "target_type": "DECIMAL",
            "target_precision": "18,2",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "CALCULATE",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "CASE WHEN total_orders > 0 THEN total_net_amount / total_orders ELSE 0 END",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "DECIMAL",
            "field_length": "18,2",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "平均订单金额"
        },
        {
            "seq": 18,
            "target_field": "avg_customer_value",
            "target_type": "DECIMAL",
            "target_precision": "18,2",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "CALCULATE",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "CASE WHEN total_customers > 0 THEN total_net_amount / total_customers ELSE 0 END",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "DECIMAL",
            "field_length": "18,2",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "客户平均消费金额"
        },
        {
            "seq": 19,
            "target_field": "channel_value_level",
            "target_type": "VARCHAR",
            "target_precision": "10",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "CASE",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "CASE WHEN total_net_amount >= 1000000 THEN 'HIGH' WHEN total_net_amount >= 100000 THEN 'MEDIUM' ELSE 'LOW' END",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "STRING",
            "field_length": "10",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "渠道价值等级：高/中/低"
        },
        {
            "seq": 20,
            "target_field": "avg_inventory_turnover",
            "target_type": "DECIMAL",
            "target_precision": "10,2",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "CALCULATE",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "CASE WHEN COALESCE(SUM(inv.avg_inventory_qty), 0) > 0 THEN SUM(total_quantity) / SUM(inv.avg_inventory_qty) ELSE 0 END",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "DECIMAL",
            "field_length": "10,2",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "平均库存周转率"
        },
        {
            "seq": 21,
            "target_field": "promotion_order_count",
            "target_type": "BIGINT",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "AGGREGATE",
            "source_schema": "dwd",
            "source_table": "dwd_trade_orders_detail_day",
            "source_alias": "ord",
            "source_field": "order_id",
            "process_logic": "COUNT(DISTINCT CASE WHEN ord.promotion_id IS NOT NULL THEN ord.order_id END)",
            "is_dedup": "Y",
            "owner": "数据开发工程师",
            "field_type": "BIGINT",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "促销订单数"
        },
        {
            "seq": 22,
            "target_field": "promotion_order_rate",
            "target_type": "DECIMAL",
            "target_precision": "5,2",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "CALCULATE",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "CASE WHEN total_orders > 0 THEN promotion_order_count * 100.0 / total_orders ELSE 0 END",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "DECIMAL",
            "field_length": "5,2",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "促销订单占比(%)"
        },
        {
            "seq": 23,
            "target_field": "first_time_customers",
            "target_type": "BIGINT",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "AGGREGATE",
            "source_schema": "dwd",
            "source_table": "dwd_trade_orders_detail_day",
            "source_alias": "ord",
            "source_field": "customer_id",
            "process_logic": "COUNT(DISTINCT CASE WHEN ord.is_first_order = 'Y' THEN ord.customer_id END)",
            "is_dedup": "Y",
            "owner": "数据开发工程师",
            "field_type": "BIGINT",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "首单客户数"
        },
        {
            "seq": 24,
            "target_field": "repurchase_rate",
            "target_type": "DECIMAL",
            "target_precision": "5,2",
            "nullable": "Y",
            "is_pk": "",
            "gen_type": "CALCULATE",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "CASE WHEN total_customers > 0 THEN (total_customers - first_time_customers) * 100.0 / total_customers ELSE 0 END",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "DECIMAL",
            "field_length": "5,2",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "复购率(%)"
        },
        {
            "seq": 25,
            "target_field": "etl_load_time",
            "target_type": "TIMESTAMP",
            "target_precision": "",
            "nullable": "N",
            "is_pk": "",
            "gen_type": "SYSTEM",
            "source_schema": "",
            "source_table": "",
            "source_alias": "",
            "source_field": "",
            "process_logic": "CURRENT_TIMESTAMP",
            "is_dedup": "",
            "owner": "数据开发工程师",
            "field_type": "TIMESTAMP",
            "field_length": "",
            "change_log": "2026-03-18 v1.0.0 初始创建",
            "remark": "ETL加载时间"
        }
    ]
    
    return attributes

def generate_create_table_sql():
    """生成建表语句"""
    
    sql = """-- ============================================
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
"""
    
    return sql

def generate_stored_procedure_sql():
    """生成存储过程代码"""
    
    sql = """-- ============================================
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
"""
    
    return sql

if __name__ == "__main__":
    # 生成配置
    entity_config = generate_entity_mapping()
    attributes = generate_attribute_mapping()
    create_table_sql = generate_create_table_sql()
    procedure_sql = generate_stored_procedure_sql()
    
    # 输出JSON配置（用于Excel填充）
    print("="*80)
    print("实体级Mapping配置 (JSON格式，用于填充Excel)")
    print("="*80)
    print(json.dumps(entity_config, ensure_ascii=False, indent=2))
    
    print("\n" + "="*80)
    print("属性级Mapping配置 (JSON格式，用于填充Excel)")
    print("="*80)
    print(json.dumps(attributes, ensure_ascii=False, indent=2))
    
    # 保存SQL文件
    with open("01_create_table_dws_ecommerce_channel_sales_analysis_day.sql", "w", encoding="utf-8") as f:
        f.write(create_table_sql)
    
    with open("02_sp_load_dws_ecommerce_channel_sales_analysis_day.sql", "w", encoding="utf-8") as f:
        f.write(procedure_sql)
    
    print("\n" + "="*80)
    print("SQL文件已生成")
    print("="*80)
    print("1. 01_create_table_dws_ecommerce_channel_sales_analysis_day.sql - 建表语句")
    print("2. 02_sp_load_dws_ecommerce_channel_sales_analysis_day.sql - 存储过程")
    print("\n请使用上述JSON数据填充Excel文档：")
    print("- 实体级Mapping.xlsx")
    print("- 属性级Mapping.xlsx")
