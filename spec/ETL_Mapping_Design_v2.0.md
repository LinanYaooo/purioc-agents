# 完备版ETL Mapping设计文档

## 文档说明

本文档是根据《DWS ETL存储过程自动化开发 - 启动评估报告》中识别的缺失项，补充完善后的Mapping设计规范。

**版本**: v2.0  
**适用场景**: DWS数据仓库ETL存储过程自动化开发  
**设计原则**: 结构化配置替代文本描述，支持全自动代码生成

---

## 一、实体级Mapping（目标表定义）

### 1.1 基础信息

| 字段名 | 类型 | 必填 | 说明 | 示例值 |
|-------|------|------|------|--------|
| mapping_id | VARCHAR(50) | Y | 唯一标识 | M_DWS_ORDERS_SUM_001 |
| target_schema | VARCHAR(30) | Y | 目标schema | dws |
| target_table | VARCHAR(100) | Y | 目标物理表名 | dws_trade_orders_summary_day |
| target_table_cn | VARCHAR(200) | N | 目标表中文名 | 交易订单日汇总表 |
| table_description | TEXT | N | 表用途说明 | 按天汇总各渠道订单金额、数量、客户数 |
| data_layer | ENUM | Y | 数据层级 | DWS |
| business_domain | VARCHAR(50) | Y | 业务域 | TRADE |
| responsible_person | VARCHAR(50) | Y | 责任人 | 张三 |
| version | VARCHAR(10) | Y | 版本号 | v1.0.0 |

### 1.2 执行策略配置（P0 - 新增）

| 字段名 | 类型 | 必填 | 说明 | 示例值 |
|-------|------|------|------|--------|
| **loading_strategy** | ENUM | Y | 加载策略 | INCREMENTAL_MERGE |
| schedule_type | ENUM | Y | 调度周期 | DAILY |
| schedule_cron | VARCHAR(50) | N | Cron表达式 | 0 2 * * * |
| **dependency_tasks** | ARRAY | Y | 上游依赖任务 | ["dwd_trade_orders_detail_day", "dim_channel_info"] |
| execution_priority | INT | N | 执行优先级 | 100 |
| timeout_minutes | INT | N | 超时时间(分钟) | 60 |
| retry_count | INT | N | 重试次数 | 3 |
| retry_interval_minutes | INT | N | 重试间隔 | 10 |
| parallel_enabled | BOOLEAN | N | 是否允许并行 | false |

**loading_strategy枚举值说明：**
- `FULL`: 全量覆盖 - 先TRUNCATE再INSERT
- `INCREMENTAL_INSERT`: 增量追加 - 直接INSERT新数据
- `INCREMENTAL_MERGE`: 增量更新 - MERGE/UPSERT逻辑
- `INCREMENTAL_UPDATE`: 增量删除插入 - DELETE+INSERT
- `SCD2`: 拉链表 - 维护历史变更记录

### 1.3 增量/拉链配置（根据loading_strategy填写）

| 字段名 | 类型 | 条件必填 | 说明 | 示例值 |
|-------|------|---------|------|--------|
| **incremental_config** | JSON | loading_strategy≠FULL时必填 | 增量配置 | 见下方示例 |
| **scd_config** | JSON | loading_strategy=SCD2时必填 | SCD Type 2配置 | 见下方示例 |

**incremental_config示例：**
```json
{
  "watermark_field": "etl_load_time",
  "watermark_value": "${BATCH_DATE} 00:00:00",
  "incremental_condition": "etl_load_time >= '${BATCH_DATE}' AND etl_load_time < '${BATCH_DATE}'::DATE + INTERVAL '1 day'",
  "delete_before_insert": true,
  "idempotent": true
}
```

**scd_config示例：**
```json
{
  "effective_date_field": "eff_start_date",
  "expire_date_field": "eff_end_date",
  "current_flag_field": "is_current",
  "version_field": "version_no",
  "natural_keys": ["order_id"],
  "track_fields": ["order_status", "order_amount"],
  "end_date_max": "9999-12-31"
}
```

### 1.4 DWS分布式配置（P0 - 新增）

| 字段名 | 类型 | 必填 | 说明 | 示例值 |
|-------|------|------|------|--------|
| **distribution_key** | VARCHAR(100) | Y | 分布键 | channel_code, stat_date |
| **distribution_type** | ENUM | N | 分布类型 | HASH |
| **partition_key** | VARCHAR(50) | Y | 分区键 | stat_date |
| partition_type | ENUM | N | 分区类型 | RANGE |
| partition_granularity | ENUM | N | 分区粒度 | DAY |
| partition_retention_days | INT | N | 分区保留天数 | 365 |

**说明：**
- `distribution_key`: 决定数据在DWS各节点间的分布，建议选择数据分布均匀、常用于JOIN和GROUP BY的字段
- `partition_key`: 支持数据按时间/范围分区，便于高效清理历史数据

### 1.5 数据质量配置（P0 - 新增）

| 字段名 | 类型 | 必填 | 说明 | 示例值 |
|-------|------|------|------|--------|
| **exception_strategy** | ENUM | Y | 异常处理策略 | LOG |
| **validation_rules** | ARRAY | N | 数据验证规则 | 见下方示例 |
| null_check_enabled | BOOLEAN | N | 是否检查NULL值 | true |
| duplicate_check_enabled | BOOLEAN | N | 是否检查重复 | true |
| error_threshold_percent | DECIMAL | N | 错误率阈值(%) | 5.0 |

**exception_strategy枚举值：**
- `REJECT`: 拒绝写入，抛出异常回滚事务
- `LOG`: 记录错误日志但继续处理
- `ALLOW`: 允许写入（仅标记质量状态）
- `REJECT_AFTER_LOG`: 先记录日志，达到阈值后拒绝

**validation_rules示例：**
```json
[
  {
    "rule_id": "R001",
    "rule_name": "主键非空检查",
    "field": "summary_id",
    "rule_type": "NOT_NULL",
    "error_action": "REJECT"
  },
  {
    "rule_id": "R002",
    "rule_name": "订单金额范围检查",
    "field": "total_amount",
    "rule_type": "RANGE",
    "min_value": 0,
    "max_value": 999999999.99,
    "error_action": "LOG"
  },
  {
    "rule_id": "R003",
    "rule_name": "渠道代码有效性检查",
    "field": "channel_code",
    "rule_type": "REFERENTIAL",
    "ref_table": "dim_channel_info",
    "ref_field": "channel_code",
    "error_action": "LOG"
  }
]
```

### 1.6 来源表配置

| 字段名 | 类型 | 必填 | 说明 | 示例值 |
|-------|------|------|------|--------|
| source_tables | ARRAY | Y | 来源表列表 | 见下方示例 |
| source_relationship | VARCHAR(20) | N | 表关系类型 | JOIN |

**source_tables示例：**
```json
[
  {
    "source_id": "S001",
    "schema": "dwd",
    "table_name": "dwd_trade_orders_detail_day",
    "alias": "a",
    "table_type": "PRIMARY",
    "description": "订单明细事实表"
  },
  {
    "source_id": "S002",
    "schema": "dim",
    "table_name": "dim_channel_info",
    "alias": "b",
    "table_type": "LOOKUP",
    "description": "渠道维度表"
  }
]
```

### 1.7 SQL条件配置（P0 - 新增核心）

| 字段名 | 类型 | 必填 | 说明 | 示例值 |
|-------|------|------|------|--------|
| **sql_conditions** | JSON | Y | SQL条件配置 | 见下方完整示例 |

**sql_conditions完整示例：**
```json
{
  "joins": [
    {
      "join_sequence": 1,
      "join_type": "LEFT",
      "table_alias": "b",
      "join_condition": "a.channel_code = b.channel_code AND b.is_valid = 'Y'",
      "join_hint": "USE_HASH"
    }
  ],
  "where": [
    {
      "sequence": 1,
      "condition_group": 1,
      "logic": "AND",
      "field": "a.order_status",
      "operator": "IN",
      "value": ["COMPLETED", "PAID"],
      "value_type": "ARRAY"
    },
    {
      "sequence": 2,
      "condition_group": 1,
      "logic": "AND",
      "field": "a.is_deleted",
      "operator": "=",
      "value": "N",
      "value_type": "STRING"
    }
  ],
  "group_by": [
    {
      "sequence": 1,
      "expression": "a.stat_date",
      "alias": null
    },
    {
      "sequence": 2,
      "expression": "a.channel_code",
      "alias": null
    },
    {
      "sequence": 3,
      "expression": "b.channel_name",
      "alias": null
    }
  ],
  "having": [],
  "order_by": [
    {
      "sequence": 1,
      "expression": "a.stat_date",
      "direction": "DESC"
    },
    {
      "sequence": 2,
      "expression": "total_amount",
      "direction": "DESC"
    }
  ]
}
```

---

## 二、属性级Mapping（字段映射）

### 2.1 字段映射主表

| 序号 | 目标字段 | 目标类型 | 目标精度 | 可空 | 主键 | 生成方式 | 加工逻辑配置 |
|-----|---------|---------|---------|------|-----|---------|-------------|
| 1 | summary_id | VARCHAR | 50 | N | Y | DERIVED | 见2.2.1 |
| 2 | stat_date | DATE | - | N | - | DIRECT | 见2.2.2 |
| 3 | channel_code | VARCHAR | 20 | N | - | DIRECT | 见2.2.3 |
| 4 | channel_name | VARCHAR | 100 | Y | - | LOOKUP | 见2.2.4 |
| 5 | total_orders | BIGINT | - | N | - | AGGREGATE | 见2.2.5 |
| 6 | total_amount | DECIMAL | 18,2 | N | - | AGGREGATE | 见2.2.6 |
| 7 | total_customers | BIGINT | - | N | - | AGGREGATE | 见2.2.7 |
| 8 | avg_order_amount | DECIMAL | 18,2 | Y | - | CALCULATE | 见2.2.8 |
| 9 | amount_level | VARCHAR | 20 | Y | - | CASE | 见2.2.9 |
| 10 | etl_load_time | TIMESTAMP | - | N | - | SYSTEM | 见2.2.10 |

**生成方式枚举值：**
- `DIRECT`: 直接映射 - 源字段直接映射到目标字段
- `DERIVED`: 派生字段 - 通过表达式或函数计算生成
- `LOOKUP`: 关联查询 - 通过JOIN关联维度表获取
- `AGGREGATE`: 聚合计算 - SUM/COUNT/AVG等聚合函数
- `CALCULATE`: 公式计算 - 基于其他字段的数学计算
- `CASE`: 条件判断 - CASE WHEN条件表达式
- `SYSTEM`: 系统变量 - SYSDATE/USER等系统值
- `CONSTANT`: 常量值 - 固定值

### 2.2 结构化加工逻辑配置

#### 2.2.1 summary_id (派生字段)

```json
{
  "expression_type": "DERIVED",
  "expression_description": "主键：日期+渠道代码MD5",
  "source_fields": [
    {"table_alias": "a", "field_name": "stat_date", "data_type": "DATE"},
    {"table_alias": "a", "field_name": "channel_code", "data_type": "VARCHAR(20)"}
  ],
  "transformation": {
    "function_name": "MD5",
    "expression": "MD5(a.stat_date::TEXT || '_' || a.channel_code)",
    "output_type": "VARCHAR(32)"
  }
}
```

#### 2.2.2 stat_date (直接映射)

```json
{
  "expression_type": "DIRECT",
  "expression_description": "统计日期",
  "source_fields": [
    {"table_alias": "a", "field_name": "stat_date", "data_type": "DATE"}
  ],
  "transformation": {
    "expression": "a.stat_date",
    "output_type": "DATE"
  }
}
```

#### 2.2.3 channel_code (直接映射)

```json
{
  "expression_type": "DIRECT",
  "expression_description": "渠道代码",
  "source_fields": [
    {"table_alias": "a", "field_name": "channel_code", "data_type": "VARCHAR(20)"}
  ],
  "transformation": {
    "expression": "a.channel_code",
    "output_type": "VARCHAR(20)"
  }
}
```

#### 2.2.4 channel_name (关联查询)

```json
{
  "expression_type": "LOOKUP",
  "expression_description": "渠道名称 - 通过LEFT JOIN关联维度表获取",
  "source_fields": [
    {"table_alias": "b", "field_name": "channel_name", "data_type": "VARCHAR(100)"}
  ],
  "transformation": {
    "expression": "COALESCE(b.channel_name, '未知渠道')",
    "output_type": "VARCHAR(100)",
    "null_handling": "COALESCE",
    "default_value": "未知渠道"
  },
  "lookup_config": {
    "source_id": "S002",
    "lookup_key": "channel_code",
    "return_field": "channel_name"
  }
}
```

#### 2.2.5 total_orders (聚合计算)

```json
{
  "expression_type": "AGGREGATE",
  "expression_description": "订单总数",
  "source_fields": [
    {"table_alias": "a", "field_name": "order_id", "data_type": "VARCHAR(50)"}
  ],
  "transformation": {
    "function": "COUNT",
    "distinct": false,
    "expression": "COUNT(a.order_id)",
    "output_type": "BIGINT"
  },
  "aggregate_config": {
    "function": "COUNT",
    "source_expression": "a.order_id",
    "distinct": false,
    "filter_condition": null
  }
}
```

#### 2.2.6 total_amount (聚合计算)

```json
{
  "expression_type": "AGGREGATE",
  "expression_description": "订单总金额",
  "source_fields": [
    {"table_alias": "a", "field_name": "order_amount", "data_type": "DECIMAL(18,2)"}
  ],
  "transformation": {
    "function": "SUM",
    "distinct": false,
    "expression": "SUM(a.order_amount)",
    "output_type": "DECIMAL(18,2)"
  },
  "aggregate_config": {
    "function": "SUM",
    "source_expression": "a.order_amount",
    "distinct": false,
    "filter_condition": null
  }
}
```

#### 2.2.7 total_customers (聚合计算-去重)

```json
{
  "expression_type": "AGGREGATE",
  "expression_description": "客户总数（去重）",
  "source_fields": [
    {"table_alias": "a", "field_name": "customer_id", "data_type": "VARCHAR(50)"}
  ],
  "transformation": {
    "function": "COUNT",
    "distinct": true,
    "expression": "COUNT(DISTINCT a.customer_id)",
    "output_type": "BIGINT"
  },
  "aggregate_config": {
    "function": "COUNT",
    "source_expression": "a.customer_id",
    "distinct": true,
    "filter_condition": "a.customer_id IS NOT NULL"
  }
}
```

#### 2.2.8 avg_order_amount (公式计算)

```json
{
  "expression_type": "CALCULATE",
  "expression_description": "平均订单金额",
  "source_fields": [
    {"field_name": "total_amount", "data_type": "DECIMAL(18,2)", "is_target_field": true},
    {"field_name": "total_orders", "data_type": "BIGINT", "is_target_field": true}
  ],
  "transformation": {
    "expression": "CASE WHEN total_orders > 0 THEN total_amount / total_orders ELSE 0 END",
    "formula": "{total_amount} / NULLIF({total_orders}, 0)",
    "output_type": "DECIMAL(18,2)",
    "decimal_places": 2
  },
  "calculation_config": {
    "formula_template": "{0} / NULLIF({1}, 0)",
    "parameter_fields": ["total_amount", "total_orders"],
    "zero_division_handling": "NULL"
  }
}
```

#### 2.2.9 amount_level (条件判断)

```json
{
  "expression_type": "CASE",
  "expression_description": "金额等级划分",
  "source_fields": [
    {"field_name": "total_amount", "data_type": "DECIMAL(18,2)", "is_target_field": true}
  ],
  "transformation": {
    "expression": "CASE WHEN total_amount >= 1000000 THEN 'HIGH' WHEN total_amount >= 100000 THEN 'MEDIUM' ELSE 'LOW' END",
    "output_type": "VARCHAR(20)"
  },
  "case_config": {
    "case_type": "SIMPLE",
    "when_clauses": [
      {
        "condition": "{total_amount} >= 1000000",
        "result": "'HIGH'",
        "description": "高价值渠道"
      },
      {
        "condition": "{total_amount} >= 100000",
        "result": "'MEDIUM'",
        "description": "中价值渠道"
      }
    ],
    "else_result": "'LOW'",
    "else_description": "低价值渠道"
  }
}
```

#### 2.2.10 etl_load_time (系统变量)

```json
{
  "expression_type": "SYSTEM",
  "expression_description": "ETL加载时间",
  "source_fields": [],
  "transformation": {
    "system_variable": "CURRENT_TIMESTAMP",
    "expression": "CURRENT_TIMESTAMP",
    "output_type": "TIMESTAMP"
  }
}
```

---

## 三、运维监控配置

### 3.1 日志配置

```json
{
  "log_level": "INFO",
  "log_target": "TABLE",
  "log_table": "etl_log.proc_execution_log",
  "log_steps": [
    "INIT",
    "DEPENDENCY_CHECK",
    "PREPROCESS",
    "DATA_LOAD",
    "DATA_QUALITY",
    "STATISTICS",
    "COMPLETE"
  ],
  "log_fields": [
    "proc_name",
    "batch_date",
    "step",
    "status",
    "start_time",
    "end_time",
    "duration_seconds",
    "row_count",
    "error_code",
    "error_message"
  ]
}
```

### 3.2 监控告警配置

```json
{
  "monitor_enabled": true,
  "metrics": [
    {
      "metric_name": "row_count",
      "threshold_min": 1,
      "threshold_max": null,
      "alert_enabled": true
    },
    {
      "metric_name": "duration_seconds",
      "threshold_min": null,
      "threshold_max": 3600,
      "alert_enabled": true
    },
    {
      "metric_name": "error_rate_percent",
      "threshold_min": null,
      "threshold_max": 5.0,
      "alert_enabled": true
    }
  ],
  "alert_channels": ["EMAIL", "DINGTALK"],
  "alert_receivers": ["data-team@company.com", "张三"]
}
```

---

## 四、Mapping文档验证清单

在提交Mapping文档前，请确认以下检查项：

- [ ] mapping_id符合命名规范 `{layer}_{domain}_{table}_{freq}_{seq}`
- [ ] target_table在40个字符以内
- [ ] loading_strategy已正确选择
- [ ] distribution_key和partition_key已填写
- [ ] sql_conditions中的joins条件完整且正确
- [ ] sql_conditions中的where条件无语法错误
- [ ] 所有字段的expression_type已正确选择
- [ ] AGGREGATE类型字段的aggregate_config已配置
- [ ] CASE类型字段的case_config已配置
- [ ] LOOKUP类型字段的lookup_config已配置
- [ ] validation_rules至少包含主键非空检查
- [ ] dependency_tasks已填写且任务ID存在
- [ ] responsible_person已填写

---

## 五、版本变更记录

| 版本 | 日期 | 修改人 | 变更内容 |
|-----|------|--------|---------|
| v1.0.0 | 2026-01-15 | 张三 | 初始版本，基于文本描述 |
| v2.0.0 | 2026-03-18 | 李四 | 重构为结构化配置，补充P0字段：sql_conditions、loading_strategy、distribution_key、partition_key、exception_strategy、dependency_tasks |
