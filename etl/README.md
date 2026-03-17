# DWS/GaussDB 存储过程示例

本项目包含华为云 DWS (GaussDB for Data Warehouse) 存储过程的实用示例。

## 目录结构

```
etl/
├── procedures/
│   ├── 01_incremental_load.sql      # 增量加载
│   ├── 02_scd_type2.sql            # 慢变化维 Type 2
│   ├── 03_data_quality_check.sql   # 数据质量校验
│   ├── 04_batch_insert.sql         # 批量插入优化
│   └── 05_partition_maintenance.sql # 分区维护
├── functions/
│   └── etl_utils.sql               # ETL 工具函数
├── triggers/
│   └── audit_trigger.sql           # 审计触发器
└── README.md
```

## 特性

- **分布式架构优化**: 针对 DWS 分布式特性设计的存储过程
- **高性能**: 使用批量操作和并行处理
- **可靠性**: 包含错误处理和事务管理
- **可维护性**: 详细的注释和日志记录

## 适用场景

- Oracle 迁移到 GaussDB
- 数据仓库 ETL 流程
- 实时/准实时数据同步
- 数据质量监控

## 快速开始

1. 在 DWS 控制台或 gsql 中执行存储过程
2. 根据实际表结构调整参数
3. 配置调度任务（如使用 DWS 定时任务或外部调度器）

## 注意事项

- 分布键选择对性能影响重大
- 大表建议使用分区
- 定期 ANALYZE 以更新统计信息
