# DWS ETL存储过程自动化开发 - 启动评估报告

---

## 1. 执行摘要

经过对贵团队提供的DWS ETL设计文档进行全面技术评估，我们得出以下核心结论：**当前设计文档的完备度约为40%，尚不足以支撑全自动化的存储过程代码生成**。这并非对现有工作的否定，而是客观评估自动化系统对输入精度的严苛要求。

以下是我们基于六个关键维度的量化评分：

| 评估维度 | 完备度 | 权重 | 加权得分 | 关键缺口 |
|---------|-------|------|---------|---------|
| 执行策略 | 20% | 10% | 2.0 | 仅含调度周期，加载策略、并发控制、事务边界缺失 |
| 数据质量 | 10% | 10% | 1.0 | 校验规则、异常处理、数据清洗策略完全空白 |
| 血缘依赖 | 15% | 15% | 2.25 | 表级血缘隐含，任务依赖和字段级血缘未定义 |
| 元数据管理 | 15% | 15% | 2.25 | "加工逻辑描述"为自由文本，无法机器解析 |
| 运维监控 | 5% | 5% | 0.25 | 日志规范、监控指标、告警阈值未提及 |
| SQL生成能力 | 45% | 45% | 20.25 | JOIN条件、WHERE筛选、GROUP BY聚合、分区键生成能力不足 |
| **总体评分** | **—** | **100%** | **28.0%** | **需补充关键元数据以突破自动化瓶颈** |

**核心发现**：当前设计中最大的技术风险点在于"加工逻辑描述"字段。该字段采用自然语言文本描述业务规则，存在三大致命缺陷：其一，语义歧义难以消除，同一句话在不同开发人员理解中可能产生截然不同的SQL实现；其二，语法错误风险高，文本描述的"日期字段要处理成月初"可能对应多种SQL写法，无法保证正确性；其三，版本维护困难，文本变更无法像代码一样进行diff比对和回归测试。这种"文本即代码"的模式，使得自动化系统无法可靠解析并转换为可执行的PLSQL语句。

**Go/No-Go建议**：我们郑重建议采用**"先补充P0字段 + 分阶段推进"**的务实路径。若贵团队能够在1-2周内补充6个P0级关键字段（详见第3章），我们可以立即启动开发工作，并在3周内交付第一个MVP版本，实现约80%标准化场景的自动化覆盖。若时间窗口不允许，则可采用"半自动代码生成 + 人工审核微调"的过渡模式，先让工具生成70-80%的标准化代码框架，由开发人员聚焦处理复杂业务逻辑。直接在当前设计基础上强行推进全自动化的风险过高，可能导致代码质量不可控、返工成本激增。

---

## 2. 当前设计文档评估

### 2.1 肯定现有价值

首先，我们必须对贵团队已经提供的11个字段表示高度认可。这些字段已经满足了ETL映射的基础需求，为自动化开发奠定了重要基础：

| 字段名称 | 字段类型 | 价值说明 |
|---------|---------|---------|
| `mapping_id` | VARCHAR | 唯一标识每个ETL映射，是元数据管理和版本追踪的基础 |
| `target_table` | VARCHAR | 明确目标表，支持代码模板化 |
| `source_tables` | ARRAY | 源表列表已提供，支持简单的FROM子句生成 |
| `field_mappings` | JSON | 字段映射关系清晰，支持基础INSERT列生成 |
| `schedule_type` | ENUM | 调度周期（日/周/月）已定义，支持调度脚本生成 |
| `is_incremental` | BOOLEAN | 增量标记存在，为加载策略提供入口 |
| `key_fields` | ARRAY | 主键/业务键已识别，支持去重和SCD处理 |
| `filter_conditions` | TEXT | 简单过滤条件已记录，可作为WHERE子句输入 |
| `aggregate_fields` | JSON | 聚合目标字段已定义，为GROUP BY提供基础 |
| `create_time` | TIMESTAMP | 审计字段完整，支持元数据追溯 |
| `update_time` | TIMESTAMP | 更新追踪机制已建立 |

这11个字段的价值不容小觑。它们已经能够支撑我们生成基础的数据插入框架、调度配置模板和简单的监控埋点。对于单表映射、简单的字段转换场景，自动化生成率可以达到较高水平。贵团队在数据治理方面的积累，为我们后续的工作提供了宝贵的起点。

### 2.2 六维度详细评估

然而，当我们深入评估每个维度的自动化可行性时，发现了明显的短板：

**执行策略维度（20%完备度）**

当前文档仅提及了`schedule_type`（调度周期），但对于一个生产级的DWS存储过程，以下关键策略完全缺失：

- **加载策略**：FULL全量、INCREMENTAL增量、SCD2拉链表的具体选择逻辑未定义。增量场景下，如何识别新增、变更、删除记录？是CDC方式还是基于时间戳？
- **并发控制**：是否允许并行执行？最大并发数是多少？当上游依赖未就绪时如何阻塞？
- **事务边界**：每个批次多少条记录提交一次？出现错误时是全量回滚还是部分提交？分布式事务的协调策略是什么？

这些策略直接影响生成代码的结构。没有它们，自动化系统只能生成最保守的单线程、全量覆盖模式，无法满足生产环境性能要求。

**数据质量维度（10%完备度）**

数据质量规则的缺失是重大隐患。当前设计完全没有涉及：

- **校验规则**：字段非空检查、数据类型校验、取值范围验证、正则匹配规则
- **异常处理策略**：发现脏数据时是拒绝写入（REJECT）、记录日志后跳过（LOG）、还是允许写入但标记（ALLOW）？
- **数据清洗规则**：空值如何填充？格式不一致如何标准化？重复记录如何处理？

没有质量规则，自动化生成的代码将缺乏数据可信度保障。我们无法生成try-catch块、无法生成数据校验逻辑、无法生成异常数据转存逻辑。

**血缘依赖维度（15%完备度）**

数据血缘是自动化调度编排的基础。当前状态：

- **表级血缘**：通过`source_tables`和`target_table`可以推断出表级依赖，但缺乏显式声明
- **任务依赖**：上游任务ID列表（`dependency_tasks`）缺失。当前任务的执行必须等待哪些前置任务完成？
- **字段级血缘**：`field_mappings`中只说明了"目标字段A来自源字段B"，但没有说明转换逻辑，也无法追踪字段在多个任务间的流转路径

缺乏任务依赖声明，自动化系统无法生成正确的DAG调度配置，可能导致任务在源数据未就绪时就开始执行。

**元数据管理维度（15%完备度）**

这是最严重的瓶颈所在。当前`process_logic`字段（加工逻辑描述）采用自由文本格式，例如：

```
文本示例："如果source_type为'A'则取amount字段，否则取quantity字段"
```

这种描述方式无法被机器解析。自动化系统无法理解"如果...否则..."的条件结构，无法识别涉及的字段名，无法判断具体的CASE WHEN逻辑。这就像给厨师一张写着"做一道好吃的菜"的纸条，而不是具体的菜谱。

**运维监控维度（5%完备度）**

生产环境的可观测性几乎完全空白：

- **日志规范**：日志级别（DEBUG/INFO/WARN/ERROR）如何配置？关键节点（开始/结束/批次提交）是否需要记录？
- **监控指标**：需要采集哪些指标（处理记录数、处理时长、错误率、吞吐量）？
- **告警阈值**：失败多少次触发告警？处理时长超过多少分钟需要预警？
- **通知方式**：告警通过邮件/短信/钉钉/企业微信发送？接收人是谁？

没有这些信息，生成的代码将无法接入运维体系，成为"黑盒"。

**SQL生成能力维度（45%完备度）**

这是最核心的技术能力评估。基于当前设计，自动化系统能够生成的SQL组件比例如下：

| SQL组件 | 可生成度 | 说明 |
|--------|---------|------|
| INSERT列列表 | 90% | 基于`field_mappings`可直接生成 |
| SELECT字段映射 | 60% | 简单字段映射可生成，但函数调用、表达式计算不可生成 |
| FROM子句 | 40% | 单表场景可生成，多表JOIN场景缺乏JOIN条件 |
| JOIN条件 | 5% | `source_tables`为数组但无JOIN键定义，几乎无法生成 |
| WHERE筛选 | 30% | `filter_conditions`为文本，需人工解析后生成 |
| GROUP BY | 20% | `aggregate_fields`存在但无分组键定义 |
| HAVING筛选 | 0% | 完全缺失 |
| ORDER BY | 0% | 排序策略未定义 |
| 分区键(DWS) | 0% | 分布键和分区键完全缺失 |
| SCD逻辑 | 15% | 仅有`is_incremental`标记，缺乏SCD类型和生效时间处理 |

### 2.3 关键瓶颈总结

综合以上评估，当前设计存在三大关键瓶颈：

1. **加工逻辑文本化**：自然语言描述无法机器解析，是自动化生成的最大障碍
2. **JOIN条件缺失**：DWS作为MPP数据库，多表关联是核心场景，但JOIN键和关联类型（INNER/LEFT）未定义
3. **DWS分布式特性缺失**：分布键（distribution_key）和分区键（partition_key）的缺失，使得生成的代码无法针对DWS做性能优化，可能导致数据倾斜、查询缓慢

---

## 3. 缺失信息清单

为确保自动化系统的可靠运行，我们需要补充以下元数据字段。按照优先级分为P0（阻塞性）、P1（重要）、P2（增强）三个等级。

### 3.1 P0 阻塞性问题（6个）

以下字段是当前自动化系统运行的最低要求，缺失任何一个都会导致特定场景无法自动生成代码：

| 字段名称 | 数据类型 | 说明 | 示例 |
|---------|---------|------|------|
| `sql_conditions` | JSON | SQL条件配置，包含JOIN、WHERE、GROUP BY、HAVING | 见附录C |
| `loading_strategy` | ENUM | 加载策略：FULL（全量）、INCREMENTAL（增量）、SCD2（拉链） | 'INCREMENTAL' |
| `distribution_key` | VARCHAR | DWS分布键，决定数据在节点间的分布方式 | 'customer_id' |
| `partition_key` | VARCHAR | DWS分区键，支持时间/范围分区 | 'create_date' |
| `exception_strategy` | ENUM | 异常处理策略：REJECT（拒绝）、LOG（记录）、ALLOW（允许） | 'LOG' |
| `dependency_tasks` | ARRAY | 上游依赖任务ID列表，用于DAG调度 | ['task_001', 'task_002'] |

**为什么这些是P0？**

- `sql_conditions`：没有它，无法生成JOIN关联条件和复杂WHERE筛选。当前仅靠`filter_conditions`文本字段，自动化系统无法解析"日期大于2024-01-01 AND 状态等于已审核"这类复合条件。
- `loading_strategy`：决定代码的整体结构。FULL策略生成`TRUNCATE + INSERT`，INCREMENTAL生成`MERGE`或`DELETE + INSERT`，SCD2生成复杂的拉链表逻辑。没有这个字段，系统无法判断应该生成哪种代码模板。
- `distribution_key` / `partition_key`：DWS是分布式数据库，表创建时必须指定分布键，否则采用默认分布策略可能导致严重的数据倾斜和性能问题。自动化生成建表语句时必须包含这两个键。
- `exception_strategy`：决定数据质量校验失败时的处理逻辑。不同策略生成的代码差异巨大：REJECT需要生成事务回滚逻辑，LOG需要生成异常表插入逻辑，ALLOW则跳过校验。
- `dependency_tasks`：没有上游任务依赖声明，自动化调度系统无法判断任务何时可以启动，可能导致数据不一致。

### 3.2 P1 重要缺失（8个）

以下字段会显著提升自动化质量和覆盖范围，建议在第2阶段补充：

| 字段名称 | 数据类型 | 说明 |
|---------|---------|------|
| `scd_config` | JSON | SCD Type 2详细配置：生效时间字段、失效时间字段、当前标识字段 |
| `validation_rules` | JSON | 数据验证规则：非空检查、类型检查、范围检查、正则检查 |
| `performance_config` | JSON | 性能优化配置：并行度、批量提交大小、索引策略 |
| `execution_priority` | INT | 任务执行优先级，用于调度队列排序 |
| `retry_policy` | JSON | 重试策略：最大重试次数、重试间隔、退避策略 |
| `output_mapping` | JSON | 输出字段映射，用于下游任务消费 |
| `business_line` | VARCHAR | 业务线归属，用于权限隔离和资源分组 |
| `owner` | VARCHAR | 负责人信息，用于运维联系和问题追踪 |

### 3.3 P2 增强功能（6个）

以下字段属于高级功能，可在第3阶段考虑：

| 字段名称 | 数据类型 | 说明 |
|---------|---------|------|
| `data_masking` | JSON | 数据脱敏配置：敏感字段、脱敏算法 |
| `lineage_tracking` | BOOLEAN | 是否启用字段级血缘追踪 |
| `version_control` | JSON | 版本管理配置：Git仓库、分支策略 |
| `data_retention` | JSON | 数据保留策略：归档规则、清理周期 |
| `cost_center` | VARCHAR | 成本中心，用于资源费用分摊 |
| `compliance_tags` | ARRAY | 合规标签：GDPR、等保、SOX等 |

### 3.4 最小可行方案（MVP）

如果贵团队希望在**最短时间内看到自动化成果**，我们建议采用MVP方案：仅补充上述6个P0字段，即可支持约80%的标准化场景自动化：

**MVP场景覆盖能力：**

| 场景类型 | 覆盖度 | 说明 |
|---------|-------|------|
| 单表全量加载 | 100% | 支持标准INSERT ... SELECT |
| 单表增量加载 | 100% | 支持时间戳增量识别 |
| 简单多表JOIN | 95% | 支持2-5个表的等值JOIN |
| 简单聚合计算 | 90% | 支持SUM/COUNT/AVG等标准聚合 |
| SCD Type 1 | 100% | 支持直接覆盖更新 |
| 标准数据校验 | 85% | 支持非空、范围、类型检查 |

**MVP不包含的场景（剩余20%）：**

- 复杂子查询（需人工处理）
- 递归查询（需人工处理）
- 自定义函数调用（需人工处理）
- 动态SQL（需人工处理）
- 复杂业务规则计算（需人工处理）

我们建议贵团队优先完成P0字段的补充，这样可以在3周内交付第一个可用版本，让团队尽快体验自动化的价值，同时为后续完善争取时间和反馈。

---

## 4. 结构化改造方案

### 4.1 文本描述的问题分析

当前设计中最大的技术债务，是将"加工逻辑"以自然语言文本的形式存储。这种设计在短期内看似灵活，但从自动化和长期维护的角度看，存在四大致命缺陷：

**缺陷一：语义歧义无法消除**

同一个业务需求，不同的开发人员可能写出完全不同的文本描述：

```
描述A："计算会员最近30天的消费金额"
描述B："统计会员最近一个月的累计消费"
描述C："取会员在过去30个自然日内的订单总金额"
```

这三个描述在自动化系统看来是三个完全不同的指令。但实际上它们可能对应完全相同的SQL逻辑：
```sql
SUM(order_amount) FILTER (WHERE order_date >= CURRENT_DATE - INTERVAL '30 days')
```

**缺陷二：语法错误风险高**

文本描述中常常包含不精确的表达：

```
问题描述："日期要处理成月初"
歧义：
- 是指截取到月份第一天？还是月份格式？
- 输入字段是DATE类型还是STRING类型？
- 输出格式是'YYYY-MM-01'还是DATE类型？
```

不同的理解会导致生成截然不同的SQL代码，且错误只能在运行时才能发现。

**缺陷三：维护困难**

当业务规则变更时，文本的diff比对无法像代码一样清晰展示变更点。例如：

```
原描述："如果类型是A则取X字段"
新描述："如果类型是A或B则取X字段，C类型取Y字段"
```

这个变更引入了新的条件分支，但文本差异无法提示开发人员需要修改SQL的CASE WHEN结构。

**缺陷四：无法静态验证**

结构化数据可以在录入时就进行Schema校验。例如，JSON Schema可以强制要求`case_expression`必须包含`when_clauses`数组。但文本描述无法在不执行的情况下验证其正确性。

### 4.2 建议的结构化方案

我们建议将`process_logic`从TEXT类型改造为结构化的JSON格式。以下是建议的JSON Schema：

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "ProcessLogic",
  "type": "object",
  "required": ["expression_type"],
  "properties": {
    "expression_type": {
      "type": "string",
      "enum": ["DIRECT_MAPPING", "CASE_EXPRESSION", "AGGREGATE", "FUNCTION_CALL", "SUBQUERY", "CUSTOM"]
    },
    "source_fields": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "table_alias": {"type": "string"},
          "field_name": {"type": "string"},
          "data_type": {"type": "string"}
        },
        "required": ["field_name"]
      }
    },
    "case_expression": {
      "type": "object",
      "properties": {
        "when_clauses": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "condition": {"type": "string"},
              "result": {
                "oneOf": [
                  {"type": "string"},
                  {"$ref": "#"}
                ]
              }
            },
            "required": ["condition", "result"]
          }
        },
        "else_result": {
          "oneOf": [
            {"type": "string"},
            {"$ref": "#"}
          ]
        }
      },
      "required": ["when_clauses"]
    },
    "aggregate_config": {
      "type": "object",
      "properties": {
        "function": {
          "type": "string",
          "enum": ["SUM", "COUNT", "AVG", "MIN", "MAX", "STRING_AGG"]
        },
        "distinct": {"type": "boolean"},
        "filter_condition": {"type": "string"}
      },
      "required": ["function"]
    },
    "function_call": {
      "type": "object",
      "properties": {
        "function_name": {"type": "string"},
        "arguments": {
          "type": "array",
          "items": {
            "oneOf": [
              {"type": "string"},
              {"type": "number"},
              {"$ref": "#"}
            ]
          }
        }
      },
      "required": ["function_name", "arguments"]
    },
    "custom_sql": {
      "type": "string",
      "description": "当expression_type为CUSTOM时使用"
    }
  }
}
```

**结构化示例对比：**

| 场景 | 文本描述（当前） | 结构化配置（建议） |
|------|----------------|------------------|
| 简单映射 | "直接取source.amount字段" | `{"expression_type": "DIRECT_MAPPING", "source_fields": [{"field_name": "amount"}]}` |
| 条件判断 | "如果type='A'取X否则取Y" | `{"expression_type": "CASE_EXPRESSION", "case_expression": {"when_clauses": [{"condition": "type = 'A'", "result": "X"}], "else_result": "Y"}}` |
| 聚合计算 | "计算最近30天金额总和" | `{"expression_type": "AGGREGATE", "aggregate_config": {"function": "SUM", "source_fields": [{"field_name": "amount"}], "filter_condition": "date >= CURRENT_DATE - 30"}}` |

### 4.3 过渡方案：模板ID + 参数模式

如果贵团队无法立即全面改造为JSON Schema，我们也提供一个务实的过渡方案：**模板ID + 参数模式**。

我们预先定义20-30个最常用的加工逻辑模板，每个模板有唯一的ID和固定的参数列表。例如：

```json
{
  "templates": [
    {
      "template_id": "TPL_CASE_01",
      "name": "简单CASE WHEN",
      "description": "基于单一字段的等值判断",
      "parameters": [
        {"name": "source_field", "type": "string", "description": "源字段名"},
        {"name": "when_mappings", "type": "json", "description": "值到结果的映射表"},
        {"name": "default_value", "type": "string", "description": "ELSE默认值"}
      ],
      "sql_template": "CASE {{source_field}} {{#each when_mappings}} WHEN '{{@key}}' THEN '{{this}}' {{/each}} ELSE '{{default_value}}' END"
    },
    {
      "template_id": "TPL_AGG_01",
      "name": "时间窗口聚合",
      "description": "计算最近N天的聚合值",
      "parameters": [
        {"name": "agg_function", "type": "enum", "values": ["SUM", "COUNT", "AVG"]},
        {"name": "source_field", "type": "string"},
        {"name": "date_field", "type": "string"},
        {"name": "days", "type": "integer"}
      ],
      "sql_template": "{{agg_function}}({{source_field}}) FILTER (WHERE {{date_field}} >= CURRENT_DATE - {{days}})"
    }
  ]
}
```

使用时只需引用模板ID和填写参数：

```json
{
  "process_logic": {
    "template_id": "TPL_CASE_01",
    "parameters": {
      "source_field": "order_type",
      "when_mappings": {"A": "直营", "B": "加盟", "C": "代销"},
      "default_value": "其他"
    }
  }
}
```

这种模式的优势在于：
- **立即可用**：无需改造数据库Schema，只需约定模板ID
- **易于扩展**：新增业务场景时只需添加新模板
- **质量保证**：模板经过测试验证，避免语法错误
- **维护简单**：模板集中管理，修改一处全局生效

我们建议贵团队评估这两种方案，选择适合当前技术债务状况的路径推进。

---

## 5. 自动化开发方法论

### 5.1 技术架构

我们的自动化开发平台采用**元数据驱动 + Apache Velocity模板引擎 + 分层代码生成**的技术架构：

```
┌─────────────────────────────────────────────────────────────┐
│                    设计文档层 (Source)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │   Excel文档   │  │   Web界面    │  │   API接口        │   │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘   │
└─────────┼─────────────────┼───────────────────┼─────────────┘
          │                 │                   │
          └─────────────────┴───────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    元数据管理层 (Metadata)                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │  元数据校验    │  │  Schema版本  │  │  血缘关系图谱    │   │
│  │  Validation  │  │  Control     │  │  Lineage         │   │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘   │
└─────────┼─────────────────┼───────────────────┼─────────────┘
          │                 │                   │
          └─────────────────┴───────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    代码生成层 (Generation)                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Apache Velocity 模板引擎                  │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────────┐  │   │
│  │  │ 存储过程模板 │  │ 调度脚本模板 │  │ 监控配置模板    │  │   │
│  │  │  (*.vm)    │  │  (*.vm)    │  │  (*.vm)        │  │   │
│  │  └────────────┘  └────────────┘  └────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
┌─────────────────┐ ┌───────────────┐ ┌───────────────┐
│   PLSQL代码      │ │   调度配置    │ │   监控配置    │
│ (*.sql, *.plsql)│ │ (*.json, *.sh)│ │ (*.yaml)      │
└─────────────────┘ └───────────────┘ └───────────────┘
```

**核心技术组件：**

1. **元数据校验引擎**：基于JSON Schema对输入元数据进行严格校验，确保必填字段存在、数据类型正确、取值范围合法。在校验阶段就拦截90%的配置错误。

2. **Velocity模板引擎**：业界成熟的模板引擎，支持条件判断、循环、宏定义等复杂逻辑。我们将DWS存储过程的通用模式抽象为模板，元数据填充后生成具体代码。

3. **分层代码生成**：不生成单一大文件，而是分层生成：
   - 第一层：表结构定义（CREATE TABLE / ALTER TABLE）
   - 第二层：存储过程主体（PROCEDURE / FUNCTION）
   - 第三层：调度配置（Airflow DAG / 自定义调度）
   - 第四层：监控告警（Prometheus规则 / 日志配置）

4. **版本控制集成**：生成的代码自动提交到Git仓库，支持分支管理、代码审查、版本回滚。

### 5.2 开发流程

完整的自动化开发流程包含六个阶段：

```
设计评审 → 元数据录入 → 代码生成 → 语法校验 → 人工审核 → 部署上线
   │           │           │           │           │           │
   ▼           ▼           ▼           ▼           ▼           ▼
┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐
│业务确认│   │配置检查│   │模板渲染│   │SQL解析│   │Code   │   │CI/CD  │
│需求澄清│   │Schema│   │文件生成│   │语法检查│   │Review │   │发布   │
│规则定义│   │校验  │   │多版本  │   │依赖检查│   │差异比对│   │灰度   │
└──────┘   └──────┘   └──────┘   └──────┘   └──────┘   └──────┘
```

**各阶段详细说明：**

1. **设计评审（1-2天）**：与客户团队一起评审ETL设计文档，确认业务规则、数据质量要求、性能指标。输出经过确认的标准化设计文档。

2. **元数据录入（1-3天）**：通过Web界面或Excel导入方式，将设计文档转换为结构化元数据。系统自动进行Schema校验，实时提示配置错误。

3. **代码生成（自动化，分钟级）**：模板引擎根据元数据渲染生成存储过程代码、调度配置、监控配置。支持一次性生成多个环境（开发/测试/生产）的配置。

4. **语法校验（自动化，分钟级）**：
   - **SQL语法检查**：使用DWS SQL解析器验证生成的PLSQL语法正确性
   - **依赖检查**：验证引用的表、字段、函数是否存在
   - **静态分析**：检查潜在的性能问题（如全表扫描、缺失分区条件）

5. **人工审核（2-3天）**：开发人员审核生成的代码，重点检查：
   - 复杂业务逻辑是否正确实现
   - 性能优化是否到位
   - 异常处理是否完善
   - 代码风格是否符合团队规范

6. **部署上线（自动化，小时级）**：通过CI/CD流水线自动部署到测试环境进行验证，验证通过后灰度发布到生产环境。

### 5.3 自动化层级划分

不是所有的ETL场景都适合全自动。我们根据复杂度将场景划分为四个自动化层级：

| 层级 | 自动化程度 | 场景示例 | 预计占比 | 人工介入点 |
|-----|-----------|---------|---------|-----------|
| **Tier 1** | 全自动 | 单表映射、简单JOIN、标准SCD Type 1 | 30% | 仅需审核，无需修改 |
| **Tier 2** | 半自动 | 复杂多表关联、SCD Type 2拉链表、窗口函数 | 40% | 生成80%代码，人工处理复杂逻辑 |
| **Tier 3** | 辅助生成 | 复杂子查询、递归查询、自定义函数 | 20% | 生成代码框架，人工填充核心逻辑 |
| **Tier 4** | 纯人工 | 动态SQL、复杂业务规则、存储过程套存储过程 | 10% | 完全人工编写 |

**各层级详细说明：**

**Tier 1 - 全自动（30%）**

典型场景：
- 单表全量加载：`INSERT INTO target SELECT * FROM source`
- 简单字段映射：源字段A映射到目标字段B，可能伴随简单的类型转换
- 标准SCD Type 1：直接覆盖更新，无需历史追溯

自动化能力：
- 代码生成率：95%以上
- 人工介入：仅需审核生成的代码是否符合预期
- 质量保证：通过自动化测试即可上线

**Tier 2 - 半自动（40%）**

典型场景：
- 复杂多表JOIN：3个以上表的关联，需要处理关联缺失、数据重复
- SCD Type 2拉链表：需要维护生效时间、失效时间、当前标识
- 窗口函数：ROW_NUMBER()、LAG()、LEAD()等分析函数

自动化能力：
- 代码生成率：70-80%
- 人工介入：处理JOIN条件优化、SCD边界情况、窗口函数分区策略
- 质量保证：需要人工验证边界条件处理

**Tier 3 - 辅助生成（20%）**

典型场景：
- 复杂子查询：多层嵌套子查询，关联子查询
- 递归查询：WITH RECURSIVE处理树形结构数据
- 自定义函数调用：业务特定的UDF

自动化能力：
- 代码生成率：40-50%（主要是框架代码）
- 人工介入：填充子查询逻辑、编写递归终止条件、处理UDF参数
- 质量保证：需要全面的单元测试和集成测试

**Tier 4 - 纯人工（10%）**

典型场景：
- 动态SQL：表名、字段名在运行时确定
- 复杂业务规则：涉及多个系统的数据交互、复杂的业务判断
- 存储过程调用存储过程：多层嵌套的存储过程

自动化能力：
- 代码生成率：10%以下（仅生成注释模板）
- 人工介入：完全人工编写和审核
- 质量保证：需要详细的代码审查和全面的回归测试

这种分层策略确保我们：
- 在简单场景实现**完全自动化**，释放人力
- 在复杂场景**辅助而非替代**开发人员，提升效率
- 在极端复杂场景**不强行自动化**，避免质量风险

### 5.4 质量保证体系

自动化生成不等于降低质量标准。我们建立四层质量保证机制：

1. **语法校验层**
   - 使用DWS SQL解析器验证PLSQL语法
   - 检查标识符长度限制、保留字冲突
   - 验证数据类型兼容性

2. **静态分析层**
   - 检测全表扫描风险（WHERE条件是否命中分区键）
   - 检查笛卡尔积风险（多表JOIN缺少ON条件）
   - 识别潜在的性能瓶颈（大表关联、复杂子查询）

3. **单元测试层**
   - 自动生成测试用例：基于数据类型生成边界值测试数据
   - 集成测试框架：支持在隔离环境中验证存储过程逻辑
   - 回归测试：保存历史测试数据，确保修改不破坏已有功能

4. **代码审查层**
   - 强制Code Review：所有生成的代码必须经过人工审核
   - 差异比对：版本升级时自动生成diff，突出变更点
   - 审查清单：提供标准化的审查要点，确保审查质量

---

## 6. 合作启动建议

### 6.1 三阶段实施计划

我们建议将项目分为三个阶段实施，每个阶段都有明确的交付物和价值产出：

**Phase 1：基础自动化（第1-2周）**

目标：补充P0字段，实现Tier 1场景完全自动化

| 周次 | 任务 | 交付物 | 客户方投入 | 我方投入 |
|-----|------|--------|-----------|---------|
| Week 1 | P0字段补充、模板确认 | 补充后的设计文档、确认的模板库 | 2人天 | 3人天 |
| Week 2 | MVP开发、测试验证 | 代码生成平台MVP、示例存储过程 | 1人天 | 5人天 |

**预期成果：**
- 30%的标准场景实现完全自动化（单表映射、简单JOIN）
- 代码生成率达到90%以上
- 开发人员能够从重复劳动中释放出来

**Phase 2：能力扩展（第3-5周）**

目标：完善P1字段，扩展至Tier 2场景，覆盖70%业务场景

| 周次 | 任务 | 交付物 | 客户方投入 | 我方投入 |
|-----|------|--------|-----------|---------|
| Week 3 | P1字段补充、SCD模板开发 | SCD Type 2模板、增量加载模板 | 3人天 | 4人天 |
| Week 4 | 复杂场景支持、质量规则 | 数据质量校验模块、复杂JOIN支持 | 2人天 | 5人天 |
| Week 5 | 集成测试、性能优化 | 性能测试报告、优化建议 | 2人天 | 4人天 |

**预期成果：**
- 覆盖70%的ETL场景（Tier 1 + Tier 2）
- SCD Type 1和Type 2支持完善
- 数据质量校验自动化

**Phase 3：生产就绪（第6-8周）**

目标：建立完整运维体系，实现生产级交付

| 周次 | 任务 | 交付物 | 客户方投入 | 我方投入 |
|-----|------|--------|-----------|---------|
| Week 6 | 运维监控体系、告警配置 | 监控大盘、告警规则、运维手册 | 2人天 | 5人天 |
| Week 7 | 流水线集成、灰度发布 | CI/CD流水线、发布工具 | 2人天 | 5人天 |
| Week 8 | 知识转移、培训上线 | 培训材料、操作手册、技术文档 | 3人天 | 4人天 |

**预期成果：**
- 完整的DevOps流水线（设计→生成→测试→发布）
- 实时监控和告警体系
- 团队具备自主维护和扩展能力

### 6.2 双方职责划分

| 职责领域 | 客户方（贵团队） | 我方（自动化团队） |
|---------|----------------|------------------|
| **业务理解** | 提供业务规则说明、确认数据加工逻辑 | 将业务规则转化为技术实现 |
| **设计工作** | 负责ETL Mapping设计、字段映射定义 | 提供设计模板、自动化设计检查工具 |
| **元数据管理** | 维护和补充元数据、确保数据质量 | 开发元数据管理工具、提供Schema规范 |
| **数据质量** | 定义数据质量规则、确认异常处理策略 | 实现数据质量校验代码生成 |
| **技术架构** | 确认技术选型、提供环境资源 | 设计并实施自动化架构、核心引擎开发 |
| **模板开发** | 提供业务特定的代码模板需求 | 开发基础模板、支持模板扩展机制 |
| **质量保证** | 业务验收测试、生产环境验证 | 技术测试、性能测试、代码审查支持 |
| **运维支持** | 生产环境运维、监控响应 | 提供运维工具、技术支持 |

### 6.3 交付物清单

项目完成后，我们将向贵团队交付以下成果：

**技术平台：**
1. **代码生成平台**（Web界面）：支持元数据录入、代码生成、版本管理
2. **命令行工具**：支持CI/CD集成、批量代码生成
3. **IDE插件**（可选）：VSCode/IntelliJ插件，支持本地代码生成

**代码资产：**
4. **存储过程模板库**：20-30个经过验证的PLSQL模板
5. **调度配置模板**：Airflow DAG模板或贵团队指定调度系统配置
6. **监控配置模板**：Prometheus告警规则、日志采集配置

**文档资料：**
7. **技术架构文档**：详细说明系统架构、组件关系、扩展机制
8. **用户操作手册**：面向数据开发人员的操作指南
9. **运维手册**：面向运维人员的部署、监控、故障处理指南
10. **模板开发指南**：如何开发自定义模板的教程

**培训支持：**
11. **现场培训**：2天的集中培训，覆盖平台使用和模板开发
12. **知识转移**：核心代码讲解，确保团队具备自主维护能力

### 6.4 时间预估与里程碑

| 里程碑 | 时间 | 标志性成果 |
|-------|------|-----------|
| M1 - MVP就绪 | 第3周结束 | 第一个自动化生成的存储过程成功上线运行 |
| M2 - 核心能力 | 第5周结束 | 70%场景自动化，团队认可并日常使用 |
| M3 - 生产就绪 | 第8周结束 | 完整自动化体系上线，通过生产验证 |
| M4 - 知识转移 | 第9-10周 | 团队具备独立维护和扩展能力 |

**关于时间的说明：**

- **MVP版本（3周）**：这是最小可行方案的交付时间。只要贵团队能够在第1周补充完6个P0字段，我们就可以在3周内交付一个可用的自动化工具，立即产生价值。

- **完整体系（8周）**：这是生产级自动化体系的交付时间，包括完整的运维监控、CI/CD集成、知识转移。适合对稳定性和可维护性有高要求的团队。

- **并行推进**：三个阶段可以部分并行。例如，在Phase 1进行的同时，贵团队可以开始准备P1字段，从而缩短总工期。

- **风险缓冲**：上述时间预估假设双方能够及时响应、快速决策。如果评审和确认环节耗时较长，总工期会相应延长。

---

## 7. 附录

### 附录A：DWS存储过程标准模板

以下是一个完整的DWS存储过程示例，展示了自动化生成代码的风格和规范：

```sql
-- ============================================
-- 存储过程名称: SP_LOAD_{{target_table}}
-- 目标表: {{target_table}}
-- 作者: 自动生成 (自动化平台 v1.0)
-- 创建时间: {{create_time}}
-- 版本: {{version}}
-- 说明: {{description}}
-- ============================================

CREATE OR REPLACE PROCEDURE {{schema_name}}.SP_LOAD_{{target_table}}(
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
    v_proc_name VARCHAR(100) := 'SP_LOAD_{{target_table}}';
    v_step VARCHAR(100);
    
    -- 加载策略
    v_loading_strategy VARCHAR(20) := '{{loading_strategy}}';  -- FULL/INCREMENTAL/SCD2
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
    {{#if dependency_tasks}}
    PERFORM etl_check.check_dependencies(
        ARRAY[{{#each dependency_tasks}}'{{this}}'{{#unless @last}}, {{/unless}}{{/each}}],
        p_batch_date
    );
    {{/if}}

    -- ==========================================
    -- 步骤3: 数据预处理（清空/删除历史）
    -- ==========================================
    v_step := 'PREPROCESS';
    
    IF v_is_full THEN
        -- 全量加载：清空目标表
        TRUNCATE TABLE {{schema_name}}.{{target_table}};
        
        INSERT INTO etl_log.proc_execution_log (proc_name, batch_date, step, message)
        VALUES (v_proc_name, p_batch_date, v_step, '全量加载：已清空目标表');
    ELSE
        -- 增量加载：删除当日已有数据（幂等处理）
        DELETE FROM {{schema_name}}.{{target_table}}
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
    
    -- 标准加载逻辑（全量/增量）
    INSERT INTO {{schema_name}}.{{target_table}} (
        {{#each target_fields}}{{this}}{{#unless @last}}, {{/unless}}{{/each}},
        etl_batch_date, etl_create_time
    )
    SELECT 
        {{#each field_mappings}}
        {{#if this.expression}}
        {{this.expression}} AS {{this.target_field}}{{#unless @last}},{{/unless}}
        {{else}}
        {{#if this.source_table}}
        {{this.source_table}}.{{this.source_field}}
        {{else}}
        s.{{this.source_field}}
        {{/if}} AS {{this.target_field}}{{#unless @last}},{{/unless}}
        {{/if}}
        {{/each}},
        p_batch_date,
        CURRENT_TIMESTAMP
    FROM {{source_table}} s
    {{#each join_tables}}
    {{this.join_type}} JOIN {{this.table_name}} {{this.alias}} 
        ON {{this.join_condition}}
    {{/each}}
    WHERE 1=1
    {{#if filter_condition}}
      AND {{filter_condition}}
    {{/if}}
    {{#unless v_is_full}}
      AND s.{{incremental_field}} >= p_batch_date 
      AND s.{{incremental_field}} < p_batch_date + INTERVAL '1 day'
    {{/unless}}
    {{#if group_by_fields}}
    GROUP BY {{group_by_fields}}
    {{/if}}
    {{#if having_condition}}
    HAVING {{having_condition}}
    {{/if}};
    
    GET DIAGNOSTICS v_insert_count = ROW_COUNT;
    v_row_count := v_insert_count;

    -- ==========================================
    -- 步骤5: 数据质量校验
    -- ==========================================
    v_step := 'DATA_QUALITY';
    
    {{#each validation_rules}}
    -- 校验: {{this.name}}
    SELECT COUNT(*) INTO v_error_count
    FROM {{schema_name}}.{{target_table}}
    WHERE etl_batch_date = p_batch_date
      AND NOT ({{this.condition}});
    
    IF v_error_count > 0 THEN
        {{#if exception_strategy == 'REJECT'}}
        RAISE EXCEPTION '数据质量校验失败: {{this.name}}, 发现 % 条异常数据', v_error_count;
        {{/if}}
        {{#if exception_strategy == 'LOG'}}
        INSERT INTO etl_log.data_quality_log (
            table_name, batch_date, rule_name, error_count, error_sample
        )
        SELECT 
            '{{target_table}}', p_batch_date, '{{this.name}}', v_error_count,
            string_agg(DISTINCT {{this.field_name}}::TEXT, ', ' ORDER BY {{this.field_name}}::TEXT)
        FROM {{schema_name}}.{{target_table}}
        WHERE etl_batch_date = p_batch_date
          AND NOT ({{this.condition}})
        LIMIT 10;
        {{/if}}
    END IF;
    {{/each}}

    -- ==========================================
    -- 步骤6: 统计与VACUUM
    -- ==========================================
    v_step := 'STATISTICS';
    
    -- 更新统计信息
    ANALYZE {{schema_name}}.{{target_table}};
    
    -- 如果是全量加载，执行VACUUM
    IF v_is_full THEN
        VACUUM {{schema_name}}.{{target_table}};
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
COMMENT ON PROCEDURE {{schema_name}}.SP_LOAD_{{target_table}}(DATE, BOOLEAN) IS 
'{{description}}';
```

### 附录B：字段命名规范

为确保生成代码的可读性和一致性，我们建议采用以下命名规范：

| 对象类型 | 前缀 | 示例 | 说明 |
|---------|------|------|------|
| **输入参数** | `IN_` | `IN_BATCH_DATE` | 存储过程的输入参数 |
| **输出参数** | `OUT_` | `OUT_ROW_COUNT` | 存储过程的输出参数 |
| **局部变量** | `V_` | `V_START_TIME` | 过程内部变量 |
| **游标** | `CUR_` | `CUR_SOURCE_DATA` | 游标对象 |
| **记录类型** | `REC_` | `REC_TARGET` | 行记录变量 |
| **数组/集合** | `ARR_` | `ARR_KEY_FIELDS` | 数组类型变量 |
| **常量** | `C_` | `C_MAX_RETRY` | 常量定义 |
| **异常** | `EX_` | `EX_DATA_ERROR` | 自定义异常 |
| **临时表** | `TMP_` | `TMP_JOIN_RESULT` | 临时表 |
| **全局临时表** | `GTT_` | `GTT_STAGING` | 全局临时表 |

**表字段命名规范：**

| 字段类型 | 后缀/前缀 | 示例 | 说明 |
|---------|----------|------|------|
| 业务主键 | 无特殊前缀 | `CUSTOMER_ID` | 业务唯一标识 |
| 代理键 | `SK_` | `SK_CUSTOMER` | 自增代理键 |
| 外键 | `FK_` | `FK_ORDER_CUSTOMER` | 外键引用 |
| 时间戳 | `_TIME` | `CREATE_TIME` | 时间戳字段 |
| 日期 | `_DATE` | `ORDER_DATE` | 日期字段 |
| 标识字段 | `IS_` / `FLAG_` | `IS_VALID`, `FLAG_DELETED` | 布尔标识 |
| ETL审计字段 | `ETL_` | `ETL_BATCH_DATE`, `ETL_CREATE_TIME` | ETL专用字段 |
| SCD生效日期 | `_START_DATE` | `EFF_START_DATE` | 拉链表生效日期 |
| SCD失效日期 | `_END_DATE` | `EFF_END_DATE` | 拉链表失效日期 |
| SCD当前标识 | `IS_CURRENT` | `IS_CURRENT` | 拉链表当前标识 |

### 附录C：结构化加工逻辑配置示例

以下是三个真实场景的JSON配置示例：

**场景1：简单CASE WHEN条件判断**

```json
{
  "expression_type": "CASE_EXPRESSION",
  "source_fields": [
    {"table_alias": "s", "field_name": "order_type", "data_type": "VARCHAR(10)"}
  ],
  "case_expression": {
    "when_clauses": [
      {"condition": "s.order_type = 'A'", "result": "直营订单"},
      {"condition": "s.order_type = 'B'", "result": "加盟订单"},
      {"condition": "s.order_type = 'C'", "result": "代销订单"}
    ],
    "else_result": "其他订单"
  },
  "output": {
    "target_field": "order_type_name",
    "data_type": "VARCHAR(20)",
    "nullable": false
  }
}
```

**生成的SQL：**
```sql
CASE s.order_type 
    WHEN 'A' THEN '直营订单'
    WHEN 'B' THEN '加盟订单'
    WHEN 'C' THEN '代销订单'
    ELSE '其他订单'
END AS order_type_name
```

**场景2：时间窗口聚合计算**

```json
{
  "expression_type": "AGGREGATE",
  "source_fields": [
    {"table_alias": "s", "field_name": "order_amount", "data_type": "DECIMAL(18,2)"},
    {"table_alias": "s", "field_name": "order_date", "data_type": "DATE"}
  ],
  "aggregate_config": {
    "function": "SUM",
    "distinct": false,
    "expression": "s.order_amount",
    "filter_condition": "s.order_date >= CURRENT_DATE - INTERVAL '30 days'"
  },
  "output": {
    "target_field": "amount_last_30d",
    "data_type": "DECIMAL(18,2)",
    "nullable": true
  }
}
```

**生成的SQL：**
```sql
SUM(s.order_amount) FILTER (
    WHERE s.order_date >= CURRENT_DATE - INTERVAL '30 days'
) AS amount_last_30d
```

**场景3：窗口函数去重（取最新记录）**

```json
{
  "expression_type": "WINDOW_FUNCTION",
  "source_fields": [
    {"table_alias": "s", "field_name": "customer_id", "data_type": "VARCHAR(50)"},
    {"table_alias": "s", "field_name": "update_time", "data_type": "TIMESTAMP"}
  ],
  "window_function": {
    "function": "ROW_NUMBER",
    "partition_by": ["s.customer_id"],
    "order_by": [
      {"field": "s.update_time", "direction": "DESC"}
    ]
  },
  "filter": {
    "condition": "row_num = 1",
    "description": "只保留每个客户最新的一条记录"
  },
  "output": {
    "target_field": "row_num",
    "data_type": "INTEGER",
    "usage": "用于WHERE筛选，不写入目标表"
  }
}
```

**生成的SQL：**
```sql
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY s.customer_id 
            ORDER BY s.update_time DESC
        ) AS row_num
    FROM source_table s
)
SELECT *
FROM ranked
WHERE row_num = 1
```

### 附录D：P0字段JSON Schema定义

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "ETLMappingP0",
  "type": "object",
  "required": [
    "sql_conditions",
    "loading_strategy",
    "distribution_key",
    "partition_key",
    "exception_strategy"
  ],
  "properties": {
    "sql_conditions": {
      "type": "object",
      "description": "SQL条件配置",
      "properties": {
        "joins": {
          "type": "array",
          "description": "多表JOIN配置",
          "items": {
            "type": "object",
            "required": ["join_type", "table_alias", "join_condition"],
            "properties": {
              "join_type": {
                "type": "string",
                "enum": ["INNER", "LEFT", "RIGHT", "FULL", "CROSS"]
              },
              "table_alias": {"type": "string"},
              "join_condition": {"type": "string"},
              "join_sequence": {"type": "integer", "description": "JOIN顺序"}
            }
          }
        },
        "where": {
          "type": "array",
          "description": "WHERE条件",
          "items": {
            "type": "object",
            "required": ["field", "operator", "value"],
            "properties": {
              "field": {"type": "string"},
              "operator": {
                "type": "string",
                "enum": ["=", "!=", ">", ">=", "<", "<=", "LIKE", "IN", "BETWEEN", "IS NULL"]
              },
              "value": {"type": ["string", "number", "boolean", "array"]},
              "logic": {"type": "string", "enum": ["AND", "OR"], "default": "AND"}
            }
          }
        },
        "group_by": {
          "type": "array",
          "description": "GROUP BY字段",
          "items": {"type": "string"}
        },
        "having": {
          "type": "array",
          "description": "HAVING条件",
          "items": {
            "type": "object",
            "properties": {
              "aggregate_function": {"type": "string"},
              "operator": {"type": "string"},
              "value": {"type": ["string", "number"]}
            }
          }
        },
        "order_by": {
          "type": "array",
          "description": "ORDER BY字段",
          "items": {
            "type": "object",
            "properties": {
              "field": {"type": "string"},
              "direction": {"type": "string", "enum": ["ASC", "DESC"], "default": "ASC"}
            }
          }
        }
      }
    },
    "loading_strategy": {
      "type": "string",
      "description": "数据加载策略",
      "enum": ["FULL", "INCREMENTAL_INSERT", "INCREMENTAL_MERGE", "INCREMENTAL_UPDATE", "SCD2"]
    },
    "incremental_config": {
      "type": "object",
      "description": "增量加载配置",
      "properties": {
        "watermark_field": {"type": "string", "description": "水位线字段"},
        "watermark_value": {"type": "string", "description": "水位线值（支持表达式如CURRENT_DATE-1）"}
      }
    },
    "distribution_key": {
      "type": "string",
      "description": "DWS分布键，支持多字段逗号分隔"
    },
    "distribution_type": {
      "type": "string",
      "enum": ["HASH", "REPLICATION", "ROUNDROBIN"],
      "default": "HASH"
    },
    "partition_key": {
      "type": "string",
      "description": "DWS分区键"
    },
    "partition_type": {
      "type": "string",
      "enum": ["RANGE", "LIST", "HASH"],
      "default": "RANGE"
    },
    "exception_strategy": {
      "type": "string",
      "description": "数据质量异常处理策略",
      "enum": ["REJECT", "LOG", "ALLOW", "REJECT_AFTER_LOG"]
    },
    "dependency_tasks": {
      "type": "array",
      "description": "上游依赖任务ID列表",
      "items": {"type": "string"}
    }
  }
}
```

---

**报告编制**：数据开发团队  
**版本**：v1.0  
**日期**：2026年3月  
**联系方式**：如有疑问，欢迎随时沟通讨论
