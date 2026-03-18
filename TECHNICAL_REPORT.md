# ETL Mapping Excel 生成技术调研报告

## 1. Python Excel生成库对比

### 1.1 功能对比表

| 特性 | **openpyxl** | **xlsxwriter** | **pandas** | xlwt/xlrd |
|------|-------------|----------------|------------|-----------|
| **读取Excel** | ✅ 完整支持 | ❌ 不支持 | ✅ 支持 | ✅ 仅xls |
| **写入Excel** | ✅ 完整支持 | ✅ 完整支持 | ✅ 支持 | ✅ 仅xls |
| **数据验证** | ✅ 支持 | ✅ 支持 | ❌ 不支持 | ❌ 不支持 |
| **条件格式** | ✅ 支持 | ✅ 支持 | ❌ 需引擎 | ❌ 不支持 |
| **Excel表格** | ✅ Table支持 | ✅ Table支持 | ❌ 不支持 | ❌ 不支持 |
| **多Sheet** | ✅ 支持 | ✅ 支持 | ✅ 支持 | ✅ 有限支持 |
| **图表** | ✅ 支持 | ✅ 更丰富 | ⚠️ 有限 | ❌ 不支持 |
| **性能** | 中等 | **更好** | 快（简单场景） | 快（xls） |
| **公式** | ✅ 支持 | ✅ 支持 | ✅ 需引擎 | ⚠️ 有限 |

### 1.2 推荐选择: **openpyxl**

**选择理由:**
1. **读写兼备**: 唯一支持读取和写入xlsx格式的全功能库
2. **ETL场景完美匹配**: 支持数据验证、条件格式、Excel表格
3. **社区活跃**: 文档完善，GitHub 3k+ stars
4. **公式支持**: 支持Excel公式保留和计算

**xlsxwriter适用场景:**
- 仅需写入，不需要读取
- 需要生成复杂图表
- 追求极致写入性能

**pandas适用场景:**
- 简单数据导出
- 数据分析后快速导出
- 不需求复杂格式

---

## 2. 复杂数据结构在Excel中的表达模式

### 2.1 JSON扁平化策略

#### 方法1: 点号路径表示法（推荐）
```
JSON: {"user": {"address": {"city": "Shanghai"}}}
Excel列: user.address.city
```
**优点**: 保留层级关系，易于解析还原
**适用**: ETL Mapping中的字段映射

#### 方法2: 多级列头
```
| user          |
| address       |
| city  | zip   |
|-------|-------|
| Shanghai|200000|
```
**优点**: 视觉清晰
**缺点**: 复杂难维护

#### 方法3: 逗号分隔
```
JSON数组: ["tag1", "tag2", "tag3"]
Excel单元格: "tag1, tag2, tag3"
```
**优点**: 简单直观
**缺点**: 无法表达复杂对象

### 2.2 数组类型处理

| 策略 | 实现方式 | 适用场景 |
|------|---------|----------|
| **逗号分隔** | 存储为单个单元格 | 简单字符串数组 |
| **多行展开** | 每条记录一行 | 数组元素有独立Mapping |
| **JSON字符串** | 保持原样存入 | 复杂嵌套对象 |
| **关联表** | 单独Sheet存储 | 一对多关系 |

**推荐做法**: ETL Mapping中使用"逗号分隔+JSON路径"的组合方式

### 2.3 嵌套结构表达示例

```json
{
  "sql_conditions": {
    "joins": [
      {"table": "orders", "on": "customer.id = orders.cust_id"}
    ],
    "where": "status = 'active'",
    "group_by": ["category"]
  }
}
```

**Excel表达方案**:
| 配置项 | 值 | 说明 |
|--------|-----|------|
| joins | JSON字符串 | `[{"table":"orders",...}]` |
| where | 文本 | `status = 'active'` |
| group_by | 逗号分隔 | `category,region` |

---

## 3. Excel模板设计最佳实践

### 3.1 用户友好的界面设计

#### Sheet结构
```
Sheet1: 实体级Mapping (Entity Level)
Sheet2: 属性级Mapping (Attribute Level)  
Sheet3: 枚举值参考 (Reference - Hidden)
```

#### 列设计原则
1. **必填列在前**: ID, Name等核心字段放前面
2. **逻辑分组**: 源字段列在一起，目标字段列在一起
3. **适当宽度**: 根据内容设置列宽
4. **冻结窗格**: 冻结表头行便于浏览

### 3.2 数据验证策略

| 验证类型 | 应用场景 | 实现方式 |
|----------|----------|----------|
| **下拉列表** | 系统类型、状态、数据类型 | DataValidation(type="list") |
| **日期范围** | 有效日期 | DataValidation(type="date") |
| **整数范围** | 长度、精度 | DataValidation(type="whole") |
| **文本长度** | 字段名长度限制 | DataValidation(type="textLength") |
| **自定义公式** | 复杂业务规则 | DataValidation(type="custom") |

### 3.3 条件格式应用

```python
# 必填项为空时红色高亮
rule = FormulaRule(formula=['ISBLANK(A2)'], 
                   fill=PatternFill(start_color="FFC7CE"))

# 状态为Approved时绿色高亮
rule = FormulaRule(formula=['I2="Approved"'], 
                   fill=PatternFill(start_color="C6EFCE"))

# 转换规则为Custom时黄色提醒
rule = FormulaRule(formula=['O2="Custom"'], 
                   fill=PatternFill(start_color="FFEB9C"))
```

### 3.4 填写说明设计

**推荐做法**:
1. **首行说明**: 合并单元格显示整体说明
2. **列批注**: 复杂列添加批注说明
3. **数据验证提示**: 使用prompt_title和prompt
4. **示例数据**: 提供2-3行示例数据
5. **参考Sheet**: 隐藏Sheet存放下拉列表值

---

## 4. 实际案例参考

### 4.1 数据字典Excel模板标准结构

| 列名 | 说明 | 验证 |
|------|------|------|
| Field Name | 字段名 | 必填，文本长度≤50 |
| Data Type | 数据类型 | 下拉：String/Number/Date/Boolean |
| Length | 长度 | 整数，≥0 |
| Nullable | 可空 | 下拉：Yes/No |
| Default Value | 默认值 | 文本 |
| Description | 描述 | 必填，文本 |
| Example | 示例值 | 文本 |

### 4.2 Informatica PowerCenter Mapping规范

参考 [Informatica官方文档](https://docs.informatica.com/data-integration/powercenter/10-5/mapping-analyst-for-excel-guide/standard-mapping-specification-template.html):
- 标准化的Mapping Specification Template
- 支持导入PowerCenter Designer
- 包含Source/Target Definition, Transformation逻辑

### 4.3 元数据管理工具对比

| 工具 | Excel导出特性 |
|------|---------------|
| **Informatica** | 标准化模板，支持导入导出 |
| **Talend** | 自动生成Mapping文档 |
| **DataHub** | 数据血缘Excel报告 |
| **Apache Atlas** | 有限支持，需定制开发 |

---

## 5. 技术选型建议

### 5.1 最终推荐方案

```
✅ 核心库: openpyxl 3.1.x
✅ 辅助库: pandas (数据处理)
✅ 输出格式: .xlsx (Excel 2007+)
✅ Python版本: 3.8+
```

### 5.2 安装命令

```bash
pip install openpyxl pandas
```

### 5.3 代码架构建议

```
etl_excel_generator/
├── __init__.py
├── generator.py          # 核心生成器
├── templates/            # 模板配置
│   ├── entity_mapping.json
│   └── attribute_mapping.json
├── styles.py             # 样式定义
├── validators.py         # 验证规则
└── examples/             # 示例数据
    └── sample_mappings.json
```

---

## 6. 最小可行代码示例

详见 `etl_mapping_generator.py`

### 快速开始

```bash
# 1. 安装依赖
pip install openpyxl

# 2. 运行生成器
python etl_mapping_generator.py

# 3. 打开生成的Excel文件
open ETL_Mapping_Template_*.xlsx
```

### 主要特性演示

1. **多Sheet结构**: 实体级 + 属性级
2. **数据验证**: 下拉列表限制输入
3. **条件格式**: 必填项自动高亮
4. **Excel表格**: Table样式和过滤
5. **JSON路径**: 支持嵌套字段表达

---

## 7. 参考资源

### 官方文档
- [openpyxl文档](https://openpyxl.readthedocs.io/)
- [xlsxwriter文档](https://xlsxwriter.readthedocs.io/)

### 最佳实践文章
- [Data Dictionary Examples](https://www.ovaledge.com/blog/data-dictionary-examples-templates)
- [Source to Target Mapping Excel](https://www.future-processing.com/blog/source-to-target-mapping-using-excel/)
- [Flatten Nested JSON for Excel](https://data-migration-tools.com/convert-nested-json-to-csv-excel/)

---

**报告生成日期**: 2026-03-18  
**版本**: v1.0
