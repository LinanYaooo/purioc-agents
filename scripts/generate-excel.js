const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

// 列名定义
const headers = [
  '目标实体 所属schema*',
  '目标实体 物理表名称*',
  '目标实体 中文名称',
  '来源实体 所属schema*',
  '来源实体 中文名称',
  '来源实体 物理表名称*',
  '来源实体 别名',
  '关联类型*',
  '关联条件*',
  '具体关联',
  '责任人',
  '关联属性mapping',
  '备注',
  '表名长度(需要小于40位)',
  '列15',
  '列16',
  '列17',
  '列18',
  '列19',
  '列20',
  '列21',
  '列22',
  '列23',
  '列24',
  '列25',
  '列26',
  '列27',
  '列28',
  '列29',
  '列30',
  '列31',
  '列32'
];

// 示例数据
const exampleData = [
  '示例schema',
  '示例表名',
  '示例中文名',
  '来源schema',
  '来源中文名',
  '来源表名',
  '别名',
  'LEFT JOIN',
  'a.id=b.id',
  '',
  '张三',
  'id->source_id;name->source_name',
  '这是一个示例',
  25,
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  '',
  ''
];

// 创建工作簿
const wb = XLSX.utils.book_new();

// 创建数据数组（表头 + 示例数据）
const data = [headers, exampleData];

// 创建工作表
const ws = XLSX.utils.aoa_to_sheet(data);

// 设置列宽
const colWidths = headers.map(() => ({ wch: 20 }));
ws['!cols'] = colWidths;

// 添加工作表到工作簿
XLSX.utils.book_append_sheet(wb, ws, '实体级Mapping');

// 保存文件
const outputPath = path.join(__dirname, '..', 'spec', '实体级Mapping.xlsx');
XLSX.writeFile(wb, outputPath);

console.log(`✅ Excel文件已生成: ${outputPath}`);
