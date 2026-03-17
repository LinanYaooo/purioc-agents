#!/usr/bin/env node

/**
 * Markdown Table to Excel Converter Skill
 * 将 Markdown 格式的表格转换为 xlsx 文件
 */

const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');

// 默认的列定义（基于用户提供的文档）
const DEFAULT_HEADERS = [
  '目标实体 物理表名称*',
  '目标实体 字段名称*',
  '目标实体 属性名称',
  '生成方式*',
  '来源实体 所属schema*',
  '来源实体 物理表名称*',
  '来源实体 别名',
  '来源实体 字段名称*',
  '来源实体 属性名称',
  '加工逻辑描述*',
  '去重判断',
  '责任人',
  '字段类型',
  '字段长度',
  '主键（Y,N）',
  '变更记录(xxxx年xx月xx日 xx版本 xxx)',
  '备注',
  '字段长度(需要小于30位)'
];

/**
 * 解析 Markdown 表格
 * @param {string} markdown - Markdown 表格内容
 * @returns {Array<Array<string>>} - 二维数组
 */
function parseMarkdownTable(markdown) {
  const lines = markdown.trim().split('\n');
  const data = [];
  
  for (const line of lines) {
    const trimmedLine = line.trim();
    
    // 跳过空行和分隔符行
    if (!trimmedLine || trimmedLine.match(/^\|[-\s:|]+\|?$/)) {
      continue;
    }
    
    // 解析表格行
    if (trimmedLine.startsWith('|')) {
      const cells = trimmedLine
        .split('|')
        .filter((_, index, arr) => index > 0 && index < arr.length - 1)
        .map(cell => cell.trim());
      
      if (cells.length > 0) {
        data.push(cells);
      }
    }
  }
  
  return data;
}

/**
 * 生成 Excel 文件
 * @param {Array<Array<string>>} data - 表格数据
 * @param {string} outputPath - 输出文件路径
 * @param {string} sheetName - 工作表名称
 */
function generateExcel(data, outputPath, sheetName = '字段映射') {
  // 创建工作簿
  const wb = XLSX.utils.book_new();
  
  // 创建工作表
  const ws = XLSX.utils.aoa_to_sheet(data);
  
  // 设置列宽
  const colWidths = data[0].map((_, index) => ({
    wch: index === 0 ? 8 : 25 // 第一列（序号）窄一些，其他列宽一些
  }));
  ws['!cols'] = colWidths;
  
  // 添加工作表到工作簿
  XLSX.utils.book_append_sheet(wb, ws, sheetName);
  
  // 保存文件
  XLSX.writeFile(wb, outputPath);
  
  return outputPath;
}

/**
 * 主函数
 */
function main() {
  try {
    // 检查命令行参数
    const args = process.argv.slice(2);
    
    // 解析参数
    let inputFile = null;
    let outputFile = 'output.xlsx';
    let useDefaultHeaders = false;
    
    for (let i = 0; i < args.length; i++) {
      if (args[i] === '-i' || args[i] === '--input') {
        inputFile = args[i + 1];
        i++;
      } else if (args[i] === '-o' || args[i] === '--output') {
        outputFile = args[i + 1];
        i++;
      } else if (args[i] === '-d' || args[i] === '--default-headers') {
        useDefaultHeaders = true;
      } else if (args[i] === '-h' || args[i] === '--help') {
        printHelp();
        return;
      }
    }
    
    // 如果没有输入文件，从 stdin 读取
    let markdownContent;
    if (inputFile) {
      markdownContent = fs.readFileSync(inputFile, 'utf-8');
    } else {
      markdownContent = fs.readFileSync(0, 'utf-8'); // 从 stdin 读取
    }
    
    if (!markdownContent.trim()) {
      console.error('❌ 错误: 没有输入内容');
      process.exit(1);
    }
    
    // 解析 Markdown 表格
    const tableData = parseMarkdownTable(markdownContent);
    
    if (tableData.length === 0) {
      console.error('❌ 错误: 无法解析 Markdown 表格');
      process.exit(1);
    }
    
    // 如果需要，使用默认表头
    let finalData = tableData;
    if (useDefaultHeaders && tableData.length > 0) {
      // 将第一行替换为默认表头
      finalData = [DEFAULT_HEADERS, ...tableData.slice(1)];
    }
    
    // 确保输出路径是绝对路径
    const outputPath = path.isAbsolute(outputFile) 
      ? outputFile 
      : path.join(process.cwd(), outputFile);
    
    // 生成 Excel 文件
    generateExcel(finalData, outputPath);
    
    console.log(`✅ Excel 文件已生成: ${outputPath}`);
    console.log(`📊 数据行数: ${finalData.length - 1} 行（不含表头）`);
    console.log(`📋 列数: ${finalData[0].length} 列`);
    
  } catch (error) {
    console.error('❌ 错误:', error.message);
    process.exit(1);
  }
}

/**
 * 打印帮助信息
 */
function printHelp() {
  console.log(`
Markdown Table to Excel Converter
将 Markdown 格式的表格转换为 xlsx 文件

用法:
  node md2xlsx.js [选项]

选项:
  -i, --input <file>      输入 Markdown 文件路径（如果不指定，从 stdin 读取）
  -o, --output <file>     输出 Excel 文件路径（默认: output.xlsx）
  -d, --default-headers   使用默认的字段映射表头
  -h, --help              显示帮助信息

示例:
  # 从文件转换
  node md2xlsx.js -i input.md -o output.xlsx

  # 使用默认表头
  node md2xlsx.js -i input.md -o output.xlsx -d

  # 从管道输入
  echo "| 序号 | 列名 |" | node md2xlsx.js -o output.xlsx

默认表头（字段映射模板）:
  - 目标实体 物理表名称*
  - 目标实体 字段名称*
  - 目标实体 属性名称
  - 生成方式*
  - 来源实体 所属schema*
  - 来源实体 物理表名称*
  - 来源实体 别名
  - 来源实体 字段名称*
  - 来源实体 属性名称
  - 加工逻辑描述*
  - 去重判断
  - 责任人
  - 字段类型
  - 字段长度
  - 主键（Y,N）
  - 变更记录
  - 备注
  - 字段长度(需要小于30位)
  `);
}

// 执行主函数
if (require.main === module) {
  main();
}

module.exports = {
  parseMarkdownTable,
  generateExcel,
  DEFAULT_HEADERS
};
