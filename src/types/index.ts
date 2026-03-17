/**
 * Subagent 类型定义
 */

export interface Task {
  type: string;
  payload: any;
}

export enum SubagentType {
  CodeReviewer = 'code-reviewer',
  TestGenerator = 'test-generator',
  EtlEngineering = 'purioc-agent-etl-engineering',
}

export interface SubagentConfig {
  name: string;
  description: string;
  prompt: string;
}
