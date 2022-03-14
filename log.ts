import { AppendEntriesArgs } from './raft.ts';
import { StateMachine } from './state_machine.ts';
import { Storage } from './storage.ts';
import { base64 } from './deps.ts';
export type LogEntry = {
  logIndex: number;
  logTerm: number;
  command: string;
};

export class Logs {
  private store: Storage;
  private stateMachine: StateMachine;

  // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
  private _commitIndex: number;
  // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
  private lastApplied: number;

  constructor(store: Storage, stateMachine: StateMachine) {
    this.store = store;
    this._commitIndex = 0;
    this.lastApplied = 0;
    this.stateMachine = stateMachine;
  }

  get commitIndex(): number {
    return this._commitIndex;
  }

  set commitIndex(index: number) {
    // 如果 commitIndex > lastApplied，则 lastApplied 递增，
    // 并将log[lastApplied] 应用到状态机中
    this._commitIndex = index;
    while (this._commitIndex > this.lastApplied) {
      this.lastApplied++;
      this.stateMachine.apply(
        base64.decode(this.entry(this.lastApplied)!.command),
      );
    }
  }

  entry(logIndex: number): LogEntry | undefined {
    return this.store.entry(logIndex);
  }

  lastIndex(): number {
    return this.store.lastIndex();
  }

  last(): LogEntry {
    return this.store.entry(this.store.lastIndex())!;
  }

  append(command: string, term: number): number {
    const logIndex = this.lastIndex() + 1;
    this.store.truncateAndAppend([{
      logTerm: term,
      logIndex,
      command,
    }]);
    return logIndex;
  }

  appendEntries(
    prevLogIndex: number,
    prevLogTerm: number,
    entries: LogEntry[],
    leaderCommit: number,
  ): boolean {
    // 返回假 如果接收者日志中没有包含这样一个条目
    // 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
    const entry = this.entry(prevLogIndex);
    if (entry?.logTerm !== prevLogTerm) {
      return false;
    }

    if (entries.length > 0) {
      // 如果一个已经存在的条目和新条目发生了冲突（因为索引相同，任期不同），
      // 那么就删除这个已经存在的条目以及它之后的所有条目
      entries = entries.slice(this.findConflict(entries) - entries[0].logIndex);
      this.store.truncateAndAppend(entries);
    }

    // 如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引
    //（leaderCommit > commitIndex）
    // 则把接收者的已知已经提交的最高的日志条目的索引 commitIndex 重置为
    // 领导人的已知已经提交的最高的日志条目的索引 leaderCommit
    // 或者是 上一个新条目的索引 取两者的最小值
    if (leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(
        leaderCommit,
        prevLogIndex + entries.length,
      );
    }
    return true;
  }

  batchEntries(
    startLogIndex: number,
    maxLen = 100,
  ): Pick<AppendEntriesArgs, 'prevLogIndex' | 'prevLogTerm' | 'entries'> {
    const entries = this.store.batchEntries(startLogIndex - 1, maxLen + 1);
    const prevLog = entries.shift() ?? this.last();
    return {
      prevLogIndex: prevLog.logIndex,
      prevLogTerm: prevLog.logTerm,
      entries,
    };
  }

  // 指定的日志是否是最新的
  isUpToDate(logIndex: number, logTerm: number): boolean {
    const last = this.last();
    return logTerm > last.logTerm ||
      (last.logTerm === logTerm && logIndex >= last.logIndex);
  }

  findConflict(entries: LogEntry[]): number {
    for (const e of entries) {
      const entry = this.entry(e.logIndex);
      if (entry?.logTerm !== e.logTerm) {
        return e.logIndex;
      }
    }
    return entries[0].logIndex;
  }
}
