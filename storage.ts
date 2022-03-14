import { LogEntry } from './log.ts';

export interface Storage {
  lastIndex(): number;
  entry(index: number): LogEntry | undefined;
  batchEntries(startIndex: number, maxLen: number): LogEntry[];
  truncateAndAppend(entries: LogEntry[]): void;
}

export class MemStorage implements Storage {
  entries: LogEntry[];

  constructor() {
    this.entries = [{ logIndex: 0, logTerm: 0, command: '' }];
  }

  batchEntries(startIndex: number, maxLen: number): LogEntry[] {
    return this.entries.slice(startIndex, startIndex + maxLen);
  }

  entry(index: number): LogEntry | undefined {
    return this.entries[index];
  }

  lastIndex(): number {
    return this.entries[this.entries.length - 1].logIndex;
  }

  truncateAndAppend(entries: LogEntry[]) {
    if (entries.length === 0) {
      return;
    }
    for (const e of entries) {
      this.entries[e.logIndex] = e;
    }
    const lastId = entries[entries.length - 1].logIndex;
    this.entries.splice(lastId + 1);
  }
}
