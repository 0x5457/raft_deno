import { msgpack } from './deps.ts';
import { log } from './deps.ts';
export interface StateMachine {
  apply(command: Uint8Array): void;
}

export type RmCommand = {
  type: 'rm';
  key: string;
};

export type SetCommand = {
  type: 'set';
  key: string;
  value: string;
};

export type Command = RmCommand | SetCommand;

export class MemStateMachine implements StateMachine {
  state: { [key: string]: string } = {};
  apply(command: Uint8Array): void {
    const cmd = <Command> msgpack.decode(command);
    log.info(cmd);
    if ((cmd).type === 'set') {
      this.state[cmd.key] = cmd.value;
    } else {
      delete this.state[cmd.key];
    }
  }

  get(key: string): string | null {
    return this.state[key] ?? null;
  }
}
