import {
  AppendEntriesArgs,
  AppendEntriesReply,
  RequestVoteArgs,
  RequestVoteReply,
} from './raft.ts';

export interface Peer {
  // 追加条目
  appendEntries(
    aea: AppendEntriesArgs,
    timeout: number,
  ): Promise<AppendEntriesReply>;
  // 请求投票
  requestVote(rv: RequestVoteArgs, timeout: number): Promise<RequestVoteReply>;
}

export class HttpPeer implements Peer {
  addr: string;

  constructor(addr: string) {
    this.addr = addr;
  }

  appendEntries(
    aea: AppendEntriesArgs,
    timeout: number,
  ): Promise<AppendEntriesReply> {
    return this.post('append_entries', aea, timeout);
  }

  requestVote(rv: RequestVoteArgs, timeout: number): Promise<RequestVoteReply> {
    return this.post('request_vote', rv, timeout);
  }

  async post<Req, Resp>(
    method: string,
    req: Req,
    timeout: number,
  ): Promise<Resp> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    const resp = await fetch(this.addr + '/' + method, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req),
      signal: controller.signal,
    });
    clearTimeout(timer);
    return (await resp.json()) as Resp;
  }
}
