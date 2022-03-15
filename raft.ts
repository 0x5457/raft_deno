import { LogEntry, Logs } from './log.ts';
import { Peer } from './rpc.ts';
import { log } from './deps.ts';
import { ReceivePromise, select, signal, timeout } from './channel.ts';

enum RaftStatus {
  Leader,
  Follower,
  Candidate,
}
export type Config = {
  rpcTimeout: number;
  heartbeatTimeout: number;
  heartbeatInterval: number;
};

export class Raft {
  leaderId?: number;
  id: number;
  // 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
  private _currentTerm: number;
  // 当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
  private votedFor?: number;
  // 当前节点状态
  private status: RaftStatus;
  // 心跳超时计时器
  private heartbeatTimeout: ResettableTimeout;
  private logs: Logs;
  private peers: { [id: number]: Peer };
  private config: Config;
  private commitEmitter: {
    [key: number]: (() => void)[];
  };

  get currentTerm() {
    return this._currentTerm;
  }

  set currentTerm(term: number) {
    this.votedFor = undefined;
    this._currentTerm = term;
  }

  private receiveShutdown: () => ReceivePromise<void>;
  private receiveHeartbeat: () => ReceivePromise<void>;

  shutdown: () => void;

  constructor(
    id: number,
    logs: Logs,
    peers: { [id: number]: Peer },
    config: Config,
  ) {
    this.id = id;
    this._currentTerm = 0;
    this.status = RaftStatus.Follower;
    this.config = config;
    const [emitHeartbeat, receiveHeartbeat] = signal();
    this.heartbeatTimeout = new ResettableTimeout(
      config.heartbeatTimeout,
      emitHeartbeat,
    );

    this.commitEmitter = {};
    this.receiveHeartbeat = receiveHeartbeat;

    const [emitShutdown, receiveShutdown] = signal();
    this.receiveShutdown = receiveShutdown;
    this.shutdown = emitShutdown;

    this.logs = logs;
    this.peers = peers;

    this.run();
    this.heartbeatTimeout.start();
  }

  // 处理追加条目
  handleAppendEntries(aea: AppendEntriesArgs): AppendEntriesReply {
    // 1. 返回假 如果领导人的任期小于接收者的当前任期
    if (aea.term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }

    // 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，
    // 则令 currentTerm = T，并切换为跟随者状态
    if (aea.term > this.currentTerm) {
      this.currentTerm = aea.term;
      this.status = RaftStatus.Follower;
    }

    if (
      !this.logs.appendEntries(
        aea.prevLogIndex,
        aea.prevLogTerm,
        aea.entries,
        aea.leaderCommit,
      )
    ) {
      return { term: this.currentTerm, success: false };
    }
    if (this.status != RaftStatus.Leader) {
      this.leaderId = aea.leaderId;
    }
    if (this.status == RaftStatus.Follower) {
      // 重置心跳计时器
      this.heartbeatTimeout.reset();
    }
    return { term: this.currentTerm, success: true };
  }

  // 处理请求投票
  handleRequestVote(rv: RequestVoteArgs): RequestVoteReply {
    // 1. 如果term < currentTerm 返回 false
    if (rv.term < this.currentTerm) {
      return { term: this.currentTerm, voteGranted: false };
    }

    // 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，
    // 则令 currentTerm = T，并切换为跟随者状态
    if (rv.term > this.currentTerm) {
      this.currentTerm = rv.term;
      this.status = RaftStatus.Follower;
    }

    // 2. 如果 votedFor 为空或者为 candidateId
    //    并且候选人的日志至少和自己一样新，那么就投票给他
    if (this.votedFor === undefined || this.votedFor === rv.candidateId) {
      if (this.logs.isUpToDate(rv.lastLogIndex, rv.lastLogTerm)) {
        // 切换状态
        this.status = RaftStatus.Follower;
        this.votedFor = rv.candidateId;
        return { term: this.currentTerm, voteGranted: true };
      }
    }
    return { term: this.currentTerm, voteGranted: false };
  }

  handleAppend(command: string): () => ReceivePromise<void> {
    const logIndex = this.logs.append(command, this.currentTerm);
    const [emitter, receive] = signal();
    if (!(logIndex in this.commitEmitter)) {
      this.commitEmitter[logIndex] = [];
    }
    this.commitEmitter[logIndex].push(emitter);
    return receive;
  }

  isLeader(): boolean {
    return this.id === this.leaderId;
  }

  private async run() {
    while (true) {
      const r = await select({
        'shutdown': this.receiveShutdown(),
        '_': Promise.resolve(true),
      });
      if (r.key == 'shutdown') {
        return;
      }
      this.leaderId = undefined;
      switch (this.status) {
        case RaftStatus.Follower:
          await this.runFollower();
          break;
        case RaftStatus.Candidate:
          await this.runCandidate();
          break;
        case RaftStatus.Leader:
          await this.runLeader();
          break;
      }
    }
  }

  private async runFollower() {
    log.info(
      `entering follower state. id: ${this.id} term: ${this.currentTerm}`,
    );
    this.heartbeatTimeout.reset();
    while (this.status === RaftStatus.Follower) {
      const r = await select({
        'heartbeatTimeout': this.receiveHeartbeat(),
        'shutdown': this.receiveShutdown(),
      });
      switch (r.key) {
        case 'heartbeatTimeout':
          this.status = RaftStatus.Candidate;
          break;
        case 'shutdown':
          return;
      }
    }
  }

  private async runCandidate() {
    log.info(
      `entering candidate state. id: ${this.id} term: ${this.currentTerm}`,
    );
    while (this.status === RaftStatus.Candidate) {
      const r = await select({
        'electSelf': this.electSelf(),
        'shutdown': this.receiveShutdown(),
      });
      switch (r.key) {
        case 'electSelf': {
          // 如果接收到大多数服务器的选票，那么就变成领导人
          let grantedVotes = 1;
          const votesNeeded = this.quorumSize();
          for (const vote of r.value as RequestVoteReply[]) {
            if (vote.term > this.currentTerm) {
              log.warning(
                '[runCandidate] newer term discovered, fallback to follower',
              );
              this.status = RaftStatus.Follower;
              this.currentTerm = vote.term;
              return;
            }
            if (vote.voteGranted) {
              grantedVotes++;
            }
            if (grantedVotes >= votesNeeded) {
              log.info(`election won. tally: ${grantedVotes}`);
              this.status = RaftStatus.Leader;
              return;
            }
          }
          // 竞选失败
          await timeout(this.config.rpcTimeout);
          break;
        }
        case 'shutdown':
          return;
      }
    }
  }

  private async runLeader() {
    log.info(
      `entering leader state. leader: ${this.id} term: ${this.currentTerm}`,
    );
    this.commitEmitter = {};
    this.leaderId = this.id;

    // 对于每一台服务器，发送到该服务器的下一个日志条目的索引
    //（初始值为领导人最后的日志条目的索引 + 1）
    const nextIndex: { [key: number]: number } = {};
    // 对于每一台服务器，
    // 已知的已经复制到该服务器的最高日志条目的索引
    //（初始值为0，单调递增）
    const matchIndex: { [key: number]: number } = {};

    for (const id of this.peerIds()) {
      nextIndex[id] = this.logs.lastIndex() + 1;
      matchIndex[id] = 0;
    }
    while (this.status === RaftStatus.Leader) {
      const r = await select({
        'appendEntries': timeout(this.config.heartbeatInterval),
        'shutdown': this.receiveShutdown(),
      });

      switch (r.key) {
        case 'appendEntries': {
          const replies = await this.leaderSendHeartbeat(nextIndex);
          for (const reply of replies) {
            if (reply.term > this.currentTerm) {
              log.warning(
                '[runLeader] newer term discovered, fallback to follower',
              );
              this.status = RaftStatus.Follower;
              this.currentTerm = reply.term;
              return;
            }
            if (reply.success) {
              nextIndex[reply.peerId] += reply.appendLen;
              matchIndex[reply.peerId] = nextIndex[reply.peerId] - 1;
            } else {
              nextIndex[reply.peerId]--;
            }
          }
          const mi = this.majorityIndex(matchIndex);
          const oldCommitIndex = this.logs.commitIndex;
          if (mi > this.logs.commitIndex) {
            this.logs.commitIndex = mi;
          }

          // 通知等待commit的客户端
          for (let i = oldCommitIndex + 1; i <= this.logs.commitIndex; i++) {
            if (this.commitEmitter[i]) {
              for (const emitter of this.commitEmitter[i]) {
                emitter();
              }
            }
          }
          break;
        }
        case 'shutdown':
          return;
      }
    }
  }

  private async electSelf(): Promise<RequestVoteReply[]> {
    // 在转变成候选人后就立即开始选举过程
    //  自增当前的任期号（currentTerm）
    //  给自己投票
    //  重置选举超时计时器
    //  发送请求投票的 RPC 给其他所有服务器
    this.currentTerm++;
    this.votedFor = this.id;
    const lastLogEntry = this.logs.last();
    const voteReplies = await Promise.allSettled(
      this.peerIds().map((peerId) =>
        this.peers[peerId].requestVote({
          term: this.currentTerm,
          candidateId: this.id,
          lastLogIndex: lastLogEntry.logIndex,
          lastLogTerm: lastLogEntry.logTerm,
        }, this.config.rpcTimeout)
      ),
    );
    return voteReplies.filter((vote) => vote.status === 'fulfilled').map((
      vote,
    ) => (vote as PromiseFulfilledResult<RequestVoteReply>).value);
  }

  // 发送心跳给其他 follower
  private async leaderSendHeartbeat(
    nextIndex: { [id: number]: number },
  ): Promise<
    (AppendEntriesReply & { peerId: number; appendLen: number })[]
  > {
    const appendEntriesArgs: { [id: number]: AppendEntriesArgs } = {};
    for (const peerId of this.peerIds()) {
      appendEntriesArgs[peerId] = {
        term: this.currentTerm,
        leaderId: this.id,
        leaderCommit: this.logs.commitIndex,
        ...this.logs.batchEntries(nextIndex[peerId]),
      };
    }
    const replies = await Promise.allSettled(
      this.peerIds().map((peerId) =>
        this.peers[peerId].appendEntries(
          appendEntriesArgs[peerId],
          this.config.rpcTimeout,
        ).then((reply) => ({ ...reply, peerId }))
      ),
    );

    return replies.filter((reply) => reply.status === 'fulfilled')
      .map((replyResult) => {
        const reply = (replyResult as PromiseFulfilledResult<
          AppendEntriesReply & { peerId: number }
        >).value;
        return {
          ...reply,
          appendLen: appendEntriesArgs[reply.peerId].entries.length,
        };
      });
  }

  private majorityIndex(matchIndex: { [id: number]: number }): number {
    const arr = [];
    for (const mi in matchIndex) {
      arr.push(matchIndex[mi]);
    }
    arr.sort();
    return arr[Math.ceil(arr.length / 2)];
  }

  private peerIds(): number[] {
    return Object.keys(this.peers).map(Number);
  }

  private quorumSize(): number {
    return Object.keys(this.peers).length / 2 + 1;
  }
}

class ResettableTimeout {
  // 超时时间 (单位: 毫秒)
  delay: number;
  timer?: number;
  callback: () => void;

  constructor(delay: number, callback: () => void) {
    this.delay = delay;
    this.callback = callback;
  }

  stop() {
    clearTimeout(this.timer);
  }

  start() {
    this.timer = setTimeout(
      this.callback,
      this.delay + Math.round(Math.random() * this.delay),
    );
  }

  reset() {
    this.stop();
    this.start();
  }
}

export type AppendEntriesArgs = {
  // 领导人的任期
  term: number;
  // 领导人 ID 因此跟随者可以对客户端进行重定向
  // 跟随者根据领导人 ID 把客户端的请求重定向到领导人
  // 比如有时客户端把请求发给了跟随者而不是领导人
  leaderId: number;
  // 紧邻新日志条目之前的那个日志条目的索引
  prevLogIndex: number;
  // 紧邻新日志条目之前的那个日志条目的任期
  prevLogTerm: number;
  // 需要被保存的日志条目
  // 被当做心跳使用时，则日志条目内容为空
  // 为了提高效率可能一次性发送多个
  entries: LogEntry[];
  // 领导人的已知已提交的最高的日志条目的索引
  leaderCommit: number;
};

export type AppendEntriesReply = {
  // 当前任期，对于领导人而言 它会更新自己的任期
  term: number;
  // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
  success: boolean;
};

export type RequestVoteArgs = {
  // 候选人的任期号
  term: number;
  // 请求选票的候选人的 ID
  candidateId: number;
  // 候选人的最后日志条目的索引值
  lastLogIndex: number;
  // 候选人最后日志条目的任期号
  lastLogTerm: number;
};

export type RequestVoteReply = {
  // 当前任期号，以便于候选人去更新自己的任期号
  term: number;
  // 候选人赢得了此张选票时为真
  voteGranted: boolean;
};
