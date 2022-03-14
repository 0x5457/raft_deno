import { serve } from 'https://deno.land/std@0.129.0/http/server.ts';
import {
  OptionType,
  parseFlags,
} from 'https://deno.land/x/cliffy@v0.20.1/flags/mod.ts';
import { assert } from 'https://deno.land/std@0.129.0/testing/asserts.ts';
import {
  HttpPeer,
  Logs,
  MemStateMachine,
  MemStorage,
  Raft,
  select,
  timeout,
} from '../mod.ts';

const { flags } = parseFlags(Deno.args, {
  flags: [{
    name: 'local',
    type: OptionType.STRING,
    required: true,
  }, {
    name: 'peer',
    collect: true,
    type: OptionType.STRING,
    required: true,
  }, {
    name: 'rpcTimeout',
    type: OptionType.NUMBER,
    default: 100,
  }, {
    name: 'heartbeatTimeout',
    type: OptionType.NUMBER,
    default: 200,
  }, {
    name: 'heartbeatInterval',
    type: OptionType.NUMBER,
    default: 100,
  }],
});
assert(flags.peer.length >= 2, 'at least 2 peers');

const port = Number(new URL(flags.local).port);
const nodeAddr: string[] = [flags.local, ...flags.peer];
nodeAddr.sort();

let id = 0;
const peers: { [id: number]: HttpPeer } = {};

for (let i = 0; i < nodeAddr.length; i++) {
  if (nodeAddr[i] === flags.local) {
    id = i;
  } else {
    peers[i] = new HttpPeer(nodeAddr[i]);
  }
}

const stateMachine = new MemStateMachine();
httpServe(
  new Raft(
    id,
    new Logs(new MemStorage(), stateMachine),
    peers,
    {
      rpcTimeout: flags.rpcTimeout,
      heartbeatTimeout: flags.heartbeatTimeout,
      heartbeatInterval: flags.heartbeatInterval,
    },
  ),
  stateMachine,
  port,
);

function httpServe(
  r: Raft,
  stateMachine: MemStateMachine,
  port: number,
) {
  serve(async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    let body;
    try {
      body = await req.json();
    } catch {
      body = {};
    }
    if (url.pathname === '/append_entries') {
      return new Response(JSON.stringify(r.handleAppendEntries(body)));
    }
    if (url.pathname === '/request_vote') {
      return new Response(JSON.stringify(r.handleRequestVote(body)));
    }

    if (r.leaderId === undefined) {
      return new Response('no leader', { status: 500 });
    }

    if (!r.isLeader()) {
      return new Response(
        JSON.stringify({ leaderAddr: peers[r.leaderId].addr }),
        {
          status: 400,
        },
      );
    }
    if (url.pathname === '/append') {
      const commandBase64 = (body as { command: string }).command;
      const receiveHandleAppend = r.handleAppend(
        commandBase64,
      );
      const s = await select({
        'receiveHandleAppend': receiveHandleAppend(),
        'timeout': timeout(400),
      });
      switch (s.key) {
        case 'timeout':
          return new Response(JSON.stringify({ success: false }));
        case 'receiveHandleAppend':
          return new Response(JSON.stringify({ success: true }));
      }
    }

    if (url.pathname === '/get') {
      const key = url.searchParams.get('key');
      return new Response(
        JSON.stringify({ value: key ? stateMachine.get(key) : null }),
      );
    }
    return new Response('hello raft');
  }, { port });
}
