import { Command } from 'https://deno.land/x/cliffy@v0.20.1/command/mod.ts';
import * as msgpack from 'https://deno.land/x/msgpack@v1.4/mod.ts';
import * as base64 from 'https://deno.land/std@0.129.0/encoding/base64.ts';

type Option = { addr: string };
async function set(op: Option, key: string, value: string) {
  const command = base64.encode(msgpack.encode({ type: 'set', key, value }));
  type B = { command: string };
  type R = { success: boolean };
  console.time('latency');
  const resp = await send<B, R>('POST', op.addr, '/append', { command });
  console.log(resp);
  console.timeEnd('latency');
}

async function get(op: Option, key: string) {
  type B = Record<never, never>;
  type R = { value: string | null };
  console.time('latency');
  const resp = await send<B, R>('GET', op.addr, `/get?key=${key}`);
  console.log(resp);
  console.timeEnd('latency');
}

async function rm(op: Option, key: string) {
  const command = base64.encode(msgpack.encode({ type: 'rm', key }));
  type B = { command: string };
  type R = { success: boolean };
  console.time('latency');
  const resp = await send<B, R>('POST', op.addr, '/append', { command });
  console.log(resp);
  console.timeEnd('latency');
}

async function send<B, R>(
  method: string,
  addr: string,
  path: string,
  body: B | null = null,
): Promise<R> {
  const resp = await fetch(addr + path, {
    method,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (resp.ok) {
    return (await resp.json()) as R;
  }
  if (resp.status === 400) {
    const { leaderAddr } = await resp.json();
    return send<B, R>(method, leaderAddr, path, body);
  }
  throw new Error(await resp.text());
}

await new Command()
  .option('-a, --addr <addr:string>', 'server addr.', { global: true })
  .command(
    'set <key:string> <value:string>',
    'Set the value of a string key to a string',
  )
  .action(set)
  .command('get <key:string>', 'Get the string value of a given string key')
  .action(get)
  .command('rm <key:string>', 'Remove a given key')
  .action(rm)
  .parse(Deno.args);
