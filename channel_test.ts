import { assertEquals } from './deps.ts';
import { channel, select, signal, timeout } from './channel.ts';

Deno.test('channel test #1', async () => {
  const [send, receive] = channel<number>();
  const [stop, done] = signal();

  (async () => {
    assertEquals(await receive().promise, 1);
    assertEquals(await receive().promise, 2);
    assertEquals(await receive().promise, 3);
    assertEquals(await receive().promise, 4);
    assertEquals(
      await Promise.any([receive().promise, timeout(10)]),
      undefined,
    );
    stop();
  })();
  send(1);
  send(2);
  send(3);
  await timeout(100);
  send(4);
  await done().promise;
});

Deno.test('channel test #2', async () => {
  const [send, receive] = channel<number>();
  const [stop, done] = signal();

  (async () => {
    const resp = await select({ 'r': receive(), 't': timeout(10) });
    assertEquals(resp.key, 't');
    assertEquals(await receive().promise, 1);
    stop();
  })();

  await timeout(100);
  send(1);
  await done().promise;
});
