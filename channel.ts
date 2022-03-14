export function signal(): [() => void, () => ReceivePromise<void>] {
  const [emit, on] = channel<boolean>();

  function send() {
    emit(true);
  }

  function receive(): ReceivePromise<void> {
    const o = on();
    return {
      cancel: o.cancel,
      promise: o.promise.then(() => undefined),
    };
  }
  return [send, receive];
}

export function channel<T>(): [(value: T) => void, () => ReceivePromise<T>] {
  const list: T[] = [];
  const wakers: ((value: T) => void)[] = [];

  function send(value: T) {
    const waker = wakers.shift();
    if (waker) {
      waker(value);
      return;
    }
    list.push(value);
  }

  function receive(): ReceivePromise<T> {
    let wakerIdx: number;
    return {
      promise: new Promise((resolve) => {
        const value = list.shift();
        if (value !== undefined) {
          resolve(value);
          return;
        }
        wakerIdx = wakers.length;
        wakers.push(resolve);
      }),
      cancel: () => {
        wakers.splice(wakerIdx, 1);
      },
    };
  }
  return [send, receive];
}

export type ReceivePromise<T> = {
  promise: Promise<T>;
  cancel: () => void;
};

function wrapper<T>(
  key: string,
  p: Promise<T>,
): Promise<{ key: string; value: T }> {
  return new Promise((receive, reject) =>
    p.then((value) => receive({ key, value }))
      .catch(reject)
  );
}

export function timeout(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function randomTimeout(ms: number): Promise<void> {
  return timeout(ms + Math.round(Math.random() * ms));
}

export async function select(
  ps: { [key: string]: Promise<unknown> | ReceivePromise<unknown> },
): Promise<{ key: string; value: unknown }> {
  const keys = Object.keys(ps);
  const resp = await Promise.any(keys.map((k) => {
    const p = ps[k];
    if ('promise' in p) {
      return wrapper(k, p.promise);
    }
    return wrapper(k, p);
  }));

  for (const k of keys) {
    const p = ps[k];
    if ('cancel' in p) {
      if (k != resp.key) {
        p.cancel();
      }
    }
  }
  return resp;
}
