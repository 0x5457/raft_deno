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
  const list: LinkedList<T> = new LinkedList();
  const wakers: LinkedList<(value: T) => void> = new LinkedList();

  function send(value: T) {
    const waker = wakers.shift();
    if (waker) {
      waker(value);
      return;
    }
    list.push(value);
  }

  function receive(): ReceivePromise<T> {
    let waker: Node<(value: T) => void>;
    return {
      promise: new Promise((resolve) => {
        const value = list.shift();
        if (value !== undefined) {
          resolve(value);
          return;
        }
        waker = wakers.push(resolve);
      }),
      cancel: () => wakers.remove(waker)
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


class Node<E> {

  // deno-lint-ignore no-explicit-any
  static readonly Undefined = new Node<any>(undefined);

  element: E;
  next: Node<E>;
  prev: Node<E>;

  constructor(element: E) {
    this.element = element;
    this.next = Node.Undefined;
    this.prev = Node.Undefined;
  }
}

export class LinkedList<E> {

  private _first: Node<E> = Node.Undefined;
  private _last: Node<E> = Node.Undefined;
  private _size = 0;

  get size(): number {
    return this._size;
  }

  isEmpty(): boolean {
    return this._first === Node.Undefined;
  }

  unshift(element: E): Node<E> {
    return this._insert(element, false);
  }

  push(element: E): Node<E> {
    return this._insert(element, true);
  }

  private _insert(element: E, atTheEnd: boolean): Node<E> {
    const newNode = new Node(element);
    if (this._first === Node.Undefined) {
      this._first = newNode;
      this._last = newNode;

    } else if (atTheEnd) {
      // push
      const oldLast = this._last!;
      this._last = newNode;
      newNode.prev = oldLast;
      oldLast.next = newNode;

    } else {
      // unshift
      const oldFirst = this._first;
      this._first = newNode;
      newNode.next = oldFirst;
      oldFirst.prev = newNode;
    }
    this._size += 1;
    return newNode;
  }

  shift(): E | undefined {
    if (this._first === Node.Undefined) {
      return undefined;
    } else {
      const res = this._first.element;
      this.remove(this._first);
      return res;
    }
  }

  pop(): E | undefined {
    if (this._last === Node.Undefined) {
      return undefined;
    } else {
      const res = this._last.element;
      this.remove(this._last);
      return res;
    }
  }

  remove(node: Node<E>): void {
    if (node.prev !== Node.Undefined && node.next !== Node.Undefined) {
      // middle
      const anchor = node.prev;
      anchor.next = node.next;
      node.next.prev = anchor;

    } else if (node.prev === Node.Undefined && node.next === Node.Undefined) {
      // only node
      this._first = Node.Undefined;
      this._last = Node.Undefined;

    } else if (node.next === Node.Undefined) {
      // last
      this._last = this._last!.prev!;
      this._last.next = Node.Undefined;

    } else if (node.prev === Node.Undefined) {
      // first
      this._first = this._first!.next!;
      this._first.prev = Node.Undefined;
    }

    // done
    this._size -= 1;
  }
}