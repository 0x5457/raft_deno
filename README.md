# raft_deno

typescript implementation of the raft.

### raft kv example

#### start raft kv server

```sh
# node 1
deno run --allow-net ./example/server.ts --local http://127.0.0.1:8080 --peer http://127.0.0.1:8081 --peer http://127.0.0.1:8082

# node 2
deno run --allow-net ./example/server.ts --local http://127.0.0.1:8081 --peer http://127.0.0.1:8080 --peer http://127.0.0.1:8082

# node 3
deno run --allow-net ./example/server.ts --local http://127.0.0.1:8082 --peer http://127.0.0.1:8080 --peer http://127.0.0.1:8081
```

#### raft kv client

```sh
# set
deno run --allow-net ./example/client.ts set <key> <value> -a http://127.0.0.1:8080

# get
deno run --allow-net ./example/client.ts get <key> -a http://127.0.0.1:8080

# rm
deno run --allow-net ./example/client.ts rm <key> -a http://127.0.0.1:8080
```
