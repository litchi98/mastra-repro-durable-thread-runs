# Repro: DurableAgent thread runs (total timeout, recovery suspension, aborted-run replay) and redis-streams unsubscribe

Minimal reproductions for three Mastra bug reports about `DurableAgent` runs started through the thread API (`sendMessage` / `sendStreamResume` / `subscribeToThread`), and one about `@mastra/redis-streams`. Each report has its own script.

- No API keys, no network: the model is a `MockLanguageModelV2` from `ai/test`.
- Storage: libsql (in-memory, or a file under `.tmp/`).
- PubSub: the in-process default for `total-timeout.ts` and `recovery-suspend.ts`. `aborted-parked-replay.ts` and `redis-unsubscribe.ts` need a local Redis (`REDIS_URL`, default `redis://127.0.0.1:6379`):

  ```bash
  docker run --rm -p 6379:6379 redis:7-alpine
  ```

- Versions: `@mastra/core@1.83.0`, `@mastra/server@1.83.0`, `@mastra/memory@1.41.1`, `@mastra/libsql@1.30.0`, `@mastra/redis-streams@0.8.1`. Same results with `@mastra/core@1.84.0-alpha.4` / `@mastra/redis-streams@0.9.0-alpha.2` (`npm run use:alpha`).

Environment: Node 26.13.0, npm 11.20.0, macOS 27 (arm64), Redis 7.4. Runtime only: no `mastra build` / `mastra dev` involved.

```bash
npm install
npm run repro:total-timeout          # 1
npm run repro:recovery-suspend       # 2
npm run repro:aborted-parked-replay  # 3 (Redis)
npm run repro:redis-unsubscribe      # 4 (Redis)
```

`shared.ts` holds the mock model, the tools (`ask_user` suspends until it gets resume data, `lookup` returns right away), the agent factory and a `subscribeToThread({ withRunStatus: true })` logger.

---

## 1. `total-timeout.ts`: a thread run that hits `modelSettings.timeout.totalMs` never settles on the thread

`sendMessage(..., { ifActive: { behavior: 'deliver' }, ifIdle: { behavior: 'wake', streamOptions: { modelSettings: { timeout: { totalMs: 2000 } } } } })`. The model call for `hang` only ends when its abort signal fires.

### Actual output (1.83.0)

```
Control: model call throws (400). sendMessage -> wake <runId>
  subscriber saw: ["start","step-start","error(Error)","thread-run-status:failed"]

[0.6s] A. sendMessage('hang') -> wake <runId>; model call hangs
[2.6s] subscriber got: error(MastraTimeoutError)
[12.7s] (1) final thread-run-status within 10 s after the error: none
    subscriber saw: ["start","error(MastraTimeoutError)"]
[12.7s] (2) getThreadLeaseOwnerRunId() -> the timed-out run; listActiveRuns() -> []
[12.7s] B. sendMessage('second message') -> deliver into the timed-out run
[22.7s]    answered within 10 s: no
[22.7s] thread lease of the timed-out run gone
[24.7s] stored messages: ["signal: hang"]
[24.7s] C. sendMessage('third message') -> wake (new run); answered: yes
```

- (1) The subscriber gets `error` but never a final `thread-run-status`. When the model call fails instead, `error` is followed by `failed`.
- (2) The timed-out run keeps the thread lease. `listActiveRuns()` is already empty. The next `sendMessage` is routed `deliver` into the dead run. Its message is never answered or stored. Only after the lease lapses does a new message wake a new run.

### Expected

A total timeout ends the run on the thread like any other failure: the run gets a final `thread-run-status` (`failed`, or `aborted`), and its thread lease is released, so the next message wakes a new run.

---

## 2. `recovery-suspend.ts`: a run that suspends during crash recovery publishes `tool-call-suspended` before its snapshot is saved

- **worker** (child process): `sendMessage`. Step 1 calls `lookup`. The step-2 model call hangs, and the worker is SIGKILLed.
- **this process**: `recoverAllDurableAgents()` (retried until the dead worker's lease lapses). The step-2 model call now calls `ask_user`, which suspends.

A `PubSub.publish` hook checks `listSuspendedRuns()` at the moment the suspension chunk is published, then polls until the run is listed. Control: a fresh run that suspends through `start()`. `SLOW_SNAPSHOT_MS` (default 200) adds the same latency to every workflow-snapshot write in both cases, like a remote database. With `SLOW_SNAPSHOT_MS=0` the result is the same, with a window of about 10 ms.

### Actual output (1.83.0)

```
Control: fresh run <runId> suspends via start()
  run in listSuspendedRuns() when the suspension chunk was published: yes; listed after 2 ms

Recovery: a run that suspends while being recovered
  [worker] step-2 model call is hanging; waiting to be killed
  [worker] SIGKILLed during step 2
  recoverAllDurableAgents() -> {"agents":1,"recovered":1,"succeeded":1,"failed":0,"skipped":0} after 17.1 s
  run in listSuspendedRuns() when the suspension chunk was published: NO; listed after 810 ms
```

### Expected

As with `start()` / `resume()`: the suspension is announced only after the suspended snapshot is saved, so a client that answers right away can resume the run.

---

## 3. `aborted-parked-replay.ts`: replay of a run aborted while parked has no final outcome, and can re-send an answered question

`RedisStreamsPubSub`. `sendMessage` → `ask_user` suspends (`call-ask-1`) → `sendStreamResume` → `ask_user` suspends again (`call-ask-2`) → `POST /agents/:agentId/runs/:runId/abort` (the `ABORT_AGENT_RUN_ROUTE` handler from `@mastra/server`). Then `subscribeToThread` again, in the same process and in a new process. Control: a run that completed on another thread.

### Actual output (1.83.0)

```
Control: completed run; a new subscribeToThread replays: ["thread-run-status:success"]

  live subscriber so far: ["start","step-start","tool-call-suspended(call-ask-1)","tool-call-resumed","step-start","tool-call-suspended(call-ask-2)"]

POST /agents/agent/runs/:runId/abort -> {"aborted":true}
  live subscriber after the abort: [...,"tool-call-suspended(call-ask-2)","abort","thread-run-status:suspended","thread-run-status:aborted"]
  listSuspendedRuns: 0

  [same process] aborted thread replay:  ["start","tool-call-suspended(call-ask-1)","abort"]
  [new process] control thread replay:  ["thread-run-status:success"]
  [new process] aborted thread replay:  ["start","abort"]
```

- The live subscriber gets `thread-run-status: aborted`. A late subscriber gets the run's `start` (the first segment's) and an `abort` chunk, but no final `thread-run-status`. A completed run replays as its final status.
- In the same process, the replay also contains the first segment's `tool-call-suspended(call-ask-1)`, a question that was already answered.

### Expected

A late subscriber gets the aborted run's final outcome, as the ChunkType reference says for opted-in late subscribers ("one outcome per still-retained finished run"), and no suspension of a segment that has already been resumed.

---

## 4. `redis-unsubscribe.ts`: unsubscribing an idle consumer-group subscription waits for the blocking read

- **A.** Five `subscribe(topic, cb, { group: 'workers' })` on one `RedisStreamsPubSub`, 1.5 s idle, then `unsubscribe()` one by one.
- **B.** `new Mastra({ pubsub: new RedisStreamsPubSub(...) })` with one durable agent, `startWorkers()`, 1.5 s idle, then `stopWorkers()`.

### Actual output (1.83.0)

```
A. blockMs = default: unsubscribe() x5 -> 532ms / 0ms / 1011ms / 0ms / 1011ms (total 2555ms)
A. blockMs = 100: unsubscribe() x5 -> 114ms / 0ms / 101ms / 0ms / 101ms (total 316ms)
B. blockMs = default: mastra.stopWorkers() on an idle instance -> 1616ms
B. blockMs = 100: mastra.stopWorkers() on an idle instance -> 205ms
```

Each `unsubscribe()` waits up to `blockMs` for the reader's in-flight `XREADGROUP ... BLOCK`, because the reader is closed with `QUIT`, which Redis answers only after the blocking read returns. `stopWorkers()` and `PullTransport.stop()` unsubscribe one after another, so the waits add up.

### Expected

Unsubscribing an idle subscription doesn't wait for the blocking read, so `stopWorkers()` / `shutdown()` don't spend their drain budget on idle readers.
