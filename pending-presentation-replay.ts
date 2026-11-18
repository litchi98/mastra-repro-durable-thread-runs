/**
 * `data-user-message-pending` is published on the thread topic as a thread-scoped control event
 * (`user-message-presentation`, envelope `runId` = the thread key), so `trimTopic({ runId })` never removes it. It
 * stays in the thread stream (redis-streams: until the 7-day idle TTL), and every later `subscribeToThread()` replays
 * it. A subscriber drops it only if it loaded history with the same message id (`withInitialHistory`), or saw
 * `user-message-delivered` / `data-user-message-cancelled` for that id before it yielded the chunk.
 *
 *   Control (no crash): queueMessage() while a run is in flight; both messages are answered. A subscriber without
 *                       `withInitialHistory` still gets the queued message as `data-user-message-pending` afterwards.
 *   Crash:              the process holding the queued message (worker, child) is SIGKILLed. The message never reaches
 *                       history, so every subscriber, with or without history, gets it as pending, also after the
 *                       recovered run and a later run on the thread have finished.
 *   observer (child):   a fresh process that only calls subscribeToThread().
 *
 * Needs a Redis: REDIS_URL (default redis://127.0.0.1:6379).
 *
 * Run: npm run repro:pending-presentation-replay
 */
import { spawn } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import { RedisStreamsPubSub } from '@mastra/redis-streams';
import { createClient } from 'redis';
import corePkg from '@mastra/core/package.json' with { type: 'json' };
import { durableAgent, lastUserText, mockModel, sleep, storedMessages } from './shared.ts';

const [role = 'main', baseThread = `thread-${Date.now()}`, threadArg] = process.argv.slice(2);
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const TMP = join(import.meta.dirname, '.tmp', 'pending-presentation');
const resourceId = 'user-1';
const keyPrefix = `repro-pending-${baseThread}`;

// 'hold' messages: the model call hangs in the worker, and waits for release() in this process.
let release = () => {};
let held = new Promise<void>(r => (release = r));
const model = mockModel(
  prompt => ({ text: `Reply to: ${lastUserText(prompt)}` }),
  async (_signal, prompt) => {
    if (!lastUserText(prompt).includes('hold')) return;
    process.send?.('hanging');
    if (role === 'worker') await new Promise(() => {});
    await held;
  },
);

if (role === 'main') {
  rmSync(TMP, { recursive: true, force: true });
  mkdirSync(TMP, { recursive: true });
}
const storage = new LibSQLStore({ id: 'store', url: `file:${join(TMP, 'repro.db')}` });
const memory = new Memory({ storage });
const mastra = new Mastra({
  agents: { agent: durableAgent({ model, memory }) as any },
  storage,
  pubsub: new RedisStreamsPubSub({ url: REDIS_URL, keyPrefix }),
  recovery: { durableAgents: 'auto' },
  logger: false,
});
const agent: any = mastra.getAgent('agent');
const wake = (threadId: string, text: string) => agent.sendMessage(text, { resourceId, threadId }).accepted;
const queue = (threadId: string, text: string) => agent.queueMessage(text, { resourceId, threadId }).accepted;

/** Subscribe, wait, and return the user-message presentation chunks (and the outcome) a new subscriber gets. */
async function replay(threadId: string, withInitialHistory = false, ms = 3_000) {
  const sub = await agent.subscribeToThread({ resourceId, threadId, withRunStatus: true, withInitialHistory });
  const seen: string[] = [];
  void (async () => {
    try {
      for await (const c of sub.stream as AsyncIterable<any>) {
        if (c.type === 'thread-history') seen.push(`thread-history(${c.payload?.messages?.length} messages)`);
        else if (c.type?.startsWith('data-user-message')) seen.push(`${c.type}(${c.data?.id}, ${c.data?.metadata?.status ?? ''})`);
        else if (c.type === 'thread-run-status') seen.push(`thread-run-status:${c.payload?.status}`);
      }
    } catch {}
  })();
  await sleep(ms);
  sub.unsubscribe?.();
  return seen;
}

if (role === 'worker') {
  const threadId = threadArg!;
  let hanging!: () => void;
  const isHanging = new Promise<void>(r => (hanging = r));
  const origSend = process.send!.bind(process);
  (process as any).send = (m: any) => (m === 'hanging' ? hanging() : origSend(m));
  const first: any = await wake(threadId, 'hold: first');
  await isHanging;
  const second: any = await queue(threadId, 'second');
  origSend(JSON.stringify({ firstAction: first.action, secondAction: second.action }));
  await new Promise(() => {});
}

if (role === 'observer') {
  const threadId = threadArg!;
  console.log(`  [new process]  subscribeToThread():                             ${JSON.stringify(await replay(threadId))}`);
  console.log(`  [new process]  subscribeToThread({ withInitialHistory: true }): ${JSON.stringify(await replay(threadId, true))}`);
  await mastra.shutdown();
  process.exit(0);
}

async function waitForMessages(threadId: string, n: number) {
  for (let i = 0; i < 150 && (await storedMessages(memory, resourceId, threadId)).length < n; i++) await sleep(100);
  return storedMessages(memory, resourceId, threadId);
}

async function newSubscriptions(threadId: string) {
  console.log(`  [same process] subscribeToThread():                             ${JSON.stringify(await replay(threadId))}`);
  console.log(`  [same process] subscribeToThread({ withInitialHistory: true }): ${JSON.stringify(await replay(threadId, true))}`);
  const observer = spawn(process.execPath, ['--import', 'tsx', import.meta.filename, 'observer', baseThread, threadId], {
    stdio: 'inherit',
  });
  await new Promise(r => observer.on('exit', r));
}

// ---------------------------------------------------------------------------------------------
console.log(`@mastra/core ${corePkg.version}, PubSub: RedisStreamsPubSub`);

// Control: no crash. queueMessage while a run is in flight; the queued message runs after it.
{
  const threadId = `${baseThread}-control`;
  (process as any).send = (m: any) => m === 'hanging' && (process as any).emit('hanging');
  const hanging = new Promise(r => (process as any).once('hanging', r));
  const first: any = await wake(threadId, 'hold: first');
  await hanging;
  const second: any = await queue(threadId, 'second');
  release();
  console.log(`\nControl (no crash): sendMessage('hold: first') -> ${first.action}; queueMessage('second') while it runs -> ${second.action}`);
  console.log(`  stored messages: ${JSON.stringify(await waitForMessages(threadId, 4))}`);
  await sleep(1_000);
  await newSubscriptions(threadId);
}

// The process that holds the queued message dies.
const threadId = `${baseThread}-crash`;
console.log(`\nCrash: the process holding the queued message is SIGKILLed`);
const worker = spawn(process.execPath, ['--import', 'tsx', import.meta.filename, 'worker', baseThread, threadId], {
  stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
});
const info: any = await new Promise(r => worker.on('message', m => r(JSON.parse(String(m)))));
console.log(`  [worker] sendMessage('hold: first') -> ${info.firstAction}; model call hangs`);
console.log(`  [worker] queueMessage('second') -> ${info.secondAction}`);
await sleep(500);
worker.kill('SIGKILL');
await new Promise(r => worker.on('exit', r));
console.log('  [worker] SIGKILLed');

console.log(`  subscribeToThread({ withInitialHistory: true }) right after: ${JSON.stringify(await replay(threadId, true))}`);
release();
const t = Date.now();
let result: any;
while (Date.now() - t < 60_000) {
  result = await mastra.recoverAllDurableAgents();
  if (result.recovered > 0) break;
  await sleep(1_000);
}
console.log(`  recoverAllDurableAgents() -> ${JSON.stringify(result)} after ${((Date.now() - t) / 1000).toFixed(1)} s`);
console.log(`  stored messages: ${JSON.stringify(await waitForMessages(threadId, 2))}`);
await sleep(1_000);
const third: any = await wake(threadId, 'third');
console.log(`  sendMessage('third') -> ${third.action}`);
console.log(`  stored messages: ${JSON.stringify(await waitForMessages(threadId, 4))}`);
await sleep(1_000);
console.log(`  Thread is idle; 'second' (lost with the worker) is not in history. New subscriptions:`);
await newSubscriptions(threadId);

// What is left in the thread topic streams.
const redis = createClient({ url: REDIS_URL });
await redis.connect();
for (const key of await redis.keys(`${keyPrefix}:*`)) {
  if ((await redis.type(key)) !== 'stream') continue;
  const entries = (await redis.xRange(key, '-', '+')).map(e => JSON.parse(e.message.event ?? '{}'));
  const pending = entries.filter(ev => ev.data?.type === 'user-message-presentation');
  if (!pending.length) continue;
  console.log(`\nRedis ${key.slice(keyPrefix.length + 1)} (TTL ${(await redis.pTTL(key)) / 3_600_000 | 0} h), ${entries.length} entries:`);
  console.log(`  ${JSON.stringify(entries.map(ev => ev.data?.type ?? ev.type))}`);
  for (const ev of pending) {
    console.log(`  user-message-presentation: envelope runId = ${JSON.stringify(ev.runId)} (thread key), chunk ${ev.data.chunk.type}(${ev.data.chunk.data.id}, ${ev.data.chunk.data.metadata?.status}), chunk.runId = ${ev.data.chunk.runId}`);
  }
}
await redis.quit();
await mastra.shutdown();
process.exit(0);
