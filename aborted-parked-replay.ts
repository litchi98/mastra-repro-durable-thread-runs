/**
 * A thread run that suspended, was resumed and suspended again, and was then stopped with the server's
 * run abort route (`POST /agents/:agentId/runs/:runId/abort`), is replayed to a new `subscribeToThread()`
 * as `start` frames (and possibly its old suspension), but without the `aborted` outcome.
 *
 * Needs a Redis for the cross-process part: REDIS_URL (default redis://127.0.0.1:6379), e.g.
 *   docker run --rm -p 6379:6379 redis:7-alpine
 *
 *   main (this process)  : sendMessage -> suspends -> sendStreamResume -> suspends again -> abort route
 *                          -> subscribeToThread (same process, new subscription)
 *   observer (child)     : a fresh process that only calls subscribeToThread on the same thread
 *
 * Run: npm run repro:aborted-parked-replay
 */
import { spawn } from 'node:child_process';
import { Mastra } from '@mastra/core/mastra';
import { RequestContext } from '@mastra/core/request-context';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import { RedisStreamsPubSub } from '@mastra/redis-streams';
import { ABORT_AGENT_RUN_ROUTE } from '@mastra/server/handlers/agents';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import corePkg from '@mastra/core/package.json' with { type: 'json' };
import { durableAgent, lastUserText, mockModel, observeThread, sleep } from './shared.ts';

const role = process.argv[2] ?? 'main';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const TMP = join(import.meta.dirname, '.tmp', 'aborted-parked');
const resourceId = 'user-1';
const threadId = process.argv[3] ?? `thread-${Date.now()}`;

// 'plain' -> text. Otherwise: ask_user (call-ask-1); after its answer: ask_user again (call-ask-2); then text.
const model = mockModel(prompt => {
  if (lastUserText(prompt).includes('plain')) return { text: 'Plain reply.' };
  const answers = prompt.filter(m => m.role === 'tool').length;
  if (answers < 2) return { toolCall: { id: `call-ask-${answers + 1}`, name: 'ask_user', args: { question: `Question ${answers + 1}?` } } };
  return { text: 'Done.' };
});

if (role === 'main') {
  rmSync(TMP, { recursive: true, force: true });
  mkdirSync(TMP, { recursive: true });
}
const storage = new LibSQLStore({ id: 'store', url: `file:${join(TMP, 'repro.db')}` });
const memory = new Memory({ storage });
const mastra = new Mastra({
  agents: { agent: durableAgent({ model, memory }) as any },
  storage,
  pubsub: new RedisStreamsPubSub({ url: REDIS_URL, keyPrefix: `repro-${process.env.KEY ?? threadId}` }),
  logger: false,
});
const agent: any = mastra.getAgent('agent');

if (role === 'observer') {
  const ctl = await observeThread(agent, resourceId, `${threadId}-control`);
  const obs = await observeThread(agent, resourceId, threadId);
  await sleep(3_000);
  console.log(`  [new process] control thread replay:  ${JSON.stringify(ctl.log)}`);
  console.log(`  [new process] aborted thread replay:  ${JSON.stringify(obs.log)}`);
  ctl.close();
  obs.close();
  await mastra.shutdown();
  process.exit(0);
}

console.log(`@mastra/core ${corePkg.version}, PubSub: RedisStreamsPubSub`);

// Control: a run that completes.
{
  const ctl = await observeThread(agent, resourceId, `${threadId}-control`);
  await agent.sendMessage('plain', { resourceId, threadId: `${threadId}-control` }).accepted;
  await ctl.until(['success', 'failed']);
  ctl.close();
  const again = await observeThread(agent, resourceId, `${threadId}-control`);
  await sleep(2_000);
  console.log(`Control: completed run; a new subscribeToThread replays: ${JSON.stringify(again.log)}\n`);
  again.close();
}

const live = await observeThread(agent, resourceId, threadId);

const a: any = await agent.sendMessage('hello', { resourceId, threadId }).accepted;
const runId = a.runId;
console.log(`sendMessage -> ${a.action} ${runId}`);
await live.until(['tool-call-suspended(call-ask-1)']);
console.log(`  suspended on call-ask-1`);

await agent.sendStreamResume({ resourceId, threadId, runId, toolCallId: 'call-ask-1', resumeData: { answer: 'A' } });
await live.until(['tool-call-suspended(call-ask-2)']);
console.log(`  resumed, suspended again on call-ask-2`);
await sleep(500);
console.log(`  live subscriber so far: ${JSON.stringify(live.log)}`);

const res = await ABORT_AGENT_RUN_ROUTE.handler({
  mastra,
  agentId: 'agent',
  runId,
  requestContext: new RequestContext(),
} as any);
console.log(`\nPOST /agents/agent/runs/:runId/abort -> ${JSON.stringify(res)}`);
await sleep(1_000);
console.log(`  live subscriber after the abort: ${JSON.stringify(live.log)}`);
const { runs } = await agent.listSuspendedRuns({ resourceId, threadId });
console.log(`  listSuspendedRuns: ${runs.length}`);
live.close();

const again = await observeThread(agent, resourceId, threadId);
await sleep(3_000);
console.log(`\n  [same process] aborted thread replay:  ${JSON.stringify(again.log)}`);
again.close();

await new Promise<void>(resolve =>
  spawn(process.execPath, ['--import', 'tsx', import.meta.filename, 'observer', threadId], {
    stdio: 'inherit',
    env: { ...process.env, KEY: threadId },
  }).on('exit', () => resolve()),
);

await mastra.shutdown();
process.exit(0);
