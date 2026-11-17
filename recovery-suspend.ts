/**
 * A run that suspends during crash recovery publishes `tool-call-suspended` before its suspended snapshot is
 * saved. start()/resume() hold suspension events back until the snapshot is saved (`withSuspensionAfterPersist`,
 * #26435); the recovery path (`recover()` -> `run.restart()`) does not.
 *
 *   worker  (child process): sendMessage; step 1 calls `lookup`, the step-2 model call hangs; it is then SIGKILLed.
 *   this process           : `recoverAllDurableAgents()`; the step-2 model call now asks `ask_user`, which suspends.
 *
 * At the moment the suspension chunk is published (PubSub.publish hook), the script asks `listSuspendedRuns()`
 * whether the run is there yet, then polls until it is. Control: a fresh run that suspends via start().
 * SLOW_SNAPSHOT_MS (default 200) adds latency to the workflow-snapshot writes (like a remote database) to make the
 * window visible; set it to 0 to use plain local libsql.
 *
 * Run: npm run repro:recovery-suspend
 */
import { spawn } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { EventEmitterPubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import corePkg from '@mastra/core/package.json' with { type: 'json' };
import { durableAgent, mockModel, sleep } from './shared.ts';

const TMP = join(import.meta.dirname, '.tmp', 'recovery');
const DB_URL = `file:${join(TMP, 'repro.db')}`;
const resourceId = 'user-1';
const role = process.argv[2] ?? 'main';
const SLOW_MS = Number(process.env.SLOW_SNAPSHOT_MS ?? 200);

// Step 1 -> lookup; step 2 -> ask_user (the worker hangs here instead); after the answer -> text.
const model = mockModel(
  prompt => {
    const toolResults = prompt.filter(m => m.role === 'tool').length;
    if (toolResults === 0) return { toolCall: { id: 'call-lookup', name: 'lookup', args: { q: 'x' } } };
    if (toolResults === 1) return { toolCall: { id: 'call-ask', name: 'ask_user', args: { question: 'Which one?' } } };
    return { text: 'Thanks.' };
  },
  role === 'worker'
    ? async (_signal, prompt) => {
        if (prompt.some(m => m.role === 'tool')) {
          console.log('  [worker] step-2 model call is hanging; waiting to be killed');
          process.send?.('hanging');
          await new Promise(() => {});
        }
      }
    : undefined,
);

if (role === 'main') {
  rmSync(TMP, { recursive: true, force: true });
  mkdirSync(TMP, { recursive: true });
}
/** In-process PubSub with a hook on publish (used only in this process, not the worker). */
class HookedPubSub extends EventEmitterPubSub {
  onPublish?: (topic: string, event: any) => void;
  override async publish(topic: string, event: any) {
    this.onPublish?.(topic, event);
    return super.publish(topic, event);
  }
}
const pubsub = new HookedPubSub();
const storage = new LibSQLStore({ id: 'store', url: DB_URL });
const memory = new Memory({ storage });
const mastra = new Mastra({
  agents: { agent: durableAgent({ model, memory }) as any },
  storage,
  pubsub,
  recovery: { durableAgents: 'auto' },
  logger: false,
});
const agent: any = mastra.getAgent('agent');

if (role === 'worker') {
  const a: any = await agent.sendMessage('hello', { resourceId, threadId: 'thread-recovered' }).accepted;
  console.log(`  [worker] sendMessage -> ${a.action} ${a.runId}`);
  await new Promise(() => {});
}

// ---------------------------------------------------------------------------------------------
console.log(`@mastra/core ${corePkg.version}, SLOW_SNAPSHOT_MS = ${SLOW_MS}`);

// Optional latency on workflow-snapshot writes, applied the same way to both cases below.
if (SLOW_MS > 0) {
  const wf: any = await (storage as any).getStore('workflows');
  for (const m of ['persistWorkflowSnapshot', 'updateWorkflowState', 'updateWorkflowResults']) {
    const orig = wf[m]?.bind(wf);
    if (orig) wf[m] = async (...args: any[]) => (await sleep(SLOW_MS), orig(...args));
  }
}

// Hook PubSub.publish: when a suspension chunk goes out, check listSuspendedRuns() right then.
const checks: Promise<string>[] = [];
pubsub.onPublish = (topic, event) => {
  if (checks.length > 0 || !JSON.stringify(event ?? '').includes('"tool-call-suspended"')) return;
  const t = Date.now();
  const found = async () =>
    ((await agent.listSuspendedRuns({ resourceId })).runs ?? []).some((r: any) => topic.includes(r.runId));
  checks.push(
    (async () => {
      const atPublish = await found();
      while (!(await found()) && Date.now() - t < 10_000) await sleep(5);
      return `run in listSuspendedRuns() when the suspension chunk was published: ${atPublish ? 'yes' : 'NO'}; listed after ${Date.now() - t} ms`;
    })(),
  );
};

async function suspensionCheck() {
  for (let i = 0; i < 400 && checks.length === 0; i++) await sleep(25);
  const r = checks.length ? await checks[0] : 'no suspension chunk published';
  checks.length = 0;
  return r;
}

// Control: start() path.
{
  const a: any = await agent.sendMessage('hello', { resourceId, threadId: 'thread-control' }).accepted;
  console.log(`\nControl: fresh run ${a.runId} suspends via start()`);
  console.log(`  ${await suspensionCheck()}`);
}

// Recovery path.
{
  console.log(`\nRecovery: a run that suspends while being recovered`);
  const child = spawn(process.execPath, ['--import', 'tsx', import.meta.filename, 'worker'], {
    stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
  });
  await new Promise<void>(resolve => child.on('message', m => m === 'hanging' && resolve()));
  child.kill('SIGKILL');
  await new Promise(r => child.on('exit', r));
  console.log('  [worker] SIGKILLed during step 2');
  const t = Date.now();
  let result: any;
  // The dead worker's run lease has to lapse before recovery can take the run.
  while (Date.now() - t < 60_000) {
    result = await mastra.recoverAllDurableAgents();
    if (result.recovered > 0) break;
    await sleep(1_000);
  }
  console.log(`  recoverAllDurableAgents() -> ${JSON.stringify(result)} after ${((Date.now() - t) / 1000).toFixed(1)} s`);
  console.log(`  ${await suspensionCheck()}`);
}

await mastra.shutdown();
process.exit(0);
