/**
 * A DurableAgent run started by `sendMessage` (wake) saves the user message to memory before the run has any
 * running checkpoint. A crash in that window leaves an accepted, stored user message that nothing answers:
 * `recoverAllDurableAgents()` finds no run (there is no `running` snapshot), and there is no public API to start a
 * turn for a message that is already stored.
 *
 *   worker (child process): sendMessage('hello') -> SIGKILLs itself right after memory.saveMessages() has stored
 *                           the user message (case A), or while the step-1 model call hangs (case B, control).
 *   this process          : `recoverAllDurableAgents()` every second for 40 s, then reads storage.
 *
 * Case B is the control: the same crash after the first running checkpoint is recovered and answered.
 *
 * Run: npm run repro:wake-crash-window
 */
import { spawn } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { createClient } from '@libsql/client';
import { EventEmitterPubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import corePkg from '@mastra/core/package.json' with { type: 'json' };
import { durableAgent, lastUserText, mockModel, sleep, storedMessages } from './shared.ts';

const TMP = join(import.meta.dirname, '.tmp', 'wake-crash');
const DB_FILE = join(TMP, 'repro.db');
const resourceId = 'user-1';
const [role = 'main', kase = ''] = process.argv.slice(2);
const RECOVERY_WINDOW_MS = Number(process.env.RECOVERY_WINDOW_MS ?? 40_000);

const prompts: string[][] = [];
const model = mockModel(
  prompt => {
    prompts.push(prompt.filter(m => m.role === 'user').map(m => lastUserText([m])));
    return { text: `Reply to: ${lastUserText(prompt)}` };
  },
  role === 'worker' && kase === 'B'
    ? async () => {
        process.send?.('hanging');
        await new Promise(() => {});
      }
    : undefined,
);

if (role === 'main') {
  rmSync(TMP, { recursive: true, force: true });
  mkdirSync(TMP, { recursive: true });
}

const storage = new LibSQLStore({ id: 'store', url: `file:${DB_FILE}` });
const memory = new Memory({ storage });

if (role === 'worker' && kase === 'A') {
  // Kill this process as soon as the user message has been written to memory.
  const orig = memory.saveMessages.bind(memory);
  (memory as any).saveMessages = async (args: any) => {
    const result = await orig(args);
    if (JSON.stringify(args.messages ?? []).includes('hello')) {
      process.send?.('saved');
      process.kill(process.pid, 'SIGKILL');
      await new Promise(() => {});
    }
    return result;
  };
}

const mastra = new Mastra({
  agents: { agent: durableAgent({ model, memory }) as any },
  storage,
  pubsub: new EventEmitterPubSub(),
  recovery: { durableAgents: 'auto' },
  logger: false,
});
const agent: any = mastra.getAgent('agent');

if (role === 'worker') {
  void agent.sendMessage('hello', { resourceId, threadId: `thread-${kase}` }).accepted.catch(() => {});
  await new Promise(() => {});
}

// ---------------------------------------------------------------------------------------------
console.log(`@mastra/core ${corePkg.version}, recovery: { durableAgents: 'auto' }, libsql file storage`);
const db = createClient({ url: `file:${DB_FILE}` });

async function rows(sql: string, args: any[] = []) {
  try {
    return (await db.execute({ sql, args })).rows.map(r => ({ ...r }));
  } catch (e) {
    return `(${(e as Error).message})`;
  }
}

async function runCase(kase: 'A' | 'B', title: string) {
  const threadId = `thread-${kase}`;
  console.log(`\nCase ${kase}: ${title}`);
  const child = spawn(process.execPath, ['--import', 'tsx', import.meta.filename, 'worker', kase], {
    stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
  });
  child.on('message', m => {
    if (m === 'hanging') child.kill('SIGKILL');
  });
  await new Promise(r => child.on('exit', r));
  console.log('  [worker] SIGKILLed');

  const snapshots = await rows(
    `SELECT workflow_name, run_id, json_extract(snapshot, '$.status') AS status FROM mastra_workflow_snapshot`,
  );
  const owners = await rows(`SELECT * FROM mastra_workflow_run_owners`);
  console.log(`  stored messages:        ${JSON.stringify(await storedMessages(memory, resourceId, threadId))}`);
  console.log(`  workflow snapshots:     ${JSON.stringify(snapshots)}`);
  console.log(`  run owner rows (db):    ${Array.isArray(owners) ? owners.length : owners}`);
  console.log(`  listActiveRuns():       ${JSON.stringify((await agent.listActiveRuns({ resourceId })).runs?.map((r: any) => r.runId) ?? [])}`);

  const t = Date.now();
  let result: any;
  let total = { recovered: 0, skipped: 0 };
  while (Date.now() - t < RECOVERY_WINDOW_MS) {
    result = await mastra.recoverAllDurableAgents();
    total.recovered += result.recovered ?? 0;
    total.skipped += result.skipped ?? 0;
    if (result.recovered > 0) break;
    await sleep(1_000);
  }
  console.log(
    `  recoverAllDurableAgents() every 1 s for ${((Date.now() - t) / 1000).toFixed(1)} s -> recovered ${total.recovered}, skipped ${total.skipped}; last ${JSON.stringify(result)}`,
  );
  await sleep(1_000);
  console.log(`  stored messages now:    ${JSON.stringify(await storedMessages(memory, resourceId, threadId))}`);
  return threadId;
}

const threadA = await runCase('A', 'SIGKILL right after the wake run saved the user message (before any running checkpoint)');

// What happens to the stored message from here: the next message wakes a new run that only answers itself.
prompts.length = 0;
const a: any = await agent.sendMessage('next message', { resourceId, threadId: threadA }).accepted;
console.log(`  sendMessage('next message') -> ${a.action}`);
for (let i = 0; i < 200 && (await storedMessages(memory, resourceId, threadA)).length < 3; i++) await sleep(50);
console.log(`  user messages in the new run's model prompt: ${JSON.stringify(prompts[0])}`);
console.log(`  stored messages:        ${JSON.stringify(await storedMessages(memory, resourceId, threadA))}`);

await runCase('B', 'control: SIGKILL while the step-1 model call hangs (after the first running checkpoint)');

await mastra.shutdown();
process.exit(0);
