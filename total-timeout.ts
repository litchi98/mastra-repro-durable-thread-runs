/**
 * `modelSettings.timeout.totalMs` on a thread-API DurableAgent run:
 *   (1) thread subscribers get `error` but no final `thread-run-status`;
 *   (2) the timed-out run keeps the thread lease, so the next `sendMessage` is delivered into it and never answered.
 *
 * Control: a run that fails because the model call throws (e.g. an upstream 400) ends with `thread-run-status: failed`.
 *
 * In-memory libsql + in-process EventEmitterPubSub (the Mastra default). Run: npm run repro:total-timeout
 */
import { agentThreadStreamRuntime } from '@mastra/core/agent/durable';
import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import corePkg from '@mastra/core/package.json' with { type: 'json' };
import { durableAgent, hangUntilAborted, lastUserText, mockModel, observeThread, sleep, storedMessages } from './shared.ts';

const TOTAL_MS = 2_000;
const resourceId = 'user-1';
const t0 = Date.now();
const at = () => `${((Date.now() - t0) / 1000).toFixed(1)}s`;

const model = mockModel(
  prompt => {
    const text = lastUserText(prompt);
    if (text.includes('fail please')) return { error: Object.assign(new Error('Bad Request'), { statusCode: 400 }) };
    return { text: `Reply to ${text}` };
  },
  // "hang" -> the model call never answers on its own; it only ends when its abort signal fires.
  async (signal, prompt) => {
    if (lastUserText(prompt).includes('hang')) await hangUntilAborted(signal);
  },
);

const storage = new LibSQLStore({ id: 'store', url: ':memory:' });
const memory = new Memory({ storage });
const mastra = new Mastra({ agents: { agent: durableAgent({ model, memory }) as any }, storage, logger: false });
const agent: any = mastra.getAgent('agent');
const pubsub = agent.getPubSub();

const send = (text: string, threadId: string) =>
  agent.sendMessage(text, {
    resourceId,
    threadId,
    ifActive: { behavior: 'deliver' },
    ifIdle: { behavior: 'wake', streamOptions: { modelSettings: { timeout: { totalMs: TOTAL_MS } } } },
  }).accepted;

console.log(`@mastra/core ${corePkg.version}, modelSettings.timeout.totalMs = ${TOTAL_MS}`);

// --- control: the model call fails ---------------------------------------------------------
{
  const threadId = 'thread-control';
  const obs = await observeThread(agent, resourceId, threadId, t0);
  const a = await send('fail please', threadId);
  console.log(`\nControl: model call throws (400). sendMessage -> ${a.action} ${a.runId}`);
  await obs.until(['failed', 'success', 'aborted'], 10_000);
  await sleep(500);
  console.log(`  subscriber saw: ${JSON.stringify(obs.log)}`);
  obs.close();
}

// --- the run hits its total deadline -------------------------------------------------------
const threadId = 'thread-timeout';
const obs = await observeThread(agent, resourceId, threadId, t0);
const first = await send('hang', threadId);
console.log(`\n[${at()}] A. sendMessage('hang') -> ${first.action} ${first.runId}; model call hangs`);

await obs.until(['error'], 10_000);
console.log(`[${at()}] subscriber got: ${obs.log.at(-1)}`);

// (1) does a final status ever arrive?
const final = await obs.until(['failed', 'success', 'aborted'], 10_000);
console.log(`[${at()}] (1) final thread-run-status within 10 s after the error: ${final ?? 'none'}`);
console.log(`    subscriber saw: ${JSON.stringify(obs.log)}`);

// (2) who holds the thread now, and where does the next message go?
const owner = await agentThreadStreamRuntime.getThreadLeaseOwnerRunId(pubsub, { resourceId, threadId });
const active = await agent.listActiveRuns({ resourceId });
console.log(`[${at()}] (2) getThreadLeaseOwnerRunId() -> ${owner === first.runId ? 'the timed-out run' : owner ?? 'none'}; listActiveRuns() -> ${JSON.stringify(active.runs?.map((r: any) => r.runId))}`);
const second = await send('second message', threadId);
console.log(`[${at()}] B. sendMessage('second message') -> ${second.action}${second.runId === first.runId ? ' into the timed-out run' : ` ${second.runId}`}`);
const answered = await obs.until(['text-delta("Reply to second'], 10_000);
console.log(`[${at()}]    answered within 10 s: ${answered ? 'yes' : 'no'}`);

let freedAt: string | undefined;
for (let i = 0; i < 120 && !freedAt; i++) {
  const o = await agentThreadStreamRuntime.getThreadLeaseOwnerRunId(pubsub, { resourceId, threadId });
  if (o !== first.runId) freedAt = at();
  else await sleep(250);
}
console.log(`[${freedAt ?? at()}] thread lease of the timed-out run ${freedAt ? 'gone' : 'still held'}`);
await sleep(2_000);
console.log(`[${at()}] stored messages: ${JSON.stringify(await storedMessages(memory, resourceId, threadId))}`);
const third = await send('third message', threadId);
const thirdAnswered = await obs.until(['text-delta("Reply to third'], 10_000);
console.log(`[${at()}] C. sendMessage('third message') -> ${third.action} ${third.runId === first.runId ? 'into the timed-out run' : '(new run)'}; answered: ${thirdAnswered ? 'yes' : 'no'}`);
console.log(`\nsubscriber timeline:\n  ${obs.timed.join('\n  ')}`);

obs.close();
await mastra.shutdown();
process.exit(0);
