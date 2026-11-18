/**
 * A run parked on a suspending tool in process A. Process B sends a message to the same thread: `sendMessage()`
 * returns `deliver` into the parked run (A holds the thread lease). Then the parked run is answered with
 * `sendStreamResume()`. Both processes have a `subscribeToThread()` open on the thread (as replicas behind a load
 * balancer would, for clients connected to either).
 *
 *   Case A (bug):     B answers. The resumed segment runs in B without the delivered message, the message is never
 *                     stored, and no later turn picks it up.
 *   Case B (control): A (the parked run's owner) answers. The delivered message is merged into the resumed segment.
 *
 * Process A is this process; process B is a child process. Both share a libsql file and a RedisStreamsPubSub (the
 * thread lease lives in the PubSub's lease provider, so the in-process PubSub can't show this across processes).
 * The model is a MockLanguageModelV2; each model call is appended to a shared log with the process that made it.
 *
 * Env: REDIS_URL (default redis://127.0.0.1:6379); SUBSCRIBE = which processes open subscribeToThread() before the
 * run starts (default 'AB'; also 'A', 'B', or '' for none); SETTLE_MS = extra wait for a later turn (default 15000);
 * ONLY_CASE_A=1 skips the control (for repeated runs).
 *
 * Run: npm run repro:cross-process-resume-delivered
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { appendFileSync, existsSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import { RedisStreamsPubSub } from '@mastra/redis-streams';
import corePkg from '@mastra/core/package.json' with { type: 'json' };
import { durableAgent, lastUserText, mockModel, sleep, storedMessages } from './shared.ts';

const [role = 'A', baseThread = `thread-${Date.now()}`] = process.argv.slice(2);
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const SUBSCRIBE = process.env.SUBSCRIBE ?? 'AB';
const SETTLE_MS = Number(process.env.SETTLE_MS ?? 15_000);
const TMP = join(import.meta.dirname, '.tmp', 'cross-process-resume');
const CALLS = join(TMP, 'model-calls.jsonl');
const resourceId = 'user-1';

/** `role: text` of every prompt message after the system prompt. */
const promptSummary = (prompt: any[]) =>
  prompt
    .filter(m => m.role !== 'system')
    .map(m => {
      const parts = typeof m.content === 'string' ? [{ type: 'text', text: m.content }] : m.content;
      const text = parts
        .map((p: any) =>
          p.type === 'text'
            ? p.text
            : p.type === 'tool-call'
              ? `tool-call ${p.toolName}`
              : p.type === 'tool-result'
                ? `tool-result ${JSON.stringify(p.output?.value ?? p.output)}`
                : p.type,
        )
        .join(' ');
      return `${m.role}: ${text}`;
    });

/** Which case a prompt belongs to (from the first user message). */
const caseOf = (prompt: any[]) => {
  const first = prompt.find(m => m.role === 'user');
  const text = typeof first?.content === 'string' ? first.content : first?.content?.map((p: any) => p.text ?? '').join('');
  return text?.match(/case-\w/)?.[0] ?? '?';
};

// 'ask ...' -> the model calls ask_user (suspends). Last message is a tool result -> 'after tool'.
// Anything else -> 'Reply to: <last user text>'.
const model = mockModel(prompt => {
  appendFileSync(CALLS, JSON.stringify({ process: role, case: caseOf(prompt), prompt: promptSummary(prompt) }) + '\n');
  if (prompt[prompt.length - 1]?.role === 'tool') return { text: 'after tool' };
  const text = lastUserText(prompt);
  if (text.startsWith('ask') && !prompt.some(m => m.role === 'tool'))
    return { toolCall: { id: `call-ask-${Date.now()}`, name: 'ask_user', args: { question: 'which one?' } } };
  return { text: `Reply to: ${text}` };
});

if (role === 'A') {
  rmSync(TMP, { recursive: true, force: true });
  mkdirSync(TMP, { recursive: true });
}
const storage = new LibSQLStore({ id: 'store', url: `file:${join(TMP, 'repro.db')}` });
const memory = new Memory({ storage });
const mastra = new Mastra({
  agents: { agent: durableAgent({ model, memory }) as any },
  storage,
  pubsub: new RedisStreamsPubSub({ url: REDIS_URL, keyPrefix: `repro-xresume-${baseThread}` }),
  logger: false,
});
const agent: any = mastra.getAgent('agent');
await (mastra as any).startWorkers?.();

/** Commands either process runs. */
async function handle(cmd: any) {
  const { threadId } = cmd;
  if (cmd.kind === 'subscribe') {
    const sub = await agent.subscribeToThread({ resourceId, threadId, withRunStatus: true, withInitialHistory: true });
    void (async () => {
      try {
        for await (const _ of sub.stream as AsyncIterable<any>);
      } catch {}
    })();
    return true;
  }
  if (cmd.kind === 'send') {
    const accepted: any = await agent.sendMessage(cmd.text, { resourceId, threadId }).accepted;
    return { action: accepted.action, runId: accepted.runId };
  }
  if (cmd.kind === 'suspended') {
    const { runs } = await agent.listSuspendedRuns({ threadId, resourceId });
    return runs.map((r: any) => ({ runId: r.runId, toolCallId: r.toolCalls?.[0]?.toolCallId }));
  }
  if (cmd.kind === 'resume') {
    return agent.sendStreamResume({
      resourceId,
      threadId,
      runId: cmd.runId,
      toolCallId: cmd.toolCallId,
      resumeData: { answer: 'skip' },
    });
  }
  throw new Error(`unknown command ${cmd.kind}`);
}

if (role === 'B') {
  process.on('message', async (raw: any) => {
    const cmd = JSON.parse(raw);
    if (cmd.kind === 'exit') {
      await mastra.shutdown();
      process.exit(0);
    }
    try {
      process.send!(JSON.stringify({ id: cmd.id, ok: await handle(cmd) }));
    } catch (e) {
      process.send!(JSON.stringify({ id: cmd.id, error: String(e) }));
    }
  });
  process.send!(JSON.stringify({ id: 'ready' }));
} else {
  const b: ChildProcess = spawn(process.execPath, ['--import', 'tsx', import.meta.filename, 'B', baseThread], {
    stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
  });
  const replies = new Map<string, (msg: any) => void>();
  b.on('message', raw => {
    const msg = JSON.parse(String(raw));
    replies.get(msg.id)?.(msg);
  });
  let seq = 0;
  const inB = (cmd: object): Promise<any> =>
    new Promise((resolve, reject) => {
      const id = String(++seq);
      replies.set(id, msg => (msg.error ? reject(new Error(msg.error)) : resolve(msg.ok)));
      b.send(JSON.stringify({ ...cmd, id }));
    });
  await new Promise(r => replies.set('ready', r));

  const modelCalls = (tag: string) =>
    existsSync(CALLS)
      ? readFileSync(CALLS, 'utf8')
          .trim()
          .split('\n')
          .map(l => JSON.parse(l))
          .filter(c => c.case === tag)
      : [];

  async function scenario(tag: 'case-A' | 'case-B', resumer: 'A' | 'B') {
    const threadId = `${baseThread}-${tag}`;
    if (SUBSCRIBE.includes('A')) await handle({ kind: 'subscribe', threadId });
    if (SUBSCRIBE.includes('B')) await inB({ kind: 'subscribe', threadId });

    const accepted: any = await agent.sendMessage(`ask (${tag})`, { resourceId, threadId }).accepted;
    console.log(`  [A] sendMessage('ask (${tag})') -> ${accepted.action}`);
    let runs: any[] = [];
    for (let i = 0; i < 100 && !runs.length; i++) {
      await sleep(100);
      runs = await handle({ kind: 'suspended', threadId });
    }
    const run = runs[0];
    console.log(`  run parked on ask_user in A: ${run ? 'yes' : 'NO'}`);
    await sleep(2_000);

    const delivered = await inB({ kind: 'send', threadId, text: `interjection (${tag})` });
    console.log(
      `  [B] sendMessage('interjection (${tag})') -> ${delivered.action}${delivered.runId === run?.runId ? ' (into the parked run)' : ''}`,
    );
    await sleep(500);

    const before = modelCalls(tag).length;
    const cmd = { kind: 'resume', threadId, runId: run.runId, toolCallId: run.toolCallId };
    const res = resumer === 'B' ? await inB(cmd) : await handle(cmd);
    console.log(`  [${resumer}] sendStreamResume({ resumeData: { answer: 'skip' } }) -> ${JSON.stringify({ accepted: res.accepted })}`);
    await sleep(4_000);
    const resumed = modelCalls(tag).slice(before);
    for (const c of resumed) console.log(`  model call after the resume [process ${c.process}]: ${JSON.stringify(c.prompt)}`);
    console.log(`  stored messages: ${JSON.stringify(await storedMessages(memory, resourceId, threadId))}`);
    if (SETTLE_MS) {
      await sleep(SETTLE_MS);
      const later = modelCalls(tag).slice(before + resumed.length);
      console.log(`  ${(4_000 + SETTLE_MS) / 1000} s after the resume: further model calls: ${later.length}`);
      console.log(`  stored messages: ${JSON.stringify(await storedMessages(memory, resourceId, threadId))}`);
    }
    console.log(`  listSuspendedRuns(): ${JSON.stringify(await handle({ kind: 'suspended', threadId }))}`);
  }

  console.log(
    `@mastra/core ${corePkg.version}, RedisStreamsPubSub, libsql file; processes A (this) and B (child); subscribeToThread() open in: ${SUBSCRIBE || 'none'}`,
  );
  console.log(`\nCase A: B delivers a message to the run parked in A, then B answers the parked run`);
  await scenario('case-A', 'B');
  if (!process.env.ONLY_CASE_A) {
    console.log(`\nCase B (control): B delivers a message to the run parked in A, then A answers the parked run`);
    await scenario('case-B', 'A');
  }

  b.send(JSON.stringify({ kind: 'exit' }));
  await new Promise(r => b.on('exit', r));
  await mastra.shutdown();
  process.exit(0);
}
