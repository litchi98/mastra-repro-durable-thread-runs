/** Shared setup: mock model, tools, a durable agent factory, and a thread observer. No network, no API keys. */
import { Agent } from '@mastra/core/agent';
import { createDurableAgent } from '@mastra/core/agent/durable';
import { createTool } from '@mastra/core/tools';
import { MockLanguageModelV2, convertArrayToReadableStream } from 'ai/test';
import { z } from 'zod';

export const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const usage = { inputTokens: 10, outputTokens: 5, totalTokens: 15 };

export type Reply = { text: string } | { toolCall: { id: string; name: string; args: object } } | { error: Error };

/**
 * Mock model. `decide` gets the prompt and returns a reply. `hold` (optional) is awaited before the
 * stream starts, so a script can keep a run "in the model call".
 */
export function mockModel(
  decide: (prompt: any[]) => Reply,
  hold?: (signal: AbortSignal | undefined, prompt: any[]) => Promise<void>,
) {
  return new MockLanguageModelV2({
    doStream: async ({ prompt, abortSignal }) => {
      if (hold) await hold(abortSignal, prompt as any[]);
      const reply = decide(prompt as any[]);
      if ('error' in reply) throw reply.error;
      const parts: any[] = [
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: `r-${Math.random()}`, modelId: 'mock', timestamp: new Date(0) },
      ];
      if ('text' in reply) {
        parts.push(
          { type: 'text-start', id: 't1' },
          { type: 'text-delta', id: 't1', delta: reply.text },
          { type: 'text-end', id: 't1' },
          { type: 'finish', finishReason: 'stop', usage },
        );
      } else {
        parts.push(
          {
            type: 'tool-call',
            toolCallId: reply.toolCall.id,
            toolName: reply.toolCall.name,
            input: JSON.stringify(reply.toolCall.args),
          },
          { type: 'finish', finishReason: 'tool-calls', usage },
        );
      }
      return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: convertArrayToReadableStream(parts) };
    },
  });
}

/** Resolves when `signal` aborts (like a fetch that only ends when it is aborted), then throws its reason. */
export async function hangUntilAborted(signal: AbortSignal | undefined) {
  await new Promise<void>(resolve => {
    if (!signal) return;
    if (signal.aborted) return resolve();
    signal.addEventListener('abort', () => resolve(), { once: true });
  });
  throw signal?.reason ?? new Error('aborted');
}

/** The text of the last user message in a prompt. */
export function lastUserText(prompt: any[]) {
  const last = [...prompt].reverse().find(m => m.role === 'user');
  const content = last?.content ?? '';
  return typeof content === 'string' ? content : content.map((p: any) => p.text ?? '').join('');
}

/** Stored messages of a thread as `role: text`. */
export async function storedMessages(memory: any, resourceId: string, threadId: string) {
  const { messages } = await memory.recall({ threadId, resourceId });
  return messages.map((m: any) => {
    const parts = m.content?.parts ?? [];
    const text = parts.map((p: any) => p.text ?? '').join('') || (typeof m.content === 'string' ? m.content : '');
    return `${m.role}: ${text}`;
  });
}

/** A tool that suspends on first call and returns the resume data afterwards. */
export const askUser = createTool({
  id: 'ask_user',
  description: 'Ask the user a question',
  inputSchema: z.object({ question: z.string() }),
  suspendSchema: z.object({ question: z.string() }),
  resumeSchema: z.object({ answer: z.string() }),
  execute: async (input, ctx) => {
    const resumeData = ctx.agent?.resumeData as { answer: string } | undefined;
    if (!resumeData) {
      await ctx.agent?.suspend({ question: input.question });
      return undefined;
    }
    return { answer: resumeData.answer };
  },
});

/** A plain tool that returns immediately (gives the run a step boundary). */
export const lookup = createTool({
  id: 'lookup',
  description: 'Look something up',
  inputSchema: z.object({ q: z.string() }),
  execute: async input => ({ result: `result for ${input.q}` }),
});

export function durableAgent(opts: { model: any; memory: any; cleanupTimeoutMs?: number }) {
  return createDurableAgent({
    agent: new Agent({
      id: 'agent',
      name: 'agent',
      instructions: 'You are a test agent.',
      model: opts.model,
      tools: { ask_user: askUser, lookup },
      memory: opts.memory,
    }),
    cleanupTimeoutMs: opts.cleanupTimeoutMs,
  });
}

const SHOWN = ['start', 'step-start', 'text-delta', 'tool-call-suspended', 'tool-call-resumed', 'finish', 'error', 'abort'];

/** Subscribe to a thread with run statuses and keep a compact, timestamped log of what arrived. */
export async function observeThread(agent: any, resourceId: string, threadId: string, t0 = Date.now()) {
  const sub = await agent.subscribeToThread({ resourceId, threadId, withRunStatus: true });
  const log: string[] = [];
  const timed: string[] = [];
  const statuses: string[] = [];
  const push = (s: string) => {
    log.push(s);
    timed.push(`${((Date.now() - t0) / 1000).toFixed(1)}s ${s}`);
  };
  void (async () => {
    try {
      for await (const c of sub.stream as AsyncIterable<any>) {
        if (c.type === 'thread-run-status') {
          statuses.push(c.payload?.status);
          push(`thread-run-status:${c.payload?.status}`);
        } else if (c.type === 'error') {
          const e = c.payload?.error ?? {};
          push(`error(${e.name ?? ''}${e.timeoutType ? ` timeoutType=${e.timeoutType}` : ''})`);
        } else if (SHOWN.includes(c.type)) {
          push(
            c.type === 'text-delta'
              ? `text-delta(${JSON.stringify(c.payload?.text)})`
              : c.type === 'tool-call-suspended'
                ? `tool-call-suspended(${c.payload?.toolCallId})`
                : c.type,
          );
        }
      }
    } catch (e) {
      push(`stream error: ${(e as Error).message}`);
    }
  })();
  return {
    log,
    timed,
    statuses,
    /** Wait until a status or a log entry in `want` arrives; returns it, or undefined on timeout. */
    async until(want: string[], ms = 20_000) {
      const hit = () => statuses.find(s => want.includes(s)) ?? log.find(l => want.some(w => l.startsWith(w)));
      const deadline = Date.now() + ms;
      while (Date.now() < deadline && !hit()) await sleep(25);
      return hit();
    },
    close: () => sub.unsubscribe?.(),
  };
}
