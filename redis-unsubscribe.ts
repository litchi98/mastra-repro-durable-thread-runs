/**
 * `RedisStreamsPubSub.unsubscribe()` of an idle consumer-group subscription waits for the in-flight
 * `XREADGROUP ... BLOCK <blockMs>` to return, and `Mastra.stopWorkers()` unsubscribes its worker
 * subscriptions one after another, so it pays that wait several times.
 *
 * Needs a Redis: REDIS_URL (default redis://127.0.0.1:6379), e.g. docker run --rm -p 6379:6379 redis:7-alpine
 * Run: npm run repro:redis-unsubscribe
 */
import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import { RedisStreamsPubSub } from '@mastra/redis-streams';
import corePkg from '@mastra/core/package.json' with { type: 'json' };
import redisPkg from '@mastra/redis-streams/package.json' with { type: 'json' };
import { durableAgent, mockModel, sleep } from './shared.ts';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const ms = (t: number) => `${Math.round(performance.now() - t)}ms`;
console.log(`@mastra/core ${corePkg.version}, @mastra/redis-streams ${redisPkg.version}`);

// A. Five idle consumer-group subscriptions on one RedisStreamsPubSub, unsubscribed one by one.
for (const blockMs of [undefined, 100]) {
  const pubsub = new RedisStreamsPubSub({ url: REDIS_URL, keyPrefix: `repro-unsub-${Date.now()}`, blockMs });
  const subs = Array.from({ length: 5 }, (_, i) => ({ topic: `topic-${i}`, cb: () => {} }));
  for (const s of subs) await pubsub.subscribe(s.topic, s.cb, { group: 'workers' });
  await sleep(1_500); // idle: every reader is parked in XREADGROUP ... BLOCK
  const times: string[] = [];
  const t = performance.now();
  for (const s of subs) {
    const t1 = performance.now();
    await pubsub.unsubscribe(s.topic, s.cb);
    times.push(ms(t1));
  }
  console.log(`\nA. blockMs = ${blockMs ?? 'default'}: unsubscribe() x5 -> ${times.join(' / ')} (total ${ms(t)})`);
  await pubsub.close();
}

// B. Mastra.stopWorkers() on an idle instance that uses RedisStreamsPubSub.
for (const blockMs of [undefined, 100]) {
  const storage = new LibSQLStore({ id: 'store', url: ':memory:' });
  const mastra = new Mastra({
    agents: { agent: durableAgent({ model: mockModel(() => ({ text: 'unused' })), memory: new Memory({ storage }) }) as any },
    storage,
    pubsub: new RedisStreamsPubSub({ url: REDIS_URL, keyPrefix: `repro-workers-${Date.now()}`, blockMs }),
    logger: false,
  });
  await mastra.startWorkers();
  await sleep(1_500);
  const runs: string[] = [];
  for (let i = 0; i < 1; i++) {
    const t = performance.now();
    await mastra.stopWorkers();
    runs.push(ms(t));
  }
  console.log(`\nB. blockMs = ${blockMs ?? 'default'}: mastra.stopWorkers() on an idle instance -> ${runs.join(', ')}`);
  await mastra.shutdown();
}
process.exit(0);
