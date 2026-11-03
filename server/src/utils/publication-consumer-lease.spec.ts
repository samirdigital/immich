import { withPublicationLease } from 'src/utils/publication-consumer-lease.js';

afterEach(() => {
  vi.useRealTimers();
});

it('renews singleflight and awaits renewal during cleanup without leaking timers', async () => {
  vi.useFakeTimers();
  const work = Promise.withResolvers<string>();
  const renewal = Promise.withResolvers<void>();
  const renew = vi.fn(() => renewal.promise);
  const result = withPublicationLease(() => work.promise, renew, 10);
  await vi.advanceTimersByTimeAsync(30);
  expect(renew).toHaveBeenCalledTimes(1);
  work.resolve('finished');
  let complete = false;
  const settled = result.then((value) => {
    complete = true;
    return value;
  });
  await Promise.resolve();
  expect(complete).toBe(false);
  renewal.resolve();
  expect(await settled).toBe('finished');
  expect(vi.getTimerCount()).toBe(0);
});

it('rejects successful work when renewal has lost its lease', async () => {
  vi.useFakeTimers();
  const work = Promise.withResolvers<void>();
  const result = withPublicationLease(
    () => work.promise,
    () => Promise.reject(new Error('lease expired')),
    10,
  );
  const outcome = result.then(() => 'incorrect success').catch((error: Error) => error.message);
  await vi.advanceTimersByTimeAsync(10);
  work.resolve();
  expect(await outcome).toBe('lease expired');
  expect(vi.getTimerCount()).toBe(0);
});
