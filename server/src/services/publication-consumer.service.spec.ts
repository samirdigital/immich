import { Reflector } from '@nestjs/core';
import { vi } from 'vitest';
import { ImmichWorker, MetadataKey } from 'src/enum.js';
import { PublicationConsumerService } from 'src/services/publication-consumer.service.js';

const fakeRepository = () => ({
  discover: vi.fn().mockResolvedValue(undefined),
  takeDispatch: vi.fn().mockResolvedValue([]),
  takeRunDispatch: vi.fn().mockResolvedValue([]),
  acknowledgeCompleted: vi.fn().mockResolvedValue(undefined),
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
});

it.each([undefined, 'false'])(
  'disabled publication consumers (%s) do not query SQL or schedule timers',
  async (flag) => {
    vi.useFakeTimers();
    vi.stubEnv('IMMICH_OBJECT_UPLOAD_ENABLED', flag);
    const repository = fakeRepository();
    const worker = new PublicationConsumerService(repository as any, {} as any, {} as any, {} as any);
    await Promise.all([worker.onBootstrap(), worker.onBootstrap()]);
    await worker.onShutdown();
    expect(repository.discover).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  },
);

it('singleflights bootstrap and awaits pending tick before shutdown without leaking timers', async () => {
  vi.useFakeTimers();
  vi.stubEnv('IMMICH_OBJECT_UPLOAD_ENABLED', 'true');
  const gate = Promise.withResolvers<void>();
  const repository = fakeRepository();
  repository.discover.mockImplementation(() => gate.promise);
  const worker = new PublicationConsumerService(repository as any, {} as any, {} as any, { error: vi.fn() } as any);
  const bootstrap = Promise.all([worker.onBootstrap(), worker.onBootstrap()]);
  let stopped = false;
  const shutdown = worker.onShutdown().then(() => {
    stopped = true;
  });
  await Promise.resolve();
  expect(stopped).toBe(false);
  expect(repository.discover).toHaveBeenCalledTimes(1);
  gate.resolve();
  await Promise.all([bootstrap, shutdown]);
  expect(stopped).toBe(true);
  expect(vi.getTimerCount()).toBe(0);
});

it('catches scheduled dispatch failures and clears the next timer on shutdown', async () => {
  vi.useFakeTimers();
  vi.stubEnv('IMMICH_OBJECT_UPLOAD_ENABLED', 'true');
  const repository = fakeRepository();
  repository.discover.mockRejectedValue(new Error('SQL unavailable'));
  const logger = { error: vi.fn() };
  const worker = new PublicationConsumerService(repository as any, {} as any, {} as any, logger as any);
  await worker.onBootstrap();
  await vi.advanceTimersByTimeAsync(1000);
  expect(repository.discover).toHaveBeenCalledTimes(2);
  expect(logger.error).toHaveBeenCalledTimes(2);
  await worker.onShutdown();
  expect(vi.getTimerCount()).toBe(0);
});

it('registers lifecycle polling only for the microservices role after plugins load', () => {
  const event = new Reflector().get(MetadataKey.EventConfig, PublicationConsumerService.prototype.onBootstrap);
  expect(event).toMatchObject({ name: 'AppBootstrap', workers: [ImmichWorker.Microservices], priority: 210 });
});
