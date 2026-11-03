import { JobName, QueueName } from 'src/enum.js';
import { JobRepository } from 'src/repositories/job.repository.js';

it('uses removable stable queue IDs only for publication jobs without treating queue acceptance as completion', async () => {
  const queue = { add: vi.fn().mockResolvedValue({}), addBulk: vi.fn().mockResolvedValue([]) };
  const repository = new JobRepository(
    { get: () => queue } as any,
    {} as any,
    {} as any,
    { setContext: vi.fn() } as any,
  );
  Object.assign(repository, {
    handlers: { [JobName.AssetExtractMetadata]: { queueName: QueueName.MetadataExtraction } },
  });
  const publication = { effectId: 'effect', receiptId: 'receipt', assetId: 'asset', ownerId: 'owner' };
  await repository.queue({ name: JobName.AssetExtractMetadata, data: { id: 'asset', publication } });
  await repository.queue({ name: JobName.AssetExtractMetadata, data: { id: 'asset', publication } });
  expect(queue.add).toHaveBeenCalledTimes(2);
  expect(queue.add).toHaveBeenCalledWith(
    JobName.AssetExtractMetadata,
    { id: 'asset', publication },
    expect.objectContaining({ jobId: 'publication-effect', removeOnComplete: true, removeOnFail: true, attempts: 1 }),
  );
  await repository.queue({ name: JobName.AssetExtractMetadata, data: { id: 'asset' } });
  expect(queue.addBulk).toHaveBeenCalledWith([
    { name: JobName.AssetExtractMetadata, data: { id: 'asset' }, options: undefined },
  ]);
});
