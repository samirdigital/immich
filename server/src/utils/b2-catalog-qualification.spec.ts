import {
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
  UploadPartCommand,
} from '@aws-sdk/client-s3';
import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { catalogQualificationMain } from 'src/bin/b2-catalog-qualify.js';
import {
  CatalogQualificationGuard,
  catalogQualificationPlan,
  parseCatalogQualificationArgs,
  runCatalogQualification,
} from 'src/utils/b2-catalog-qualification.js';

const config = () => ({
  bucket: 'synthetic-bucket',
  endpoint: 'https://s3.us-west-004.backblazeb2.com',
  region: 'us-west-004',
  prefix: `immich-b2-qualification/${randomUUID()}/`,
  workspace: '/nonexistent/catalog-probe',
  pgSocket: '/private/postgres',
  pgPort: 55_441,
  disposableDatabaseOptIn: true,
});

it('produces a pure dry plan without touching dependencies or input', async () => {
  const dependencies = new Proxy(
    {},
    {
      get: () => {
        throw new Error('DependencyTouched');
      },
    },
  );
  expect(await runCatalogQualification(config(), dependencies)).toMatchObject({ status: 'dry-plan' });
  expect(catalogQualificationPlan(config())).toMatchObject({
    live: false,
    sampleSize: 17 * 1024 ** 2,
    maxRequests: 24,
  });
});

it('requires explicit disposable local PG settings and rejects credential arguments', () => {
  expect(() => catalogQualificationPlan({ ...config(), pgSocket: 'localhost' })).toThrow();
  expect(() => catalogQualificationPlan({ ...config(), disposableDatabaseOptIn: false })).toThrow();
  expect(() => catalogQualificationPlan({ ...config(), endpoint: 'http://localhost' })).toThrow();
  expect(() => parseCatalogQualificationArgs(['--secretAccessKey', 'never-accepted'])).toThrow();
});

it('rejects duplicate PG flags even when the first supplied value is invalid', () => {
  const c = config();
  const args = [
    '--bucket',
    c.bucket,
    '--endpoint',
    c.endpoint,
    '--region',
    c.region,
    '--prefix',
    c.prefix,
    '--workspace',
    c.workspace,
    '--pg-socket',
    c.pgSocket,
    '--pg-port',
    '0',
    '--pg-port',
    String(c.pgPort),
    '--disposable-local-pg-opt-in',
  ];
  expect(() => parseCatalogQualificationArgs(args)).toThrow('InvalidArguments');
});

it('CLI without synthetic opt-in never reads even a hostile stdin', async () => {
  const c = config();
  const input = new Readable({
    read() {
      throw new Error('SecretRead');
    },
  });
  const report = vi.fn();
  const args = [
    '--bucket',
    c.bucket,
    '--endpoint',
    c.endpoint,
    '--region',
    c.region,
    '--prefix',
    c.prefix,
    '--workspace',
    c.workspace,
    '--pg-socket',
    c.pgSocket,
    '--pg-port',
    String(c.pgPort),
    '--disposable-local-pg-opt-in',
  ];
  expect(await catalogQualificationMain(args, input, report)).toBe(0);
  expect(input.readableDidRead).toBe(false);
  expect(report).toHaveBeenCalledWith(expect.objectContaining({ status: 'dry-plan' }));
  await expect(catalogQualificationMain([...args, '--synthetic-test-opt-in'], input, report)).rejects.toThrow(
    'PrivateStdinPipeRequired',
  );
  expect(input.readableDidRead).toBe(false);
  input.destroy();
});

it('owns GET errors throughout the ACK fsync barrier and disposes the response on failure', async () => {
  const c = config();
  const client = new S3Client({ region: c.region });
  const body = new Readable({ read() {} });
  const transport = vi
    .fn()
    .mockResolvedValueOnce({ UploadId: 'upload' })
    .mockResolvedValueOnce({ ETag: 'part1' })
    .mockResolvedValueOnce({ ETag: 'part2' })
    .mockResolvedValueOnce({ ETag: 'part3' })
    .mockResolvedValueOnce({ ETag: 'complete', VersionId: 'version' })
    .mockResolvedValueOnce({ Body: body, VersionId: 'version' });
  vi.spyOn(client, 'send').mockImplementation(transport);
  let ownedDuringBarrier = false;
  const fsyncFailure = new Error('FsyncFailed');
  const guard = new CatalogQualificationGuard(
    c,
    client,
    async (event) => {
      await Promise.resolve();
      if (!(event.type === 'remote-ack' && event.command === 'GetObjectCommand')) {
        return;
      }

      ownedDuringBarrier = body.listenerCount('error') > 0;
      throw fsyncFailure;
    },
    new AbortController().signal,
  );
  const key = `${c.prefix}${randomUUID()}/${randomUUID()}`;
  const scope = { Bucket: c.bucket, Key: key };
  await guard.enroll(key);
  await guard.client.send(new CreateMultipartUploadCommand(scope));
  for (let number = 1; number <= 3; number++) {
    const size = (number < 3 ? 8 : 1) * 1024 ** 2;
    await guard.client.send(
      new UploadPartCommand({
        ...scope,
        UploadId: 'upload',
        PartNumber: number,
        ContentLength: size,
        Body: Buffer.alloc(size),
      }),
    );
  }
  await guard.client.send(
    new CompleteMultipartUploadCommand({
      ...scope,
      UploadId: 'upload',
      MultipartUpload: { Parts: [1, 2, 3].map((number) => ({ PartNumber: number, ETag: `part${number}` })) },
    }),
  );
  await expect(guard.client.send(new GetObjectCommand({ ...scope, VersionId: 'version' }))).rejects.toBe(fsyncFailure);
  expect(ownedDuringBarrier).toBe(true);
  expect(body.destroyed).toBe(true);
  await expect(guard.client.send(new DeleteObjectCommand({ ...scope, VersionId: 'version' }))).rejects.toThrow(
    'Frozen',
  );
  expect(transport).toHaveBeenCalledTimes(6);
  await guard.settle();
  client.destroy();
});

it('snapshots the validated approved scope instead of following caller mutations', async () => {
  const c = config();
  const client = new S3Client({ region: c.region });
  const send = vi.spyOn(client, 'send').mockImplementation(vi.fn().mockResolvedValue({ UploadId: 'known-upload' }));
  const guard = new CatalogQualificationGuard(c, client, async () => {}, new AbortController().signal);
  const bucket = c.bucket;
  const key = `${c.prefix}${randomUUID()}/${randomUUID()}`;
  c.bucket = 'unapproved-bucket';
  await guard.enroll(key);
  await expect(
    guard.client.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: key })),
  ).resolves.toMatchObject({ UploadId: 'known-upload' });
  expect(send).toHaveBeenCalledTimes(1);
  client.destroy();
});

it('denies out-of-prefix keys, listings, bare-key deletion and unowned upload IDs before transport', async () => {
  const c = config();
  const client = new S3Client({ region: c.region });
  const send = vi.spyOn(client, 'send');
  const guard = new CatalogQualificationGuard(c, client, async () => {}, new AbortController().signal);
  await expect(guard.enroll(`catalog/${randomUUID()}/${randomUUID()}`)).rejects.toThrow();
  const key = `${c.prefix}${randomUUID()}/${randomUUID()}`;
  await guard.enroll(key);
  await expect(guard.client.send(new ListObjectsV2Command({ Bucket: c.bucket }))).rejects.toThrow();
  await expect(guard.client.send(new DeleteObjectCommand({ Bucket: c.bucket, Key: key }))).rejects.toThrow();
  await expect(
    guard.client.send(new CreateMultipartUploadCommand({ Bucket: c.bucket, Key: `${key}-other` })),
  ).rejects.toThrow();
  await expect(
    guard.client.send(new UploadPartCommand({ Bucket: c.bucket, Key: key, UploadId: 'unowned', PartNumber: 1 })),
  ).rejects.toThrow();
  expect(send).not.toHaveBeenCalled();
  client.destroy();
});

it('freezes every subsequent call after an uncertain part result, without retrying', async () => {
  const c = config();
  const client = new S3Client({
    region: c.region,
    credentials: { accessKeyId: 'synthetic', secretAccessKey: 'synthetic' },
  });
  const send = vi
    .spyOn(client, 'send')
    .mockImplementation(
      vi.fn().mockResolvedValueOnce({ UploadId: 'known-upload' }).mockRejectedValue(new Error('TransportUncertain')),
    );
  const guard = new CatalogQualificationGuard(c, client, async () => {}, new AbortController().signal);
  const key = `${c.prefix}${randomUUID()}/${randomUUID()}`;
  await guard.enroll(key);
  await guard.client.send(new CreateMultipartUploadCommand({ Bucket: c.bucket, Key: key }));
  const part = new UploadPartCommand({
    Bucket: c.bucket,
    Key: key,
    UploadId: 'known-upload',
    PartNumber: 1,
    ContentLength: 8 * 1024 ** 2,
    Body: Buffer.alloc(8 * 1024 ** 2),
  });
  await expect(guard.client.send(part)).rejects.toThrow();
  await expect(guard.client.send(part)).rejects.toThrow('Frozen');
  expect(send).toHaveBeenCalledTimes(2);
  client.destroy();
});

it('retains the observed Create handle before a failed ACK while freezing every later command', async () => {
  const c = config();
  const client = new S3Client({ region: c.region });
  const send = vi.spyOn(client, 'send').mockImplementation(vi.fn().mockResolvedValue({ UploadId: 'received-upload' }));
  const guard = new CatalogQualificationGuard(
    c,
    client,
    async (event) => {
      await Promise.resolve();
      if (event.type === 'remote-ack') throw new Error('AckFailed');
    },
    new AbortController().signal,
  );
  const key = `${c.prefix}${randomUUID()}/${randomUUID()}`;
  const scope = { Bucket: c.bucket, Key: key };
  await guard.enroll(key);
  await expect(guard.client.send(new CreateMultipartUploadCommand(scope))).rejects.toThrow('AckFailed');
  expect(guard.receivedObservation).toEqual({ key, receivedCreateUploadId: 'received-upload', persisted: false });
  await expect(guard.client.send(new CreateMultipartUploadCommand(scope))).rejects.toThrow('Frozen');
  await expect(guard.allowDeletion()).rejects.toThrow();
  await expect(guard.client.send(new DeleteObjectCommand({ ...scope, VersionId: 'received-version' }))).rejects.toThrow(
    'Frozen',
  );
  expect(send).toHaveBeenCalledTimes(1);
  client.destroy();
});

it.each([false, true])(
  'retains Complete observations without authorizing GET or cleanup when secondary failure is %s',
  async (secondaryFailure) => {
    const c = config();
    const client = new S3Client({ region: c.region });
    const send = vi
      .spyOn(client, 'send')
      .mockImplementation(
        vi
          .fn()
          .mockResolvedValueOnce({ UploadId: 'received-upload' })
          .mockResolvedValueOnce({ ETag: 'part1' })
          .mockResolvedValueOnce({ ETag: 'part2' })
          .mockResolvedValueOnce({ ETag: 'part3' })
          .mockResolvedValueOnce({ VersionId: 'received-version', ETag: 'complete', secret: 'not-evidence' }),
      );
    const snapshots: unknown[] = [];
    const guard = new CatalogQualificationGuard(
      c,
      client,
      async (event) => {
        await Promise.resolve();
        if (event.type === 'remote-ack' && event.command === 'CompleteMultipartUploadCommand')
          throw new Error('AckFailed');
      },
      new AbortController().signal,
      async (observation) => {
        await Promise.resolve();
        snapshots.push(observation);
        if (secondaryFailure && observation.receivedCompleteVersionId) throw new Error('ObservationFailed');
      },
    );
    const key = `${c.prefix}${randomUUID()}/${randomUUID()}`;
    const scope = { Bucket: c.bucket, Key: key };
    await guard.enroll(key);
    await guard.client.send(new CreateMultipartUploadCommand(scope));
    for (let number = 1; number <= 3; number++) {
      const size = (number < 3 ? 8 : 1) * 1024 ** 2;
      await guard.client.send(
        new UploadPartCommand({
          ...scope,
          UploadId: 'received-upload',
          PartNumber: number,
          ContentLength: size,
          Body: Buffer.alloc(size),
        }),
      );
    }
    await expect(
      guard.client.send(
        new CompleteMultipartUploadCommand({
          ...scope,
          UploadId: 'received-upload',
          MultipartUpload: { Parts: [1, 2, 3].map((number) => ({ PartNumber: number, ETag: `part${number}` })) },
        }),
      ),
    ).rejects.toThrow(secondaryFailure ? 'ObservationFailed' : 'AckFailed');
    const expected = { key, receivedCreateUploadId: 'received-upload', receivedCompleteVersionId: 'received-version' };
    expect(snapshots.at(-1)).toEqual(expected);
    expect(guard.receivedObservation).toEqual({ ...expected, persisted: !secondaryFailure });
    await expect(guard.client.send(new GetObjectCommand({ ...scope, VersionId: 'received-version' }))).rejects.toThrow(
      'Frozen',
    );
    await expect(
      guard.client.send(new DeleteObjectCommand({ ...scope, VersionId: 'received-version' })),
    ).rejects.toThrow('Frozen');
    expect(send).toHaveBeenCalledTimes(5);
    client.destroy();
  },
);
