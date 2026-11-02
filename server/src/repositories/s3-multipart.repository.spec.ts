import { S3Client } from '@aws-sdk/client-s3';
import { createHash } from 'node:crypto';
import { getEventListeners } from 'node:events';
import { mkdtemp, open, rename, rm, truncate, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { buffer } from 'node:stream/consumers';
import { vitest } from 'vitest';
import { createObjectValidator } from 'src/repositories/object-store.repository.js';
import { type S3MultipartJournalEvent, S3ObjectStoreRepository } from 'src/repositories/s3-object-store.repository.js';

vitest.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return { ...actual, open: vitest.fn(actual.open) };
});

vitest.mock('src/repositories/object-store.repository.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('src/repositories/object-store.repository.js')>();
  return { ...actual, createObjectValidator: vitest.fn(actual.createObjectValidator) };
});

const response = (body = '', headers = {}, statusCode = 200) => ({
  response: { statusCode, headers, body: Readable.from([body]) },
});
const success = (request: { method: string; query: Record<string, string> }) => {
  if (request.query.uploads !== undefined) {
    return response('<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>');
  }
  if (request.method === 'PUT') {
    return response('', { etag: `"opaque-${request.query.partNumber}"` });
  }
  if (request.method === 'DELETE') {
    return response('', {}, 204);
  }
  return response('<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>', {
    'x-amz-version-id': 'exact-version',
  });
};

const partSize = 8 * 1024 ** 2;
const multipart = { threshold: partSize, partSize, maxObjectSize: partSize * 10, cleanupTimeoutMs: 1000 };

describe('S3 multipart owned file uploads', () => {
  let root: string;
  let client: S3Client;
  const handle = vitest.fn();
  const journal = vitest.fn<(event: S3MultipartJournalEvent) => Promise<void>>(async () => {});
  const object = { key: 'originals/multipart.bin' };
  const stage = async (size = partSize + 7) => {
    const bytes = Buffer.alloc(size, 19);
    const filename = path.join(root, 'source');
    await writeFile(filename, bytes);
    return {
      filename,
      options: {
        multipartAttemptToken: 'attempt-1',
        size,
        expectedHash: { algorithm: 'sha256' as const, hex: createHash('sha256').update(bytes).digest('hex') },
        multipartJournal: journal,
      },
    };
  };
  const store = () => new S3ObjectStoreRepository(client, { bucket: 'synthetic', multipart });
  beforeEach(async () => {
    root = await mkdtemp(path.join(tmpdir(), 'immich-multipart-'));
    handle.mockReset();
    vitest.mocked(createObjectValidator).mockClear();
    journal.mockReset();
    journal.mockResolvedValue(undefined);
    client = new S3Client({
      region: 'us-east-1',
      credentials: { accessKeyId: 'synthetic', secretAccessKey: 'synthetic' },
      maxAttempts: 1,
      requestChecksumCalculation: 'WHEN_REQUIRED',
      responseChecksumValidation: 'WHEN_REQUIRED',
      requestHandler: { handle },
    });
  });
  afterEach(async () => {
    client.destroy();
    await rm(root, { recursive: true, force: true });
  });

  it.each([partSize + 7, partSize * 2])(
    'uploads %s bytes sequentially with exact opaque ordered ETags',
    async (size) => {
      const { filename, options } = await stage(size);
      const lengths: number[] = [];
      let manifest = '';
      handle.mockImplementation(async (request) => {
        if (request.method === 'POST' && request.query.uploads !== undefined) {
          expect(journal.mock.calls.at(-1)?.[0]).toMatchObject({
            phase: 'beforeCreate',
            bucket: 'synthetic',
            key: object.key,
          });
          return {
            response: {
              statusCode: 200,
              headers: {},
              body: Readable.from([
                '<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>',
              ]),
            },
          };
        }
        if (request.method === 'PUT') {
          expect(
            journal.mock.calls.some(([event]) => event.phase === 'uploadCreated' && event.uploadId === 'upload-1'),
          ).toBe(true);
          const bytes = await buffer(Readable.from([request.body]));
          lengths.push(bytes.length);
          return {
            response: {
              statusCode: 200,
              headers: { etag: `"opaque-${request.query.partNumber}"` },
              body: Readable.from([]),
            },
          };
        }
        manifest = request.body;
        expect(journal.mock.calls.at(-1)?.[0]).toMatchObject({ phase: 'beforeComplete', uploadId: 'upload-1' });
        return {
          response: {
            statusCode: 200,
            headers: { 'x-amz-version-id': 'exact-version' },
            body: Readable.from([
              '<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>',
            ]),
          },
        };
      });
      expect(await store().putFile(object, filename, options)).toEqual({
        object: { ...object, versionId: 'exact-version' },
        etag: '"complete"',
      });
      expect(lengths).toEqual([partSize, size - partSize]);
      expect(manifest).toContain('<PartNumber>1</PartNumber>');
      expect(manifest).toContain('&quot;opaque-1&quot;');
      expect(manifest.indexOf('opaque-1')).toBeLessThan(manifest.indexOf('opaque-2'));
      expect(journal.mock.calls.at(-1)?.[0]).toMatchObject({ phase: 'receivedResult', versionId: 'exact-version' });
      expect(handle).toHaveBeenCalledTimes(4);
    },
  );

  it.each([
    { ...multipart, partSize: 5 * 1024 ** 2 - 1 },
    { ...multipart, partSize: 64 * 1024 ** 2 + 1 },
    { ...multipart, threshold: partSize - 1 },
    { ...multipart, maxObjectSize: Number.MAX_SAFE_INTEGER },
    { ...multipart, cleanupTimeoutMs: 0 },
    { ...multipart, partSize: NaN },
    { threshold: partSize, partSize, maxObjectSize: partSize * 10 } as typeof multipart,
  ])('rejects invalid bounds before file access or Create: %j', async (config) => {
    await expect(
      new S3ObjectStoreRepository(client, { bucket: 'synthetic', multipart: config }).putFile(object, '/missing', {
        size: partSize + 1,
        expectedHash: { algorithm: 'sha256', hex: '0'.repeat(64) },
        multipartJournal: journal,
      }),
    ).rejects.toBeInstanceOf(RangeError);
    expect(handle).not.toHaveBeenCalled();
  });

  it.each([partSize * 10 + 1, partSize * 10_000 + 1])('rejects size or part count %s before Create', async (size) => {
    await expect(
      store().putFile(object, '/missing', {
        size,
        expectedHash: { algorithm: 'sha256', hex: '0'.repeat(64) },
        multipartJournal: journal,
      }),
    ).rejects.toBeInstanceOf(RangeError);
    expect(handle).not.toHaveBeenCalled();
  });

  it.each(['beforeCreate', 'uploadCreated', 'beforeComplete', 'receivedResult'] as const)(
    'stops on journal failure at %s and records uncertainty',
    async (phase) => {
      const { filename, options } = await stage();
      const primary = new Error(`journal ${phase}`);
      handle.mockImplementation(success);
      journal.mockImplementation((event) => (event.phase === phase ? Promise.reject(primary) : Promise.resolve()));
      await expect(store().putFile(object, filename, options)).rejects.toBe(primary);
      const methods = handle.mock.calls.map(([request]) => request.method);
      expect(methods).toEqual(
        phase === 'beforeCreate'
          ? []
          : phase === 'uploadCreated'
            ? ['POST', 'DELETE']
            : phase === 'beforeComplete'
              ? ['POST', 'PUT', 'PUT', 'DELETE']
              : ['POST', 'PUT', 'PUT', 'POST'],
      );
      expect(journal.mock.calls.at(-1)?.[0]).toMatchObject({
        phase: 'failure',
        at: phase,
        completeDispatched: phase === 'receivedResult',
        reason: primary,
      });
    },
  );

  it('requires an exact attempt token before opening the source', async () => {
    const { filename, options } = await stage();
    handle.mockImplementation(success);
    await expect(store().putFile(object, filename, { ...options, multipartAttemptToken: undefined })).rejects.toThrow(
      'token',
    );
    expect(handle).not.toHaveBeenCalled();
  });

  it('pins intent and bounds against mutation across awaited hooks', async () => {
    const { filename, options } = await stage();
    const requested = { ...object };
    const config = { ...multipart };
    const repository = new S3ObjectStoreRepository(client, { bucket: 'synthetic', multipart: config });
    journal.mockImplementation((event) => {
      if (event.phase === 'beforeCreate') {
        requested.key = 'other/key';
        options.size = 1;
        options.expectedHash.hex = '0'.repeat(64);
        config.partSize = 1;
      }
      return Promise.resolve();
    });
    handle.mockImplementation(success);
    expect(await repository.putFile(requested, filename, options)).toMatchObject({
      object: { key: object.key, versionId: 'exact-version' },
    });
    expect(handle.mock.calls.every(([request]) => request.path.endsWith(object.key))).toBe(true);
    expect(handle.mock.calls.filter(([request]) => request.method === 'PUT')).toHaveLength(2);
  });

  it('rejects a retry-enabled client before opening the source', async () => {
    const { filename, options } = await stage();
    const retrying = new S3Client({
      region: 'us-east-1',
      credentials: { accessKeyId: 'synthetic', secretAccessKey: 'synthetic' },
      maxAttempts: 2,
      requestChecksumCalculation: 'WHEN_REQUIRED',
      requestHandler: { handle },
    });
    handle.mockImplementation(success);
    try {
      await expect(
        new S3ObjectStoreRepository(retrying, { bucket: 'synthetic', multipart }).putFile(object, filename, options),
      ).rejects.toThrow('single attempt');
      expect(handle).not.toHaveBeenCalled();
    } finally {
      retrying.destroy();
    }
  });

  it.each([
    'success',
    'embeddedError',
    'resetComplete',
    'missingCompleteETag',
    'missingVersion',
    'nullVersion',
    'missingUploadId',
    'missingETag',
    'cleanupTimeout',
    'sourceError',
    'inFlightSourceError',
    'hash',
    'shortSource',
    'longSource',
    'cancelCreate',
    'cancelPart',
    'cancelComplete',
    'cancelCreateFalse',
    'cancelCreateZero',
    'cancelCreateEmpty',
    'cancelCreateNull',
    'cancelCreateUndefined',
    'cancelPartFalse',
    'cancelPartZero',
    'cancelPartEmpty',
    'cancelPartNull',
    'cancelPartUndefined',
    'cancelCompleteFalse',
    'cancelCompleteZero',
    'cancelCompleteEmpty',
    'cancelCompleteNull',
    'cancelCompleteUndefined',
    'create503',
    'part503',
    'complete503',
  ] as const)('settles %s with the native SDK HTTP handler and owned descriptor', async (mode) => {
    const { filename, options } = await stage();
    const file = await open(filename, 'r');
    const create = file.createReadStream.bind(file);
    let source: ReturnType<typeof create> | undefined;
    vitest.spyOn(file, 'createReadStream').mockImplementation((options) => {
      source = create(options);
      return source;
    });
    vitest.mocked(open).mockResolvedValueOnce(file);
    const diskError = new Error('synthetic disk fault');
    const { promise: readFault, reject: rejectRead } = Promise.withResolvers<never>();
    readFault.catch(() => {});
    if (mode === 'inFlightSourceError') {
      const read = file.read.bind(file);
      let reads = 0;
      vitest
        .spyOn(file, 'read')
        .mockImplementation((...args) => (++reads > partSize / (64 * 1024) ? readFault : read(...args)));
    } else if (mode === 'sourceError') {
      vitest.spyOn(file, 'read').mockRejectedValue(diskError);
    }
    const controller = new AbortController();
    const cancel = mode.endsWith('False')
      ? false
      : mode.endsWith('Zero')
        ? 0
        : mode.endsWith('Empty')
          ? ''
          : mode.endsWith('Null')
            ? null
            : mode.endsWith('Undefined')
              ? undefined
              : new Error('caller cancelled');
    if (mode === 'hash') {
      options.expectedHash.hex = '0'.repeat(64);
    } else if (mode === 'shortSource' || mode === 'longSource') {
      journal.mockImplementation(async (event) => {
        if (event.phase === 'beforeCreate') {
          await truncate(filename, options.size + (mode === 'shortSource' ? -1 : 1));
        }
      });
    }
    const requests: string[] = [];
    const parts: number[] = [];
    let completeBody = '';
    const server = createServer((request, reply) => {
      request.on('error', () => {});
      const query = new URL(request.url!, 'http://localhost').searchParams;
      const phase = query.has('uploads')
        ? 'create'
        : request.method === 'DELETE'
          ? 'abort'
          : request.method === 'PUT'
            ? 'part'
            : 'complete';
      requests.push(phase);
      if (
        (mode.startsWith('cancelCreate') && phase === 'create') ||
        (mode.startsWith('cancelPart') && phase === 'part') ||
        (mode.startsWith('cancelComplete') && phase === 'complete')
      ) {
        controller.abort(cancel);
        return;
      }
      if (mode === 'inFlightSourceError' && phase === 'part') {
        request.once('data', () => rejectRead(diskError));
        return;
      }
      let length = 0;
      request.on('data', (chunk) => {
        length += chunk.length;
        if (phase === 'complete') {
          completeBody += chunk.toString();
        }
      });
      request.on('end', () => {
        if (mode === `${phase}503`) {
          reply.writeHead(503, { 'content-type': 'application/xml' });
          reply.end('<Error><Code>SlowDown</Code><Message>synthetic failure</Message></Error>');
          return;
        }
        switch (phase) {
          case 'create': {
            reply.end(
              mode === 'missingUploadId'
                ? '<InitiateMultipartUploadResult/>'
                : '<InitiateMultipartUploadResult><UploadId>native-upload</UploadId></InitiateMultipartUploadResult>',
            );

            break;
          }
          case 'part': {
            parts.push(length);
            if (mode !== 'missingETag' && mode !== 'cleanupTimeout') {
              reply.setHeader('etag', `"native-${query.get('partNumber')}"`);
            }
            reply.end();

            break;
          }
          case 'abort': {
            if (mode === 'cleanupTimeout') {
              return;
            }
            expect(query.get('uploadId')).toBe('native-upload');
            reply.writeHead(204).end();

            break;
          }
          default: {
            if (mode === 'resetComplete') {
              request.socket.destroy();
            } else if (mode === 'embeddedError') {
              reply.writeHead(200, { 'content-type': 'application/xml' });
              reply.end('<Error><Code>InternalError</Code><Message>embedded failure</Message></Error>');
            } else {
              if (mode !== 'missingVersion') {
                reply.setHeader('x-amz-version-id', mode === 'nullVersion' ? 'null' : 'native-version');
              }
              reply.end(
                mode === 'missingCompleteETag'
                  ? '<CompleteMultipartUploadResult/>'
                  : '<CompleteMultipartUploadResult><ETag>"native-complete"</ETag></CompleteMultipartUploadResult>',
              );
            }
          }
        }
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') {
      throw new Error('Expected loopback address');
    }
    const native = new S3Client({
      endpoint: `http://127.0.0.1:${address.port}`,
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: { accessKeyId: 'synthetic', secretAccessKey: 'synthetic' },
      maxAttempts: 1,
      requestChecksumCalculation: 'WHEN_REQUIRED',
      responseChecksumValidation: 'WHEN_REQUIRED',
    });
    try {
      const write = new S3ObjectStoreRepository(native, {
        bucket: 'synthetic',
        multipart: { ...multipart, cleanupTimeoutMs: mode === 'cleanupTimeout' ? 20 : 1000 },
      }).putFile(object, filename, { ...options, signal: controller.signal });
      if (mode === 'success') {
        expect(await write).toEqual({ object: { ...object, versionId: 'native-version' }, etag: '"native-complete"' });
        expect(parts).toEqual([partSize, 7]);
        expect(completeBody).toContain('&quot;native-1&quot;');
        expect(requests).toEqual(['create', 'part', 'part', 'complete']);
      } else {
        if (mode.startsWith('cancel')) {
          await expect(write).rejects.toSatisfy((error: unknown) => Object.is(error, controller.signal.reason));
        } else
          switch (mode) {
            case 'sourceError':
            case 'inFlightSourceError': {
              await expect(write).rejects.toBe(diskError);

              break;
            }
            case 'embeddedError': {
              await expect(write).rejects.toMatchObject({ name: 'InternalError' });

              break;
            }
            case 'cleanupTimeout': {
              await expect(write).rejects.toThrow('ETag');
              expect(journal.mock.calls.some(([event]) => event.phase === 'abortResult' && !event.outcome.ok)).toBe(
                true,
              );

              break;
            }
            default: {
              await expect(write).rejects.toBeDefined();
            }
          }
        const dispatched =
          mode.startsWith('cancelComplete') ||
          [
            'embeddedError',
            'resetComplete',
            'complete503',
            'missingCompleteETag',
            'missingVersion',
            'nullVersion',
            'cancelComplete',
          ].includes(mode);
        expect(requests.filter((phase) => phase === 'complete')).toHaveLength(dispatched ? 1 : 0);
        expect(requests.filter((phase) => phase === 'abort')).toHaveLength(
          dispatched || mode === 'missingUploadId' || mode === 'create503' || mode.startsWith('cancelCreate') ? 0 : 1,
        );
        expect(requests.filter((phase) => phase === 'create')).toHaveLength(1);
        if (mode === 'part503') {
          expect(requests.filter((phase) => phase === 'part')).toHaveLength(1);
        }
      }
      expect(file.fd).toBe(-1);
      expect(source?.closed).toBe(true);
      expect(source?.eventNames()).toEqual([]);
      const validator = vitest.mocked(createObjectValidator).mock.results.at(-1)!.value;
      expect(validator.closed).toBe(true);
      expect(validator.eventNames()).toEqual([]);
      expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0);
      await new Promise<void>((resolve) => setImmediate(resolve));
    } finally {
      native.destroy();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      vitest.restoreAllMocks();
      await file.close();
    }
  });

  it.each(['CreateMultipartUploadCommand', 'UploadPartCommand', 'CompleteMultipartUploadCommand'])(
    'preserves falsey rejection identity at %s',
    async (commandName) => {
      for (const reason of [false, 0, '', null, undefined]) {
        const { filename, options } = await stage();
        handle.mockClear();
        journal.mockClear();
        handle.mockImplementation(success);
        const injectable = client as unknown as { send: (command: unknown, options?: unknown) => Promise<unknown> };
        const spy = vitest.spyOn(injectable, 'send');
        if (commandName !== 'CreateMultipartUploadCommand') {
          spy.mockResolvedValueOnce({ UploadId: 'upload-1' });
        }
        if (commandName === 'CompleteMultipartUploadCommand') {
          spy.mockResolvedValueOnce({ ETag: '"one"' }).mockResolvedValueOnce({ ETag: '"two"' });
        }
        spy.mockRejectedValueOnce(reason);
        try {
          await expect(store().putFile(object, filename, options)).rejects.toBe(reason);
          expect(journal.mock.calls.at(-1)?.[0]).toMatchObject({
            phase: 'failure',
            reason,
            completeDispatched: commandName === 'CompleteMultipartUploadCommand',
          });
          expect(handle.mock.calls.filter(([request]) => request.method === 'DELETE')).toHaveLength(
            commandName === 'UploadPartCommand' ? 1 : 0,
          );
        } finally {
          spy.mockRestore();
        }
      }
    },
  );

  it.each(['beforeCreate', 'uploadCreated', 'beforeComplete', 'receivedResult'] as const)(
    'preserves cancellation at journal %s and awaits separate cleanup',
    async (phase) => {
      for (const reason of [false, 0, '', null, undefined]) {
        const { filename, options } = await stage();
        const controller = new AbortController();
        handle.mockImplementation(success);
        handle.mockClear();
        journal.mockClear();
        journal.mockImplementation((event) => {
          if (event.phase === phase) {
            controller.abort(reason);
          }
          return Promise.resolve();
        });
        await expect(store().putFile(object, filename, { ...options, signal: controller.signal })).rejects.toSatisfy(
          (error: unknown) => Object.is(error, controller.signal.reason),
        );
        expect(handle.mock.calls.filter(([request]) => request.method === 'DELETE')).toHaveLength(
          phase === 'uploadCreated' || phase === 'beforeComplete' ? 1 : 0,
        );
        expect(journal.mock.calls.at(-1)?.[0]).toMatchObject({ phase: 'failure', reason: controller.signal.reason });
      }
    },
  );

  it('keeps cleanup and cleanup journal failures secondary to the first error', async () => {
    const { filename, options } = await stage();
    const primary = new Error('failed part');
    const cleanup = new Error('failed cleanup');
    const bookkeeping = new Error('failed abort bookkeeping');
    handle.mockImplementation((request) => {
      if (request.method === 'PUT') {
        throw primary;
      }
      if (request.method === 'DELETE') {
        throw cleanup;
      }
      return success(request);
    });
    journal.mockImplementation((event) =>
      event.phase === 'abortResult' ? Promise.reject(bookkeeping) : Promise.resolve(),
    );
    await expect(store().putFile(object, filename, options)).rejects.toBe(primary);
    expect(journal.mock.calls.at(-1)?.[0]).toMatchObject({ phase: 'failure', secondary: [cleanup, bookkeeping] });
    expect(handle.mock.calls.filter(([request]) => request.method === 'DELETE')).toHaveLength(1);
  });

  it('bounds part allocations and source read-ahead while a part request is blocked, without reopening the path', async () => {
    const { filename, options } = await stage(partSize * 3);
    const file = await open(filename, 'r');
    const create = file.createReadStream.bind(file);
    let source: ReturnType<typeof create> | undefined;
    vitest.spyOn(file, 'createReadStream').mockImplementation((options) => {
      source = create(options);
      return source;
    });
    vitest.mocked(open).mockClear();
    vitest.mocked(open).mockResolvedValueOnce(file);
    journal.mockImplementation(async (event) => {
      if (event.phase !== 'beforeCreate') {
        return;
      }

      await rename(filename, `${filename}.original`);
      await writeFile(filename, Buffer.alloc(options.size, 99));
    });
    const allocate = vitest.spyOn(Buffer, 'allocUnsafe');
    const { promise: blocked, resolve: arrived } = Promise.withResolvers<void>();
    const { promise: released, resolve: release } = Promise.withResolvers<void>();
    handle.mockImplementation(async (request) => {
      if (request.method === 'PUT' && request.query.partNumber === '1') {
        arrived();
        await released;
      }
      if (request.method === 'PUT') {
        expect(request.body.every((byte: number) => byte === 19)).toBe(true);
      }
      return success(request);
    });
    const write = store().putFile(object, filename, options);
    try {
      await blocked;
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(source!.bytesRead).toBeLessThanOrEqual(partSize + 256 * 1024);
      expect(source!.readableLength).toBeLessThanOrEqual(64 * 1024);
      expect(allocate.mock.calls.filter(([size]) => size === partSize)).toHaveLength(1);
      expect(handle.mock.calls.filter(([request]) => request.method === 'PUT')).toHaveLength(1);
      release();
      await write;
      expect(allocate.mock.calls.filter(([size]) => size === partSize)).toHaveLength(3);
      expect(open).toHaveBeenCalledTimes(1);
      expect(file.fd).toBe(-1);
      expect(source!.eventNames()).toEqual([]);
    } finally {
      release();
      await write.catch(() => {});
      allocate.mockRestore();
      vitest.restoreAllMocks();
      await file.close();
    }
  });

  it.each([0, 7, partSize - 1])(
    'keeps %s byte files on the existing single PUT path without a journal',
    async (size) => {
      const { filename, options } = await stage(size);
      handle.mockImplementation(async (request) => {
        expect(request.query.uploads).toBeUndefined();
        const bytes = await buffer(request.body);
        expect(bytes.length).toBe(size);
        return response('', { 'x-amz-version-id': 'single-version' });
      });
      expect(
        await store().putFile(object, filename, {
          ...options,
          multipartJournal: undefined,
          multipartAttemptToken: undefined,
        }),
      ).toMatchObject({ object: { versionId: 'single-version' } });
      expect(journal).not.toHaveBeenCalled();
      expect(handle).toHaveBeenCalledTimes(1);
    },
  );

  it.each(['beforeCreate', 'uploadCreated', 'beforeComplete', 'receivedResult'] as const)(
    'preserves falsey journal errors at %s',
    async (phase) => {
      for (const reason of [false, 0, '', null, undefined]) {
        const { filename, options } = await stage();
        handle.mockImplementation(success);
        journal.mockImplementation((event) => (event.phase === phase ? Promise.reject(reason) : Promise.resolve()));
        await expect(store().putFile(object, filename, options)).rejects.toBe(reason);
      }
    },
  );

  it('awaits every durability gate before the next provider effect', async () => {
    const { filename, options } = await stage();
    const beforeCreate = Promise.withResolvers<void>();
    const created = Promise.withResolvers<void>();
    const beforeComplete = Promise.withResolvers<void>();
    const arrivedCreate = Promise.withResolvers<void>();
    const arrivedCreated = Promise.withResolvers<void>();
    const arrivedComplete = Promise.withResolvers<void>();
    journal.mockImplementation(async (event) => {
      if (event.phase === 'beforeCreate') {
        arrivedCreate.resolve();
        await beforeCreate.promise;
      } else if (event.phase === 'uploadCreated') {
        arrivedCreated.resolve();
        await created.promise;
      }
      if (event.phase !== 'beforeComplete') {
        return;
      }

      arrivedComplete.resolve();
      await beforeComplete.promise;
    });
    handle.mockImplementation(success);
    const write = store().putFile(object, filename, options);
    try {
      await arrivedCreate.promise;
      expect(handle).not.toHaveBeenCalled();
      beforeCreate.resolve();
      await arrivedCreated.promise;
      expect(handle.mock.calls.map(([request]) => request.method)).toEqual(['POST']);
      created.resolve();
      await arrivedComplete.promise;
      expect(handle.mock.calls.map(([request]) => request.method)).toEqual(['POST', 'PUT', 'PUT']);
      beforeComplete.resolve();
      await write;
    } finally {
      beforeCreate.resolve();
      created.resolve();
      beforeComplete.resolve();
      await write.catch(() => {});
    }
  });

  it('checks the 10000-part boundary independently of the configured object limit', async () => {
    const repository = new S3ObjectStoreRepository(client, {
      bucket: 'synthetic',
      multipart: { ...multipart, maxObjectSize: 5 * 1024 ** 4 },
    });
    const options = {
      size: partSize * 10_000 + 1,
      expectedHash: { algorithm: 'sha256' as const, hex: '0'.repeat(64) },
      multipartAttemptToken: 'attempt-1',
      multipartJournal: journal,
    };
    await expect(repository.putFile(object, '/missing', options)).rejects.toBeInstanceOf(RangeError);
    await expect(repository.putFile(object, '/missing', { ...options, size: partSize * 10_000 })).rejects.toMatchObject(
      { code: 'ENOENT' },
    );
    expect(handle).not.toHaveBeenCalled();
  });

  it('closes the opened descriptor if validator construction fails before Create', async () => {
    const { filename, options } = await stage();
    const file = await open(filename, 'r');
    vitest.mocked(open).mockResolvedValueOnce(file);
    const primary = new Error('validator initialization failed');
    vitest.mocked(createObjectValidator).mockImplementationOnce(() => {
      throw primary;
    });
    try {
      await expect(store().putFile(object, filename, options)).rejects.toBe(primary);
      expect(file.fd).toBe(-1);
      expect(handle).not.toHaveBeenCalled();
    } finally {
      await file.close();
    }
  });

  it('selects multipart at the exact threshold without an empty trailing part', async () => {
    const { filename, options } = await stage(partSize);
    handle.mockImplementation(success);
    await store().putFile(object, filename, options);
    expect(handle.mock.calls.map(([request]) => request.method)).toEqual(['POST', 'PUT', 'POST']);
    expect(journal.mock.calls.find(([event]) => event.phase === 'beforeComplete')?.[0]).toMatchObject({
      parts: [{ PartNumber: 1, ETag: '"opaque-1"' }],
    });
  });

  it('requires a durable journal before creating a multipart upload', async () => {
    const { filename, options } = await stage();
    await expect(store().putFile(object, filename, { ...options, multipartJournal: undefined })).rejects.toThrow(
      'journal',
    );
    expect(handle).not.toHaveBeenCalled();
  });
});
