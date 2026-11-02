import { S3Client } from '@aws-sdk/client-s3';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, open, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { buffer } from 'node:stream/consumers';
import { promisify } from 'node:util';
import { expectTypeOf, vitest } from 'vitest';
import { ObjectStore } from 'src/repositories/object-store.repository.js';
import { S3ObjectStoreRepository } from 'src/repositories/s3-object-store.repository.js';

vitest.mock('node:crypto', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:crypto')>();
  return { ...actual, createHash: vitest.fn(actual.createHash) };
});

vitest.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return { ...actual, open: vitest.fn(actual.open) };
});

const expectedHash = (bytes: Buffer) => ({
  algorithm: 'sha256' as const,
  hex: createHash('sha256').update(bytes).digest('hex'),
});

describe(S3ObjectStoreRepository.name, () => {
  const object = { key: 'originals/asset.bin' };
  const versioned = { ...object, versionId: 'b2-version-1' };
  const handle = vitest.fn();
  let client: S3Client;
  let store: S3ObjectStoreRepository;

  let stagingRoot: string;
  const stage = async (bytes: Buffer = Buffer.alloc(0)) => {
    const filename = path.join(stagingRoot, 'source');
    await writeFile(filename, bytes);
    return filename;
  };

  beforeEach(async () => {
    stagingRoot = await mkdtemp(path.join(tmpdir(), 'immich-s3-source-'));
    handle.mockReset();
    client = new S3Client({
      endpoint: 'https://s3.us-west-004.backblazeb2.com',
      region: 'us-west-004',
      credentials: { accessKeyId: 'synthetic-key', secretAccessKey: 'synthetic-secret' },
      forcePathStyle: true,
      maxAttempts: 1,
      requestChecksumCalculation: 'WHEN_REQUIRED',
      responseChecksumValidation: 'WHEN_REQUIRED',
      requestHandler: { handle },
    });
    store = new S3ObjectStoreRepository(client, { bucket: 'synthetic-bucket' });
  });

  afterEach(async () => {
    vitest.restoreAllMocks();
    vitest.mocked(createHash).mockReset();
    client.destroy();
    await rm(stagingRoot, { recursive: true, force: true });
  });

  it('should stream a pinned binary GET without buffering it', async () => {
    const bytes = Buffer.from([0, 255, 128, 1]);
    const body = Readable.from([bytes]);
    handle.mockResolvedValue({
      response: {
        statusCode: 200,
        headers: { 'content-length': '4', 'x-amz-version-id': versioned.versionId, etag: '"opaque"' },
        body,
      },
    });
    const result = await store.get(versioned);
    expect(result?.stream).toBe(body);
    expect(result?.size).toBe(4);
    expect(result?.object).toEqual(versioned);
    expect(await buffer(result!.stream)).toEqual(bytes);
    expect(handle.mock.calls[0][0]).toMatchObject({ method: 'GET', query: { versionId: versioned.versionId } });
  });

  it('should serialize and validate an exact inclusive range', async () => {
    handle.mockResolvedValue({
      response: {
        statusCode: 206,
        headers: { 'content-length': '3', 'content-range': 'bytes 1-3/6', 'x-amz-version-id': versioned.versionId },
        body: Readable.from([Buffer.from('bcd')]),
      },
    });
    const result = await store.get(versioned, { range: { start: 1, end: 3 } });
    expect(result?.size).toBe(3);
    expect(await buffer(result!.stream)).toEqual(Buffer.from('bcd'));
    expect(handle.mock.calls[0][0].headers.range).toBe('bytes=1-3');
    const calls = handle.mock.calls.length;
    for (const range of [
      { start: -1, end: 2 },
      { start: 2, end: 1 },
      { start: 0.5, end: 2 },
    ]) {
      await expect(store.get(versioned, { range })).rejects.toBeInstanceOf(RangeError);
    }
    expect(handle).toHaveBeenCalledTimes(calls);
  });

  it('should reject an ignored range and destroy the unexpected body', async () => {
    const body = Readable.from([Buffer.from('abcdef')]);
    handle.mockResolvedValue({
      response: {
        statusCode: 200,
        headers: { 'content-length': '6', 'x-amz-version-id': versioned.versionId },
        body,
      },
    });
    await expect(store.get(versioned, { range: { start: 1, end: 3 } })).rejects.toBeInstanceOf(RangeError);
    expect(body.destroyed).toBe(true);
  });

  it('should delete only the pinned version idempotently', async () => {
    handle.mockResolvedValue({ response: { statusCode: 204, headers: {}, body: Readable.from([]) } });
    await store.delete(versioned);
    await store.delete(versioned);
    expect(handle).toHaveBeenCalledTimes(2);
    for (const [request] of handle.mock.calls) {
      expect(request).toMatchObject({ method: 'DELETE', query: { versionId: versioned.versionId } });
    }
  });

  it('should distinguish object misses from permission, bucket and transport failures', async () => {
    for (const code of ['NoSuchKey', 'NoSuchVersion', 'NoSuchBucket', 'AccessDenied', 'InvalidRange']) {
      const statusCode = code === 'AccessDenied' ? 403 : code === 'InvalidRange' ? 416 : 404;
      handle.mockImplementation(() =>
        Promise.resolve({
          response: {
            statusCode,
            headers: { 'content-type': 'application/xml' },
            body: Readable.from([`<Error><Code>${code}</Code><Message>synthetic</Message></Error>`]),
          },
        }),
      );
      if (code === 'NoSuchKey' || code === 'NoSuchVersion') {
        expect(await store.get(versioned)).toBeUndefined();
        await store.delete(versioned);
      } else if (code === 'InvalidRange') {
        await expect(store.get(versioned, { range: { start: 10, end: 12 } })).rejects.toBeInstanceOf(RangeError);
      } else {
        await expect(store.get(versioned)).rejects.toMatchObject({ name: code });
        await expect(store.delete(versioned)).rejects.toMatchObject({ name: code });
      }
    }
    const transport = new Error('synthetic transport failure');
    handle.mockRejectedValue(transport);
    await expect(store.get(versioned)).rejects.toBe(transport);
    await expect(store.delete(versioned)).rejects.toBe(transport);
    await expect(
      store.putFile(object, await stage(), { size: 0, expectedHash: expectedHash(Buffer.alloc(0)) }),
    ).rejects.toBe(transport);
  });

  it('should require safe keys and pinned versions before issuing requests', async () => {
    for (const key of ['', '../escape', '/absolute', 'a/../b', 'a//b', String.raw`a\b`]) {
      const invalid = { key, versionId: versioned.versionId };
      await expect(store.get(invalid)).rejects.toBeInstanceOf(TypeError);
      await expect(store.delete(invalid)).rejects.toBeInstanceOf(TypeError);
      await expect(
        store.putFile({ key }, await stage(), { size: 0, expectedHash: expectedHash(Buffer.alloc(0)) }),
      ).rejects.toBeInstanceOf(TypeError);
    }
    for (const versionId of [undefined, '', 'null']) {
      await expect(store.get({ ...object, versionId })).rejects.toBeInstanceOf(TypeError);
      await expect(store.delete({ ...object, versionId })).rejects.toBeInstanceOf(TypeError);
    }
    await expect(
      store.putFile(versioned, await stage(), { size: 0, expectedHash: expectedHash(Buffer.alloc(0)) }),
    ).rejects.toBeInstanceOf(TypeError);
    for (const size of [-1, 0.5, NaN, 5 * 1024 ** 3 + 1]) {
      await expect(
        store.putFile(object, await stage(), { size, expectedHash: expectedHash(Buffer.alloc(0)) }),
      ).rejects.toBeInstanceOf(RangeError);
    }
    expect(handle).not.toHaveBeenCalled();
  });

  it('should reject unversioned PUT success rather than claiming an immutable identity', async () => {
    handle.mockImplementation(async (request) => {
      await buffer(request.body);
      return { response: { statusCode: 200, headers: {}, body: Readable.from([]) } };
    });
    await expect(
      store.putFile(object, await stage(), { size: 0, expectedHash: expectedHash(Buffer.alloc(0)) }),
    ).rejects.toThrow('version');
  });

  it('should destroy a GET body whose version or length cannot be trusted', async () => {
    for (const headers of [
      { 'content-length': '1', 'x-amz-version-id': 'wrong-version' },
      { 'content-length': '-1', 'x-amz-version-id': versioned.versionId },
      { 'x-amz-version-id': versioned.versionId },
    ]) {
      const body = Readable.from([Buffer.from('x')]);
      handle.mockResolvedValue({ response: { statusCode: 200, headers, body } });
      await expect(store.get(versioned)).rejects.toBeInstanceOf(TypeError);
      expect(body.destroyed).toBe(true);
    }
  });

  it.each([
    'immediate',
    'in-flight',
    'early-success',
    'early-error',
    'success',
    'transport',
    'cancel',
    'cancel-false',
    'cancel-zero',
    'cancel-empty',
    'cancel-null',
    'cancel-undefined',
    'hash',
  ])('should settle %s uploads with the real HTTP handler', async (mode) => {
    const { stdout } = await promisify(execFile)(
      process.execPath,
      [
        '-e',
        `
          const { readFileSync } = require('node:fs');
          const fs = require('node:fs/promises');
          const { tmpdir } = require('node:os');
          const { createHash } = require('node:crypto');
          const { createServer } = require('node:http');
          const { Readable } = require('node:stream');
          const { Module } = require('node:module');
          const { resolve } = require('node:path');
          const { transpileModule, ModuleKind } = require('typescript');
          const { S3Client } = require('@aws-sdk/client-s3');
          const load = (name, dependencies = {}) => {
            const filename = resolve('src/repositories', name + '.ts');
            const loaded = new Module(filename, module);
            loaded.filename = filename;
            loaded.paths = module.paths;
            const originalRequire = loaded.require.bind(loaded);
            loaded.require = (id) => dependencies[id] ?? originalRequire(id);
            loaded._compile(transpileModule(readFileSync(filename, 'utf8'), {
              compilerOptions: { module: ModuleKind.CommonJS, target: 99 },
            }).outputText, filename);
            return loaded.exports;
          };
          let source;
          let body;
          const { promise: fault, reject: failSource } = Promise.withResolvers();
          fault.catch(() => {});
          const failure = new Error('disk read failed');
          const objectStore = load('object-store.repository', {
              'node:fs/promises': {
                ...fs,
                async open(...args) {
                  const file = await fs.open(...args);
                  const read = file.read.bind(file);
                  let first = true;
                  file.read = (...args) => {
                    if (!['immediate', 'in-flight'].includes(process.argv[1])) { return read(...args); }
                    if (process.argv[1] === 'immediate') { return Promise.reject(failure); }
                    if (first) { first = false; return read(...args); }
                    return fault;
                  };
                  const create = file.createReadStream.bind(file);
                  file.createReadStream = (...args) => { source = create(...args); return source; };
                  return file;
                },
              },
          });
          const { S3ObjectStoreRepository } = load('s3-object-store.repository', {
            'src/repositories/s3-multipart.js': load('s3-multipart', {
              'src/repositories/object-store.repository.js': objectStore,
            }),
            'src/repositories/object-store.repository.js': {
              ...objectStore,
              createObjectValidator(options) {
                body = objectStore.createObjectValidator(options);
                return body;
              },
            },
          });
          (async () => {
            const uncaught = [];
            const unhandled = [];
            process.on('uncaughtException', (error) => uncaught.push(error.message));
            process.on('unhandledRejection', (error) => unhandled.push(String(error)));
            const controller = new AbortController();
            const cancelMode = process.argv[1].startsWith('cancel');
            const cancellationReasons = {
              'cancel': new Error('caller cancelled'),
              'cancel-false': false,
              'cancel-zero': 0,
              'cancel-empty': '',
              'cancel-null': null,
              'cancel-undefined': undefined,
            };
            let cancellation;
            let requestCancelled = false;
            let received = false;
            let receivedSize = 0;
            let acknowledged = false;
            const receivedHash = createHash('sha256');
            const stagingRoot = await fs.mkdtemp(require('node:path').join(tmpdir(), 'immich-read-fault-'));
            const filename = require('node:path').join(stagingRoot, 'source');
            const bytes = Buffer.alloc(16 * 1024 * 1024, 'x');
            await fs.writeFile(filename, bytes);
            const { promise: requestClosed, resolve: closed } = Promise.withResolvers();
            const server = createServer((request, response) => {
              if (process.argv[1].startsWith('early-')) {
                response.writeHead(process.argv[1] === 'early-success' ? 200 : 403, { 'x-amz-version-id': 'early-version' });
                response.end();
                return;
              }
              request.on('error', (error) => {
                if (error.code !== 'ECONNRESET') { throw error; }
              });
              request.on('close', () => { requestCancelled = !request.complete; closed(); });
              request.on('data', (chunk) => {
                received = true;
                receivedSize += chunk.length;
                receivedHash.update(chunk);
                if (process.argv[1] === 'in-flight') { failSource(failure); }
                if (process.argv[1] === 'transport') { request.socket.destroy(); }
                if (cancelMode && !controller.signal.aborted) {
                  controller.abort(cancellationReasons[process.argv[1]]);
                  cancellation = controller.signal.reason;
                }
              });
              request.on('end', () => {
                if (process.argv[1] === 'hash') { return; }
                acknowledged = true;
                response.writeHead(200, { 'x-amz-version-id': 'completed-version' });
                response.end();
              });
            });
            await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
            const client = new S3Client({
              endpoint: 'http://127.0.0.1:' + server.address().port,
              region: 'us-east-1', forcePathStyle: true, maxAttempts: 1,
              requestChecksumCalculation: 'WHEN_REQUIRED',
              credentials: { accessKeyId: 'synthetic-key', secretAccessKey: 'synthetic-secret' },
            });
            let timer;
            try {
              const store = new S3ObjectStoreRepository(client, { bucket: 'synthetic-bucket' });
              const write = store.putFile({ key: 'originals/asset.bin' }, filename, {
                size: bytes.length, expectedHash: { algorithm: 'sha256', hex: process.argv[1] === 'hash' ? '0'.repeat(64) : createHash('sha256').update(bytes).digest('hex') }, signal: controller.signal,
              }).then((value) => ({ status: 'fulfilled', value }), (error) => ({
                status: 'rejected',
                sameCancellation: cancelMode && Object.is(error, cancellation),
                outcome: error === failure ? 'source failure' : cancelMode && Object.is(error, cancellation) ? 'caller cancellation' : process.argv[1].startsWith('early-') ? 'early rejection' : process.argv[1] === 'transport' ? 'transport failure' : String(error?.message ?? error),
              }));
              const outcome = await Promise.race([
                write,
                new Promise((resolve) => { timer = setTimeout(() => resolve('pending'), 1000); }),
              ]);
              clearTimeout(timer);
              if (process.argv[1] === 'in-flight' || cancelMode) {
                await Promise.race([
                  requestClosed,
                  new Promise((resolve) => { timer = setTimeout(resolve, 1000); }),
                ]);
                clearTimeout(timer);
              }
              await new Promise((resolve) => setImmediate(resolve));
              console.log(JSON.stringify({
                settlement: outcome, uncaught, unhandled, received, requestCancelled, receivedSize, acknowledged,
                receivedHash: receivedHash.digest('hex'),
                destroyed: source.destroyed, closed: source.closed,
                bodyDestroyed: body.destroyed, bodyClosed: body.closed,
                bodyListeners: body.eventNames().length,
                sourceBytesRead: source.bytesRead,
                sourcePersisted: (await fs.readFile(filename)).equals(bytes),
                errorListeners: source.listenerCount('error'),
                callerAborted: controller.signal.aborted,
              }));
              controller.abort();
              await write;
            } finally {
              clearTimeout(timer);
              source?.destroy();
              await fs.rm(stagingRoot, { recursive: true, force: true });
              client.destroy();
              server.closeAllConnections();
              await new Promise((resolve) => server.close(resolve));
            }
          })().catch((error) => { console.error(error); process.exitCode = 1; });
        `,
        mode,
      ],
      { cwd: process.cwd(), timeout: 5000 },
    );
    const result = JSON.parse(stdout.trim());
    expect(result).toMatchObject({
      settlement:
        mode === 'success'
          ? { status: 'fulfilled', value: { object: { key: object.key, versionId: 'completed-version' } } }
          : {
              status: 'rejected',
              outcome: mode.startsWith('early-')
                ? 'early rejection'
                : mode === 'success'
                  ? 'resolved'
                  : mode === 'transport'
                    ? 'transport failure'
                    : mode.startsWith('cancel')
                      ? 'caller cancellation'
                      : mode === 'hash'
                        ? 'Object hash differs from expected hash'
                        : 'source failure',
              sameCancellation: mode.startsWith('cancel'),
            },
      uncaught: [],
      unhandled: [],
      destroyed: true,
      closed: true,
      errorListeners: 0,
      bodyDestroyed: true,
      bodyClosed: true,
      bodyListeners: 0,
      sourcePersisted: true,
      callerAborted: mode.startsWith('cancel'),
    });
    if (mode === 'success') {
      const bytes = Buffer.alloc(16 * 1024 * 1024, 'x');
      expect(result.receivedSize).toBe(bytes.length);
      expect(result.receivedHash).toBe(expectedHash(bytes).hex);
    } else if (mode === 'hash') {
      expect(result.acknowledged).toBe(false);
      expect(result.sourceBytesRead).toBe(16 * 1024 * 1024);
    } else if (mode === 'in-flight' || mode.startsWith('cancel')) {
      expect(result).toMatchObject({ received: true, requestCancelled: true });
    }
  });

  it.each([
    ['transport failure', false],
    ['caller cancellation', false],
    ['transport failure', true],
    ['caller cancellation', true],
  ])('should reject unsupported streams before %s (async destroy: %s)', async (mode, asynchronous) => {
    const source = new Readable({
      emitClose: false,
      read() {},
      ...(asynchronous && {
        destroy(_error, callback) {
          setImmediate(() => callback(null));
        },
      }),
    });
    const read = vitest.spyOn(source, 'read');
    const destroy = vitest.spyOn(source, 'destroy');
    const on = vitest.spyOn(source, 'on');
    const once = vitest.spyOn(source, 'once');
    const controller = new AbortController();
    if (mode === 'caller cancellation') {
      controller.abort();
    }
    await expect(
      store.putFile(object, source as unknown as string, {
        size: 10,
        expectedHash: expectedHash(Buffer.alloc(10)),
        signal: controller.signal,
      }),
    ).rejects.toBeInstanceOf(TypeError);
    expectTypeOf<Parameters<ObjectStore['putFile']>[1]>().toEqualTypeOf<string>();
    expectTypeOf<Extract<Readable, Parameters<ObjectStore['putFile']>[1]>>().toEqualTypeOf<never>();
    expect(handle).not.toHaveBeenCalled();
    for (const method of [read, destroy, on, once]) {
      expect(method).not.toHaveBeenCalled();
    }
    expect(source.eventNames()).toEqual([]);
    expect(source.destroyed).toBe(false);
  });

  it.each([false, 0, '', null, undefined])('should reject the first transport reason %s exactly', async (reason) => {
    const filename = await stage(Buffer.alloc(1024 * 1024));
    vitest.spyOn(client, 'send').mockRejectedValueOnce(reason);
    const settlement = await store
      .putFile(object, filename, { size: 1024 * 1024, expectedHash: expectedHash(Buffer.alloc(1024 * 1024)) })
      .then((value) => ({ status: 'fulfilled', value }))
      .catch((error: unknown) => ({ status: 'rejected', reason: error }));
    expect(settlement).toEqual({ status: 'rejected', reason });
    expect('reason' in settlement && Object.is(settlement.reason, reason)).toBe(true);
  });

  it('should close upload sources on transport failure', async () => {
    const failure = new Error('synthetic upload failed');
    const source = await stage(Buffer.alloc(10));
    const file = await open(source, 'r');
    vitest.mocked(open).mockResolvedValueOnce(file);
    handle.mockRejectedValue(failure);
    await expect(
      store.putFile(object, source, { size: 10, expectedHash: expectedHash(Buffer.alloc(10)) }),
    ).rejects.toBe(failure);
    expect(file.fd).toBe(-1);
  });

  it('should forward upload cancellation and close its source', async () => {
    const controller = new AbortController();
    const source = await stage(Buffer.alloc(10));
    const file = await open(source, 'r');
    vitest.mocked(open).mockResolvedValueOnce(file);
    const { promise: started, resolve: requested } = Promise.withResolvers<void>();
    handle.mockImplementation(
      (_request, options) =>
        new Promise((_resolve, reject) => {
          requested();
          options.abortSignal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), {
            once: true,
          });
        }),
    );
    const write = store.putFile(object, source, {
      size: 10,
      expectedHash: expectedHash(Buffer.alloc(10)),
      signal: controller.signal,
    });
    await started;
    controller.abort();
    await expect(write).rejects.toMatchObject({ name: 'AbortError' });
    expect(file.fd).toBe(-1);
  });

  it('should cancel a returned download stream after headers arrive', async () => {
    const controller = new AbortController();
    const body = new Readable({ read() {} });
    handle.mockResolvedValue({
      response: {
        statusCode: 200,
        headers: { 'content-length': '10', 'x-amz-version-id': versioned.versionId },
        body,
      },
    });
    const result = await store.get(versioned, { signal: controller.signal });
    const consumed = buffer(result!.stream);
    controller.abort();
    await expect(consumed).rejects.toMatchObject({ name: 'AbortError' });
    expect(body.destroyed).toBe(true);
  });

  it('should validate staging inputs before acquiring resources or issuing requests', async () => {
    const file = await open(await stage(), 'r');
    const mockedOpen = vitest.mocked(open);
    mockedOpen.mockClear();
    try {
      for (const input of [file, () => file, undefined, '', 'relative/path']) {
        await expect(
          store.putFile(object, input as unknown as string, { size: 0, expectedHash: expectedHash(Buffer.alloc(0)) }),
        ).rejects.toBeInstanceOf(TypeError);
      }
      for (const hash of [
        undefined,
        { algorithm: 'sha1', hex: 'z'.repeat(40) },
        { algorithm: 'sha256', hex: '0'.repeat(40) },
        { algorithm: 'md5', hex: '0'.repeat(32) },
      ]) {
        await expect(
          store.putFile(object, '/missing', { size: 0, expectedHash: hash } as never),
        ).rejects.toBeInstanceOf(TypeError);
      }
      expect(mockedOpen).not.toHaveBeenCalled();
      expect(handle).not.toHaveBeenCalled();
      const stats = await file.stat();
      expect(stats.isFile()).toBe(true);
    } finally {
      await file.close();
    }
  });

  it('should reject pre-aborted valid file uploads before opening the source', async () => {
    const filename = await stage();
    const controller = new AbortController();
    const reason = new Error('cancelled');
    controller.abort(reason);
    const mockedOpen = vitest.mocked(open);
    mockedOpen.mockClear();
    await expect(
      store.putFile(object, filename, {
        size: 0,
        expectedHash: expectedHash(Buffer.alloc(0)),
        signal: controller.signal,
      }),
    ).rejects.toBe(reason);
    expect(mockedOpen).not.toHaveBeenCalled();
    expect(handle).not.toHaveBeenCalled();
  });

  it('should reject missing, symlink, nonregular and wrong-size files without HTTP', async () => {
    const bytes = Buffer.from('data');
    const filename = await stage(bytes);
    const link = path.join(stagingRoot, 'link');
    await symlink(filename, link);
    for (const [filename, error] of [
      [path.join(stagingRoot, 'missing'), 'ENOENT'],
      [link, 'ELOOP'],
    ] as const) {
      await expect(
        store.putFile(object, filename, { size: 4, expectedHash: expectedHash(bytes) }),
      ).rejects.toMatchObject({ code: error });
    }
    await expect(
      store.putFile(object, stagingRoot, { size: 0, expectedHash: expectedHash(Buffer.alloc(0)) }),
    ).rejects.toBeInstanceOf(TypeError);
    const file = await open(filename, 'r');
    vitest.mocked(open).mockResolvedValueOnce(file);
    await expect(
      store.putFile(object, filename, { size: 5, expectedHash: expectedHash(bytes) }),
    ).rejects.toBeInstanceOf(RangeError);
    expect(file.fd).toBe(-1);
    expect(handle).not.toHaveBeenCalled();
  });

  it('should close the owned source when validator construction fails', async () => {
    const bytes = Buffer.from('data');
    const filename = await stage(bytes);
    const hash = expectedHash(bytes);
    const file = await open(filename, 'r');
    vitest.mocked(open).mockResolvedValueOnce(file);
    const failure = new Error('hash initialization failed');
    vitest.mocked(createHash).mockImplementationOnce(() => {
      throw failure;
    });
    try {
      await expect(store.putFile(object, filename, { size: bytes.length, expectedHash: hash })).rejects.toBe(failure);
      expect(file.fd).toBe(-1);
      expect(handle).not.toHaveBeenCalled();
    } finally {
      await file.close();
    }
  });

  it('should reject a full-source hash mismatch, close the file, and never report a version', async () => {
    const bytes = Buffer.alloc(1024 * 1024);
    const filename = await stage(bytes);
    const file = await open(filename, 'r');
    vitest.mocked(open).mockResolvedValueOnce(file);
    handle.mockImplementation(async (request) => {
      const hash = createHash('sha256');
      for await (const chunk of request.body) {
        hash.update(chunk);
      }
      return {
        response: { statusCode: 200, headers: { 'x-amz-version-id': versioned.versionId }, body: Readable.from([]) },
      };
    });
    await expect(
      store.putFile(object, filename, {
        size: bytes.length,
        expectedHash: { algorithm: 'sha256', hex: '0'.repeat(64) },
      }),
    ).rejects.toThrow('hash');
    expect(file.fd).toBe(-1);
    expect(await readFile(filename)).toEqual(bytes);
  });

  it('should bound read-ahead under transport backpressure and close every owned stream', async () => {
    const bytes = Buffer.alloc(16 * 1024 * 1024, 'x');
    const filename = await stage(bytes);
    const file = await open(filename, 'r');
    const create = file.createReadStream.bind(file);
    let source: ReturnType<typeof create> | undefined;
    vitest.spyOn(file, 'createReadStream').mockImplementation((options) => {
      source = create(options);
      return source;
    });
    vitest.mocked(open).mockResolvedValueOnce(file);
    let body: Readable | undefined;
    let uploaded = 0;
    handle.mockImplementation(async (request) => {
      body = request.body;
      if (!body!.readableLength) {
        await new Promise<void>((resolve) => body!.once('readable', resolve));
      }
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(source!.bytesRead).toBeLessThanOrEqual(256 * 1024);
      expect(source!.readableLength).toBeLessThanOrEqual(64 * 1024);
      expect(body!.readableLength).toBeLessThanOrEqual(64 * 1024);
      for await (const chunk of body!) {
        uploaded += chunk.length;
      }
      return {
        response: { statusCode: 200, headers: { 'x-amz-version-id': versioned.versionId }, body: Readable.from([]) },
      };
    });
    await store.putFile(object, filename, { size: bytes.length, expectedHash: expectedHash(bytes) });
    expect(uploaded).toBe(bytes.length);
    expect(file.fd).toBe(-1);
    for (const stream of [source!, body!]) {
      expect(stream.closed).toBe(true);
      expect(stream.eventNames()).toEqual([]);
    }
  });

  it('should serialize a binary single PUT and return its version with an opaque etag', async () => {
    const bytes = Buffer.from([0, 255, 128, 1]);
    let uploaded: Buffer | undefined;
    handle.mockImplementation(async (request) => {
      uploaded = await buffer(request.body);
      return {
        response: {
          statusCode: 200,
          headers: { 'x-amz-version-id': versioned.versionId, etag: '"opaque-not-a-checksum"' },
          body: Readable.from([]),
        },
      };
    });
    const result = await store.putFile(object, await stage(bytes), {
      size: bytes.length,
      expectedHash: expectedHash(bytes),
    });
    expect(uploaded).toEqual(bytes);
    expect(result).toEqual({ object: versioned, etag: '"opaque-not-a-checksum"' });
    expect(handle.mock.calls[0][0]).toMatchObject({
      method: 'PUT',
      path: '/synthetic-bucket/originals/asset.bin',
      headers: { 'content-length': '4' },
    });
  });
});
