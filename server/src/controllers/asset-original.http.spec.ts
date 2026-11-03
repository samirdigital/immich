import { createHash } from 'node:crypto';
import { getEventListeners, once } from 'node:events';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { get as httpGet } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import request from 'supertest';
import { AssetMediaController } from 'src/controllers/asset-media.controller.js';
import { AssetOriginalRepository } from 'src/repositories/asset-original.repository.js';
import { ObjectStore } from 'src/repositories/object-store.repository.js';
import { AssetMediaService } from 'src/services/asset-media.service.js';
import { OriginalReader } from 'src/services/original-reader.js';
import { AuthFactory } from 'test/factories/auth.factory.js';
import { authStub } from 'test/fixtures/auth.stub.js';
import { ControllerContext, ServiceMocks, controllerSetup, newTestService } from 'test/utils.js';

// Component boundary: real Nest/Express/controller/service/access branching/reader/sender.
// Authentication and access/database results are controlled fixtures, NOT full production auth proof.
let ctx: ControllerContext;
let mocks: ServiceMocks;
let directory: string;
let source: string;
const id = '00000000-0000-4000-8000-000000000001';
const get = vi.fn<ObjectStore['get']>();
const resolve = vi.fn<AssetOriginalRepository['resolve']>();
let readTimeoutMs = 5000;
const bytes = Buffer.from('abcdef');
const mapping = {
  provider: 's3' as const,
  storeId: 'fixture',
  key: 'catalog/fixture',
  versionId: 'v1',
  verification: 'verified' as const,
  size: '6',
  hash: createHash('sha256').update(bytes).digest('hex'),
  publishedAt: new Date('2025-01-01T00:00:00Z'),
};
beforeAll(async () => {
  directory = await mkdtemp(path.join(os.tmpdir(), 'original-http-'));
  source = path.join(directory, 'original.jpg');
  await writeFile(source, bytes);
  const fixture = newTestService(AssetMediaService);
  mocks = fixture.mocks;
  Object.assign(fixture.sut, {
    originalReader: new OriginalReader({ resolve } as unknown as AssetOriginalRepository, {
      get readTimeoutMs() {
        return readTimeoutMs;
      },
      get: (storeId: string) => (storeId === 'fixture' ? ({ get } as unknown as ObjectStore) : undefined),
    }),
  });
  ctx = await controllerSetup(AssetMediaController, [{ provide: AssetMediaService, useValue: fixture.sut }]);
  await new Promise<void>((done) => ctx.getHttpServer().listen(0, '127.0.0.1', done));
});
afterAll(async () => {
  await ctx?.close();
  await rm(directory, { recursive: true, force: true });
});
beforeEach(() => {
  readTimeoutMs = 5000;
  vi.clearAllMocks();
  ctx.reset();
  ctx.authenticate.mockResolvedValue(authStub.admin);
  get.mockReset();
  resolve.mockReset();
  mocks.access.asset.checkOwnerAccess.mockResolvedValue(new Set([id]));
  mocks.access.asset.checkAlbumAccess.mockResolvedValue(new Set());
  mocks.access.asset.checkPartnerAccess.mockResolvedValue(new Set());
  mocks.access.asset.checkSharedLinkAccess.mockResolvedValue(new Set());
  mocks.asset.getForOriginal.mockResolvedValue({
    id,
    originalPath: source,
    originalFileName: 'name.jpg',
    editedPath: undefined,
  });
  resolve.mockResolvedValue({ authority: 'object', mapping });
  get.mockImplementation((object, options) => {
    const range = options?.range;
    const body = range ? bytes.subarray(range.start, range.end + 1) : bytes;
    return Promise.resolve({ object, stream: Readable.from([body]), size: body.length, totalSize: bytes.length });
  });
});
it('serves the exact cloud original through the existing route', async () => {
  const result = await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(200);
  expect(result.body).toEqual(bytes);
  expect(result.headers['content-disposition']).toBe("inline; filename*=UTF-8''name.jpg");
  expect(get).toHaveBeenCalledWith(
    { key: mapping.key, versionId: mapping.versionId },
    expect.objectContaining({ signal: expect.any(AbortSignal), expectedSize: 6 }),
  );
});
it('retains local default and legacy local unsatisfiable-range flattening', async () => {
  resolve.mockResolvedValue({ authority: 'local' });
  const response = await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(200);
  expect(response.body).toEqual(bytes);
  await request(ctx.getHttpServer()).get(`/assets/${id}/original`).set('Range', 'bytes=100-').expect(404);
  expect(get).not.toHaveBeenCalled();
});
it.each([
  ['bytes=1-3', 206, 'bcd', 'bytes 1-3/6'],
  ['bytes=2-', 206, 'cdef', 'bytes 2-5/6'],
  ['bytes=-2', 206, 'ef', 'bytes 4-5/6'],
  ['bytes=0-1,2-3', 206, 'abcd', 'bytes 0-3/6'],
  ['bytes=0-0,3-3', 200, 'abcdef', undefined],
  ['garbage', 200, 'abcdef', undefined],
  ['bytes=9-', 416, '', 'bytes */6'],
])('range %s yields %s without forwarding raw client ranges', async (range, status, content, contentRange) => {
  const result = await request(ctx.getHttpServer()).get(`/assets/${id}/original`).set('Range', range).expect(status);
  expect(result.body).toEqual(Buffer.from(content));
  expect(result.headers['content-range']).toBe(contentRange);
  expect(get).toHaveBeenCalledTimes(status === 416 ? 0 : 1);
});
it.each([
  ['If-None-Match', '"sha256-' + mapping.hash + '"', 304],
  ['If-Modified-Since', 'Wed, 01 Jan 2025 00:00:00 GMT', 304],
  ['If-Match', '"different"', 412],
  ['If-Unmodified-Since', 'Tue, 31 Dec 2024 00:00:00 GMT', 412],
])('metadata-only %s precondition returns %s without GET', async (name, value, status) => {
  await request(ctx.getHttpServer())
    .get(`/assets/${id}/original`)
    .set(name, value)
    .set('Range', 'bytes=99-')
    .expect(status);
  expect(get).not.toHaveBeenCalled();
});
it('HEAD resolves authorized stable metadata but never opens GET', async () => {
  const result = await request(ctx.getHttpServer())
    .head(`/assets/${id}/original`)
    .set('Range', 'bytes=1-3')
    .expect(206);
  expect(result.headers['content-length']).toBe('3');
  expect(result.headers['last-modified']).toBe('Wed, 01 Jan 2025 00:00:00 GMT');
  expect(result.headers.etag).toBe('"sha256-' + mapping.hash + '"');
  expect(get).not.toHaveBeenCalled();
});
it('If-None-Match overrides date freshness and stale If-Range ignores even unsatisfiable range', async () => {
  await request(ctx.getHttpServer())
    .get(`/assets/${id}/original`)
    .set('If-None-Match', '"different"')
    .set('If-Modified-Since', 'Wed, 01 Jan 2031 00:00:00 GMT')
    .set('Range', 'bytes=99-')
    .set('If-Range', '"different"')
    .expect(200);
});
it.each(['album', 'partner'])('retains valid %s download access rather than requiring owner equality', async (kind) => {
  mocks.access.asset.checkOwnerAccess.mockResolvedValue(new Set());
  if (kind === 'album') {
    mocks.access.asset.checkAlbumAccess.mockResolvedValue(new Set([id]));
  } else {
    mocks.access.asset.checkPartnerAccess.mockResolvedValue(new Set([id]));
  }
  await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(200);
  expect(get).toHaveBeenCalledTimes(1);
});
it.each([true, false])(
  'shared link allowDownload=%s respects membership and forces edited selection first',
  async (allowDownload) => {
    ctx.authenticate.mockResolvedValue(AuthFactory.from().sharedLink({ allowDownload }).build());
    mocks.access.asset.checkSharedLinkAccess.mockResolvedValue(new Set([id]));
    await request(ctx.getHttpServer())
      .get(`/assets/${id}/original?edited=false`)
      .expect(allowDownload ? 200 : 404);
    if (allowDownload) {
      expect(mocks.asset.getForOriginal).toHaveBeenCalledWith(id, true);
    } else {
      expect(resolve).not.toHaveBeenCalled();
      expect(get).not.toHaveBeenCalled();
    }
  },
);
it('shared link missing membership denies even when download allowed', async () => {
  ctx.authenticate.mockResolvedValue(AuthFactory.from().sharedLink({ allowDownload: true }).build());
  await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(404);
  expect(resolve).not.toHaveBeenCalled();
  expect(get).not.toHaveBeenCalled();
});
it('edited representation stays local before any original mapping lookup', async () => {
  mocks.asset.getForOriginal.mockResolvedValue({
    id,
    originalPath: '/must-not-read.jpg',
    originalFileName: 'name.jpg',
    editedPath: source,
  });
  const response = await request(ctx.getHttpServer()).get(`/assets/${id}/original?edited=true`).expect(200);
  expect(response.body).toEqual(bytes);
  expect(resolve).not.toHaveBeenCalled();
  expect(get).not.toHaveBeenCalled();
});
it.each(['missing', 'timeout', 'identity', 'length', 'total', 'early-stream-error'])(
  'cloud %s fails closed with sanitized 503 before headers',
  async (failure) => {
    const stream =
      failure === 'early-stream-error'
        ? new Readable({
            read() {
              stream.destroy(new Error('sensitive upstream'));
            },
          })
        : Readable.from([bytes]);
    get.mockImplementation((object) => {
      if (failure === 'missing') {
        return Promise.resolve(undefined);
      }
      if (failure === 'timeout') {
        return Promise.reject(new Error('sensitive upstream'));
      }
      return Promise.resolve({
        object: failure === 'identity' ? { ...object, versionId: 'other' } : object,
        stream,
        size: failure === 'length' ? 7 : 6,
        totalSize: failure === 'total' ? 99 : 6,
      });
    });
    const response = await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(503);
    expect(response.text).not.toContain('sensitive');
    expect(response.text).not.toBe('abcdef');
    if (['missing', 'timeout'].includes(failure)) {
      stream.destroy();
    } else {
      await vi.waitFor(() => expect(stream.closed).toBe(true));
      expect(stream.listenerCount('error')).toBe(0);
    }
  },
);
it.each(['missing-mapping', 'bad-size', 'bad-hash', 'bad-store', 'database'])(
  '%s never selects retained local original as fallback',
  async (failure) => {
    if (failure === 'database') {
      resolve.mockRejectedValue(new Error('sensitive database'));
    } else {
      resolve.mockResolvedValue({
        authority: 'object',
        mapping:
          failure === 'missing-mapping'
            ? undefined
            : {
                ...mapping,
                ...(failure === 'bad-size' && { size: '-1' }),
                ...(failure === 'bad-hash' && { hash: 'opaque-etag' }),
                ...(failure === 'bad-store' && { storeId: 'not-registered' }),
              },
      });
    }
    const response = await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(503);
    expect(response.text).not.toContain('sensitive');
    expect(get).not.toHaveBeenCalled();
  },
);
it.each(['short', 'overlong'])(
  'destroys incomplete %s stream after headers without completing Content-Length or sending JSON',
  async (kind) => {
    const release = deferred<void>();
    const stream = Readable.from(
      (async function* () {
        yield Buffer.from('abc');
        await release.promise;
        yield Buffer.from(kind === 'short' ? 'd' : 'def');
        if (kind === 'overlong') {
          yield Buffer.from('g');
        }
      })(),
    );
    get.mockImplementation((object) => Promise.resolve({ object, stream, size: 6, totalSize: 6 }));
    const { client, closed } = beginClient();
    const [response] = await once(client, 'response');
    expect(response.statusCode).toBe(200);
    const received: Buffer[] = [];
    response.on('data', (chunk: Buffer) => {
      received.push(chunk);
    });
    response.on('error', () => {});
    const aborted = once(response, 'aborted');
    release.resolve();
    await aborted;
    await closed;
    expect(response.complete).toBe(false);
    expect(Buffer.concat(received).length).toBeLessThan(6);
    expect(Buffer.concat(received).toString()).not.toContain('message');
    await vi.waitFor(() => expect(stream.listenerCount('error')).toBe(0));
    expect(stream.closed).toBe(true);
  },
);
const deferred = <T>() => Promise.withResolvers<T>();
const beginClient = () => {
  const server = ctx.getHttpServer();
  const closed = deferred<void>();
  server.once('request', (_req: unknown, res: import('node:http').ServerResponse) =>
    res.once('close', () => closed.resolve()),
  );
  const client = httpGet(`http://127.0.0.1:${server.address().port}/assets/${id}/original`);
  client.on('error', () => {});
  return { client, closed: closed.promise };
};
it('bounds an awaited provider GET with a real abort deadline and sanitized 503', async () => {
  readTimeoutMs = 30;
  get.mockImplementation(async (_object, options) => {
    await new Promise<void>((_done, reject) =>
      options!.signal!.addEventListener('abort', () => reject(new Error('sensitive timeout')), { once: true }),
    );
    return;
  });
  const result = await request(ctx.getHttpServer()).get(`/assets/${id}/original`).timeout(2000).expect(503);
  expect(result.text).not.toContain('sensitive');
  expect(get.mock.calls[0][1]?.signal?.aborted).toBe(true);
});
it('disconnect while resolving metadata prevents GET entirely', async () => {
  const entered = deferred<void>();
  const release = deferred<void>();
  resolve.mockImplementation(async () => {
    entered.resolve();
    await release.promise;
    return { authority: 'object', mapping };
  });
  const { client, closed } = beginClient();
  await entered.promise;
  client.destroy();
  await closed;
  release.resolve();
  await new Promise((done) => setImmediate(done));
  expect(get).not.toHaveBeenCalled();
});
it('disconnect during awaited GET aborts upstream and owns and closes a late body', async () => {
  const entered = deferred<AbortSignal>();
  const release = deferred<void>();
  const stream = new Readable({ read() {} });
  get.mockImplementation(async (object, options) => {
    entered.resolve(options!.signal!);
    await release.promise;
    return { object, stream, size: 6, totalSize: 6 };
  });
  const { client, closed } = beginClient();
  const signal = await entered.promise;
  client.destroy();
  await closed;
  expect(signal.aborted).toBe(true);
  release.resolve();
  await vi.waitFor(() => expect(stream.closed).toBe(true));
  expect(stream.listenerCount('error')).toBe(0);
});
it('disconnect during streaming aborts upstream and closes source without listener leaks', async () => {
  const entered = deferred<AbortSignal>();
  const stream = new Readable({
    read() {
      stream.push(Buffer.alloc(16_384));
    },
  });
  resolve.mockResolvedValue({ authority: 'object', mapping: { ...mapping, size: '1000000000' } });
  get.mockImplementation((object, options) => {
    entered.resolve(options!.signal!);
    return Promise.resolve({ object, stream, size: 1_000_000_000, totalSize: 1_000_000_000 });
  });
  const { client, closed } = beginClient();
  const signal = await entered.promise;
  const [response] = await once(client, 'response');
  response.on('error', () => {});
  client.destroy();
  await closed;
  await vi.waitFor(() => expect(stream.closed).toBe(true));
  expect(signal.aborted).toBe(true);
  await vi.waitFor(() => expect(stream.listenerCount('error')).toBe(0));
});
it.each([
  ['If-Range', `"sha256-${mapping.hash}"`, 206],
  ['If-Range', 'Wed, 01 Jan 2025 00:00:00 GMT', 206],
  ['If-Range', `W/"sha256-${mapping.hash}"`, 200],
  ['If-Range', 'Tue, 31 Dec 2024 00:00:00 GMT', 200],
])('freshness %s %s gives %s with normalized range', async (header, value, status) => {
  const response = await request(ctx.getHttpServer())
    .get(`/assets/${id}/original`)
    .set('Range', 'bytes=1-3')
    .set(header, value)
    .expect(status);
  expect(response.body).toEqual(status === 206 ? Buffer.from('bcd') : bytes);
  expect(get.mock.calls[0][1]?.range).toEqual(status === 206 ? { start: 1, end: 3 } : undefined);
});
it('weak/list If-None-Match takes precedence over an older date, and If-Match suppresses date precondition', async () => {
  await request(ctx.getHttpServer())
    .get(`/assets/${id}/original`)
    .set('If-None-Match', `"other", W/"sha256-${mapping.hash}"`)
    .set('If-Modified-Since', 'Tue, 31 Dec 2024 00:00:00 GMT')
    .expect(304);
  expect(get).not.toHaveBeenCalled();
  await request(ctx.getHttpServer())
    .get(`/assets/${id}/original`)
    .set('If-Match', `"sha256-${mapping.hash}"`)
    .set('If-Unmodified-Since', 'Tue, 31 Dec 2024 00:00:00 GMT')
    .expect(200);
});
it.each([
  ['Range', 'bytes=99-', 416],
  ['If-None-Match', '*', 304],
  ['If-Match', '"other"', 412],
])('HEAD %s returns %s without GET', async (header, value, status) => {
  await request(ctx.getHttpServer()).head(`/assets/${id}/original`).set(header, value).expect(status);
  expect(get).not.toHaveBeenCalled();
});
it('local original preserves filename MIME cache, range, HEAD and conditional handling', async () => {
  resolve.mockResolvedValue({ authority: 'local' });
  const full = await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(200);
  expect(full.headers['content-type']).toBe('image/jpeg');
  expect(full.headers['cache-control']).toBe(
    'private, max-age=86400, no-transform, stale-while-revalidate=2592000, stale-if-error=2592000',
  );
  expect(full.headers['content-disposition']).toBe("inline; filename*=UTF-8''name.jpg");
  const partial = await request(ctx.getHttpServer())
    .get(`/assets/${id}/original`)
    .set('Range', 'bytes=1-3')
    .expect(206);
  expect(partial.body).toEqual(Buffer.from('bcd'));
  await request(ctx.getHttpServer()).head(`/assets/${id}/original`).expect(200).expect('Content-Length', '6');
  await request(ctx.getHttpServer()).get(`/assets/${id}/original`).set('If-None-Match', full.headers.etag).expect(304);
  expect(get).not.toHaveBeenCalled();
});
it('normal cloud completion cleans stream and AbortSignal listeners', async () => {
  const stream = Readable.from([bytes]);
  get.mockImplementation((object) => Promise.resolve({ object, stream, size: 6, totalSize: 6 }));
  await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(200);
  const signal = get.mock.calls[0][1]!.signal!;
  await vi.waitFor(() => expect(stream.listenerCount('error')).toBe(0));
  expect(stream.closed).toBe(true);
  expect(getEventListeners(signal, 'abort')).toHaveLength(0);
});
it('does not replace a selected but invalid empty edited path with cloud original bytes', async () => {
  ctx.authenticate.mockResolvedValue(AuthFactory.from().sharedLink({ allowDownload: true }).build());
  mocks.access.asset.checkSharedLinkAccess.mockResolvedValue(new Set([id]));
  mocks.asset.getForOriginal.mockResolvedValue({
    id,
    originalPath: source,
    originalFileName: 'name.jpg',
    editedPath: '',
  });
  await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(404);
  expect(resolve).not.toHaveBeenCalled();
  expect(get).not.toHaveBeenCalled();
});
it('does not start GET when the client disconnected while authentication was still pending', async () => {
  const entered = deferred<void>();
  const release = deferred<void>();
  ctx.authenticate.mockImplementation(async () => {
    entered.resolve();
    await release.promise;
    return authStub.admin;
  });
  get.mockRejectedValue(new Error('must not dispatch provider after disconnect'));
  const { client, closed } = beginClient();
  await entered.promise;
  client.destroy();
  await closed;
  release.resolve();
  await new Promise((done) => setImmediate(done));
  expect(get).not.toHaveBeenCalled();
});
it('denies access before selecting or resolving cloud mapping (legacy flattened 404)', async () => {
  mocks.access.asset.checkOwnerAccess.mockResolvedValue(new Set());
  await request(ctx.getHttpServer()).get(`/assets/${id}/original`).expect(404);
  expect(mocks.asset.getForOriginal).not.toHaveBeenCalled();
  expect(resolve).not.toHaveBeenCalled();
  expect(get).not.toHaveBeenCalled();
});
