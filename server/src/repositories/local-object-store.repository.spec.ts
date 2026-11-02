import { execFileSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { appendFileSync, renameSync, writeFileSync } from 'node:fs';
import { mkdtemp, open, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { buffer } from 'node:stream/consumers';
import { vitest } from 'vitest';
import { LocalObjectStoreRepository } from 'src/repositories/local-object-store.repository.js';
import { ObjectWriteOptions } from 'src/repositories/object-store.repository.js';

vitest.mock('node:crypto', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:crypto')>();
  return { ...actual, randomUUID: vitest.fn(actual.randomUUID) };
});

vitest.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return { ...actual, open: vitest.fn(actual.open) };
});

const options = (bytes: Buffer, algorithm: 'sha1' | 'sha256' = 'sha256'): ObjectWriteOptions => ({
  size: bytes.length,
  expectedHash: { algorithm, hex: createHash(algorithm).update(bytes).digest('hex') },
});

describe(LocalObjectStoreRepository.name, () => {
  let root: string;
  let stagingRoot: string;
  let store: LocalObjectStoreRepository;
  const object = { key: 'originals/asset.bin' };
  const stage = async (bytes: Buffer = Buffer.alloc(0)) => {
    const filename = path.join(stagingRoot, 'source');
    await writeFile(filename, bytes);
    return filename;
  };
  const upload = async (bytes: Buffer) => store.putFile(object, await stage(bytes), options(bytes));

  beforeEach(async () => {
    root = await mkdtemp(path.join(tmpdir(), 'immich-object-store-'));
    stagingRoot = await mkdtemp(path.join(tmpdir(), 'immich-object-source-'));
    store = new LocalObjectStoreRepository(root);
  });

  afterEach(async () => {
    vitest.restoreAllMocks();
    vitest.mocked(randomUUID).mockReset();
    await rm(root, { recursive: true, force: true });
    await rm(stagingRoot, { recursive: true, force: true });
  });

  it('should reject unsupported upload inputs without touching caller streams', async () => {
    const source = new Readable({ read() {}, emitClose: false });
    const read = vitest.spyOn(source, 'read');
    const destroy = vitest.spyOn(source, 'destroy');
    const file = await open(await stage(), 'r');
    try {
      for (const input of [source, file, () => source, '', 'relative/path', new URL('file:///tmp/source')]) {
        await expect(
          store.putFile(object, input as unknown as string, options(Buffer.alloc(0))),
        ).rejects.toBeInstanceOf(TypeError);
      }
      const stats = await file.stat();
      expect(stats.isFile()).toBe(true);
      expect(read).not.toHaveBeenCalled();
      expect(destroy).not.toHaveBeenCalled();
      expect(source.eventNames()).toEqual([]);
      expect(await readdir(root)).toEqual([]);
    } finally {
      await file.close();
    }
  });

  it.each(['sha1', 'sha256'] as const)(
    'should upload an owned regular staging file without deleting it (%s)',
    async (algorithm) => {
      const bytes = Buffer.from('data');
      const staging = await stage(bytes);
      await store.putFile(object, staging, options(bytes, algorithm));
      expect(await buffer((await store.get(object))!.stream)).toEqual(bytes);
      expect(await readFile(staging)).toEqual(bytes);
    },
  );

  it('should reject a hash mismatch before publishing and preserve the old object', async () => {
    await upload(Buffer.from('old'));
    const bytes = Buffer.from('new');
    await expect(
      store.putFile(object, await stage(bytes), { size: 3, expectedHash: { algorithm: 'sha1', hex: '0'.repeat(40) } }),
    ).rejects.toThrow('hash');
    expect(await buffer((await store.get(object))!.stream)).toEqual(Buffer.from('old'));
    expect(await readdir(root)).toHaveLength(1);
  });

  it('should reject malformed expected hashes before opening the source', async () => {
    const mockedOpen = vitest.mocked(open);
    mockedOpen.mockClear();
    for (const expectedHash of [
      undefined,
      { algorithm: 'md5', hex: '0'.repeat(32) },
      { algorithm: 'sha1', hex: 'g'.repeat(40) },
      { algorithm: 'sha256', hex: '0'.repeat(40) },
      { algorithm: 'sha1', hex: ` ${'0'.repeat(40)}` },
      { algorithm: 'sha1', hex: `${'0'.repeat(40)}\n` },
      { algorithm: 'sha256', hex: `${'0'.repeat(64)}\n` },
    ]) {
      await expect(
        store.putFile(object, '/missing/staging', { size: 4, expectedHash } as never),
      ).rejects.toBeInstanceOf(TypeError);
    }
    expect(mockedOpen).not.toHaveBeenCalled();
  });

  it('should distinguish missing objects from filesystem errors and delete idempotently', async () => {
    expect(await store.get(object)).toBeUndefined();
    await store.delete(object);
    await upload(Buffer.from('data'));
    await store.delete(object);
    await store.delete(object);
    expect(await store.get(object)).toBeUndefined();
    await rm(root, { recursive: true });
    await expect(upload(Buffer.alloc(0))).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('should return exact inclusive ranges and reject invalid or unsatisfiable ranges', async () => {
    await upload(Buffer.from('abcdef'));
    const result = await store.get(object, { range: { start: 1, end: 3 } });
    expect(result?.size).toBe(3);
    expect(await buffer(result!.stream)).toEqual(Buffer.from('bcd'));
    for (const range of [
      { start: -1, end: 2 },
      { start: 3, end: 2 },
      { start: 0.5, end: 2 },
      { start: 0, end: Infinity },
      { start: 0, end: 6 },
      { start: 6, end: 6 },
    ]) {
      await expect(store.get(object, { range })).rejects.toBeInstanceOf(RangeError);
    }
  });

  it('should reject unsafe keys without touching the filesystem', async () => {
    for (const key of [
      '',
      '../escape',
      '/absolute',
      'a/../b',
      'a/./b',
      'a//b',
      String.raw`a\b`,
      'a\u{0}b',
      'a'.repeat(1025),
    ]) {
      await expect(store.get({ key })).rejects.toBeInstanceOf(TypeError);
      await expect(store.putFile({ key }, '/missing', options(Buffer.alloc(0)))).rejects.toBeInstanceOf(TypeError);
      await expect(store.delete({ key })).rejects.toBeInstanceOf(TypeError);
    }
  });

  it('should not follow a symlink at the physical object path', async () => {
    const outside = path.join(root, 'outside');
    await writeFile(outside, 'private');
    const physical = path.join(root, createHash('sha256').update(object.key).digest('hex'));
    await symlink(outside, physical);
    await expect(store.get(object)).rejects.toMatchObject({ code: 'ELOOP' });
  });

  it('should reject missing, symlink and nonregular staging sources', async () => {
    await expect(
      store.putFile(object, path.join(stagingRoot, 'missing'), options(Buffer.alloc(0))),
    ).rejects.toMatchObject({ code: 'ENOENT' });
    const source = await stage();
    const link = path.join(stagingRoot, 'link');
    await symlink(source, link);
    await expect(store.putFile(object, link, options(Buffer.alloc(0)))).rejects.toMatchObject({ code: 'ELOOP' });
    await expect(store.putFile(object, stagingRoot, options(Buffer.alloc(0)))).rejects.toBeInstanceOf(TypeError);
    expect(await readdir(root)).toEqual([]);
  });

  it('should reject a FIFO staging source without waiting for a writer', async () => {
    const filename = path.join(stagingRoot, 'fifo');
    execFileSync('mkfifo', [filename]);
    await expect(store.putFile(object, filename, options(Buffer.alloc(0)))).rejects.toBeInstanceOf(TypeError);
    expect(await readdir(root)).toEqual([]);
  });

  it('should close the source if destination setup throws synchronously', async () => {
    const bytes = Buffer.from('data');
    const filename = await stage(bytes);
    const file = await open(filename, 'r');
    vitest.mocked(open).mockResolvedValueOnce(file);
    const invalidStore = new LocalObjectStoreRepository(`${root}\u{0}`);
    try {
      await expect(invalidStore.putFile(object, filename, options(bytes))).rejects.toBeInstanceOf(TypeError);
      expect(file.fd).toBe(-1);
    } finally {
      await file.close();
    }
  });

  it('should never remove a colliding temporary file it did not create', async () => {
    await upload(Buffer.from('old'));
    const uuid = '00000000-0000-4000-8000-000000000000';
    const physical = path.join(root, createHash('sha256').update(object.key).digest('hex'));
    const collision = `${physical}.${uuid}.tmp`;
    await writeFile(collision, 'not owned');
    vitest.mocked(randomUUID).mockReturnValueOnce(uuid);
    const bytes = Buffer.from('new');
    const filename = await stage(bytes);
    const file = await open(filename, 'r');
    vitest.mocked(open).mockResolvedValueOnce(file);
    await expect(store.putFile(object, filename, options(bytes))).rejects.toMatchObject({ code: 'EEXIST' });
    expect(file.fd).toBe(-1);
    expect(await readFile(collision, 'utf8')).toBe('not owned');
    expect(await buffer((await store.get(object))!.stream)).toEqual(Buffer.from('old'));
  });

  it('should preserve the previous object and clean temporary files on a size failure', async () => {
    await upload(Buffer.from('old'));
    const bytes = Buffer.from('new');
    for (const size of [2, 4, -1, NaN, 0.5]) {
      await expect(store.putFile(object, await stage(bytes), { ...options(bytes), size })).rejects.toBeInstanceOf(
        RangeError,
      );
      expect(await buffer((await store.get(object))!.stream)).toEqual(Buffer.from('old'));
      expect(await readdir(root)).toHaveLength(1);
    }
  });

  it('should preserve the previous object and close its owned stream on disk read failure', async () => {
    await upload(Buffer.from('old'));
    const bytes = Buffer.alloc(100_000);
    const staging = await stage(bytes);
    const file = await open(staging, 'r');
    const create = file.createReadStream.bind(file);
    let source: ReturnType<typeof create> | undefined;
    const failure = new Error('source failed');
    vitest.spyOn(file, 'read').mockRejectedValue(failure);
    vitest.spyOn(file, 'createReadStream').mockImplementation((options) => {
      source = create(options);
      return source;
    });
    vitest.mocked(open).mockResolvedValueOnce(file);
    await expect(store.putFile(object, staging, options(bytes))).rejects.toBe(failure);
    expect(source?.closed).toBe(true);
    expect(source?.eventNames()).toEqual([]);
    expect(await readdir(root)).toHaveLength(1);
    expect(await buffer((await store.get(object))!.stream)).toEqual(Buffer.from('old'));
  });

  it.each(['grow', 'shrink'] as const)('should validate the full byte count after opening (%s)', async (mode) => {
    const bytes = Buffer.alloc(100_000);
    const staging = await stage(bytes);
    const file = await open(staging, 'r');
    const create = file.createReadStream.bind(file);
    vitest.spyOn(file, 'createReadStream').mockImplementation((options) => {
      if (mode === 'grow') {
        appendFileSync(staging, 'extra');
      } else {
        writeFileSync(staging, 'short');
      }
      return create(options);
    });
    vitest.mocked(open).mockResolvedValueOnce(file);
    await expect(store.putFile(object, staging, options(bytes))).rejects.toBeInstanceOf(RangeError);
    expect(await readdir(root)).toEqual([]);
  });

  it('should never reopen the staging path between validation and transfer', async () => {
    const bytes = Buffer.from('original');
    const staging = await stage(bytes);
    const file = await open(staging, 'r');
    const create = file.createReadStream.bind(file);
    vitest.spyOn(file, 'createReadStream').mockImplementation((options) => {
      renameSync(staging, `${staging}.moved`);
      writeFileSync(staging, 'replaced');
      return create(options);
    });
    vitest.mocked(open).mockResolvedValueOnce(file);
    await store.putFile(object, staging, options(bytes));
    expect(await buffer((await store.get(object))!.stream)).toEqual(bytes);
    expect(await readFile(staging, 'utf8')).toBe('replaced');
  });

  it('should clean up aborted writes and close cancelled read streams', async () => {
    const controller = new AbortController();
    const bytes = Buffer.alloc(10 * 1024 * 1024);
    const staging = await stage(bytes);
    const file = await open(staging, 'r');
    const create = file.createReadStream.bind(file);
    vitest.spyOn(file, 'createReadStream').mockImplementation((options) => {
      const source = create(options);
      source.once('data', () => controller.abort());
      return source;
    });
    vitest.mocked(open).mockResolvedValueOnce(file);
    await expect(
      store.putFile(object, staging, { ...options(bytes), signal: controller.signal }),
    ).rejects.toMatchObject({ name: 'AbortError' });
    expect(await readdir(root)).toEqual([]);
    await upload(Buffer.alloc(100_000));
    const result = await store.get(object);
    const closed = new Promise<void>((resolve) => result!.stream.once('close', resolve));
    result!.stream.destroy();
    await closed;
    expect(result!.stream.closed).toBe(true);
  });

  it('should reject pre-aborted writes before opening any file', async () => {
    const controller = new AbortController();
    const reason = new Error('cancelled');
    controller.abort(reason);
    const mockedOpen = vitest.mocked(open);
    mockedOpen.mockClear();
    await expect(
      store.putFile(object, '/missing', { ...options(Buffer.alloc(0)), signal: controller.signal }),
    ).rejects.toBe(reason);
    expect(mockedOpen).not.toHaveBeenCalled();
  });

  it('should return explicit local identity and reject unsupported versions', async () => {
    const result = await upload(Buffer.alloc(0));
    expect(result).toEqual({ object });
    const read = await store.get(object);
    expect(read?.object).toEqual(object);
    expect(await buffer(read!.stream)).toEqual(Buffer.alloc(0));
    const versioned = { ...object, versionId: 'remote-version' };
    await expect(store.get(versioned)).rejects.toBeInstanceOf(TypeError);
    await expect(store.delete(versioned)).rejects.toBeInstanceOf(TypeError);
    await expect(store.putFile(versioned, await stage(), options(Buffer.alloc(0)))).rejects.toBeInstanceOf(TypeError);
  });

  it('should roundtrip binary bytes through a stream', async () => {
    const bytes = Buffer.from([0, 255, 128, 1, 0]);
    await upload(bytes);
    const result = await store.get(object);
    expect(result?.size).toBe(bytes.length);
    expect(await buffer(result!.stream)).toEqual(bytes);
  });
});
