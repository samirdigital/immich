import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { open } from 'node:fs/promises';
import path from 'node:path';
import { Readable, Transform, Writable } from 'node:stream';
import { finished } from 'node:stream/promises';
import type { S3ObjectWriteOptions } from 'src/repositories/s3-object-store.repository.js';

export interface ObjectIdentity {
  key: string;
  versionId?: string;
}

export interface ObjectByteRange {
  start: number;
  end: number;
}

export interface ObjectReadOptions {
  expectedSize?: number;
  range?: ObjectByteRange;
  signal?: AbortSignal;
}

export type ObjectFileIdentity = Readonly<
  Record<'dev' | 'ino' | 'size' | 'mtimeNs' | 'ctimeNs' | 'mode' | 'uid' | 'gid' | 'nlink', string>
>;

export interface ObjectWriteOptions {
  expectedFileIdentity?: ObjectFileIdentity;
  size: number;
  expectedHash: { algorithm: 'sha1' | 'sha256'; hex: string };
  signal?: AbortSignal;
}

export interface ObjectWriteResult {
  object: ObjectIdentity;
  etag?: string;
}

export interface ObjectRead extends ObjectWriteResult {
  totalSize?: number;
  stream: Readable;
  size: number;
}

export const validateObjectIdentity = (object: ObjectIdentity) => {
  if (
    typeof object.key !== 'string' ||
    Buffer.byteLength(object.key) > 1024 ||
    !/^[\w.-]+(?:\/[\w.-]+)*$/.test(object.key) ||
    object.key.split('/').some((part) => part === '.' || part === '..')
  ) {
    throw new TypeError('Invalid object key');
  }
};

export const validateObjectWrite = (options: ObjectWriteOptions) => {
  const { size: declaredSize } = options;
  if (!Number.isSafeInteger(declaredSize) || declaredSize < 0) {
    throw new RangeError('Invalid object size');
  }
  const hash = options.expectedHash;
  if (
    !hash ||
    !['sha1', 'sha256'].includes(hash.algorithm) ||
    typeof hash.hex !== 'string' ||
    !new RegExp(`^[a-fA-F0-9]{${hash.algorithm === 'sha1' ? 40 : 64}}$`).test(hash.hex)
  ) {
    throw new TypeError('Invalid expected object hash');
  }
};

export const validateObjectRange = (range?: ObjectByteRange, size?: number) => {
  if (
    range &&
    (!Number.isSafeInteger(range.start) ||
      !Number.isSafeInteger(range.end) ||
      range.start < 0 ||
      range.end < range.start ||
      (size !== undefined && range.end >= size))
  ) {
    throw new RangeError('Invalid inclusive object byte range');
  }
};

export interface ObjectStore {
  readonly s3Multipart?: Readonly<{
    bucket: string;
    putFile: (object: ObjectIdentity, stagingPath: string, options: S3ObjectWriteOptions) => Promise<ObjectWriteResult>;
  }>;
  get(object: ObjectIdentity, options?: ObjectReadOptions): Promise<ObjectRead | undefined>;
  putFile(object: ObjectIdentity, stagingPath: string, options: ObjectWriteOptions): Promise<ObjectWriteResult>;
  delete(object: ObjectIdentity): Promise<void>;
}

export const openObjectFile = async (stagingPath: string, options: ObjectWriteOptions) => {
  if (typeof stagingPath !== 'string' || !path.isAbsolute(stagingPath)) {
    throw new TypeError('Expected an absolute staging file path');
  }
  validateObjectWrite(options);
  options.signal?.throwIfAborted();
  const identity =
    options.expectedFileIdentity === undefined ? undefined : Object.freeze({ ...options.expectedFileIdentity });
  const file = await open(stagingPath, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stats = await file.stat({ bigint: true });
    if (identity) {
      const fields = ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs', 'mode', 'uid', 'gid', 'nlink'] as const;
      if (
        !stats.isFile() ||
        stats.nlink !== 1n ||
        (stats.mode & 0o222n) !== 0n ||
        stats.uid !== BigInt(process.getuid!()) ||
        fields.some(
          (key) =>
            typeof identity[key] !== 'string' ||
            !/^(0|[1-9][0-9]*)$/.test(identity[key]) ||
            identity[key] !== stats[key].toString(),
        )
      ) {
        throw new TypeError('Staged file identity mismatch');
      }
    }
    if (!stats.isFile()) {
      throw new TypeError('Staging source is not a regular file');
    }
    if (stats.size !== BigInt(options.size)) {
      throw new RangeError('Object size differs from declared size');
    }
    options.signal?.throwIfAborted();
    return file.createReadStream({ autoClose: true, emitClose: true, highWaterMark: 64 * 1024 });
  } catch (error) {
    await file.close();
    throw error;
  }
};

export const createObjectValidator = (options: ObjectWriteOptions) => {
  const hash = createHash(options.expectedHash.algorithm);
  let size = 0;
  return new Transform({
    highWaterMark: 64 * 1024,
    autoDestroy: true,
    emitClose: true,
    transform(chunk: Buffer, _encoding, callback) {
      size += chunk.length;
      if (size > options.size) {
        callback(new RangeError('Object size exceeds declared size'));
        return;
      }
      hash.update(chunk);
      callback(null, chunk);
    },
    flush(callback) {
      if (size !== options.size) {
        callback(new RangeError('Object size differs from declared size'));
      } else if (hash.digest('hex') === options.expectedHash.hex.toLowerCase()) {
        callback();
      } else {
        callback(new Error('Object hash differs from expected hash'));
      }
    },
  });
};

export const closeObjectStreams = async (...streams: (Readable | Writable)[]) => {
  const closed = streams.map((stream) => finished(stream, { cleanup: true }).catch(() => {}));
  for (const stream of streams) {
    stream.destroy();
  }
  await Promise.all(closed);
  for (const stream of streams) {
    stream.removeAllListeners();
  }
};
