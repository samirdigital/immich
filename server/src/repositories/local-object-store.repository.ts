import { createHash, randomUUID } from 'node:crypto';
import { constants, createWriteStream } from 'node:fs';
import { open, rename, unlink } from 'node:fs/promises';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import {
  ObjectIdentity,
  ObjectReadOptions,
  ObjectStore,
  ObjectWriteOptions,
  closeObjectStreams,
  createObjectValidator,
  openObjectFile,
  validateObjectIdentity,
  validateObjectRange,
} from 'src/repositories/object-store.repository.js';

export class LocalObjectStoreRepository implements ObjectStore {
  constructor(private root: string) {}

  private getPath(object: ObjectIdentity) {
    validateObjectIdentity(object);
    if (object.versionId !== undefined) {
      throw new TypeError('Local object store does not support versions');
    }
    return path.join(this.root, createHash('sha256').update(object.key).digest('hex'));
  }

  async putFile(object: ObjectIdentity, stagingPath: string, options: ObjectWriteOptions) {
    const destination = this.getPath(object);
    const temporary = `${destination}.${randomUUID()}.tmp`;
    const input = await openObjectFile(stagingPath, options);
    const owned: Parameters<typeof closeObjectStreams> = [input];
    let created = false;
    try {
      const validator = createObjectValidator(options);
      owned.push(validator);
      const output = createWriteStream(temporary, { flags: 'wx', mode: 0o600, flush: true });
      owned.push(output);
      output.once('open', () => {
        created = true;
      });
      await pipeline(input, validator, output, { signal: options.signal });
      options.signal?.throwIfAborted();
      await rename(temporary, destination);
      return { object: { key: object.key } };
    } finally {
      await closeObjectStreams(...owned);
      if (created) {
        await unlink(temporary).catch((error: NodeJS.ErrnoException) => {
          if (error.code !== 'ENOENT') {
            throw error;
          }
        });
      }
    }
  }

  async get(object: ObjectIdentity, options: ObjectReadOptions = {}) {
    const destination = this.getPath(object);
    validateObjectRange(options.range);
    try {
      const file = await open(destination, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
      try {
        const stats = await file.stat();
        if (!stats.isFile()) {
          throw new TypeError('Object is not a regular file');
        }
        validateObjectRange(options.range, stats.size);
        return {
          object: { key: object.key },
          stream: file.createReadStream({ ...options.range, signal: options.signal }),
          size: options.range ? options.range.end - options.range.start + 1 : stats.size,
        };
      } catch (error) {
        await file.close();
        throw error;
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        throw error;
      }
    }
  }

  async delete(object: ObjectIdentity): Promise<void> {
    try {
      await unlink(this.getPath(object));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        throw error;
      }
    }
  }
}
