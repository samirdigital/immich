import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
  S3ServiceException,
} from '@aws-sdk/client-s3';
import { Readable, addAbortSignal } from 'node:stream';
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
  validateObjectWrite,
} from 'src/repositories/object-store.repository.js';

export interface S3ObjectStoreOptions {
  bucket: string;
}

const isObjectMissing = (error: unknown) =>
  error instanceof S3ServiceException &&
  error.$metadata.httpStatusCode === 404 &&
  (error.name === 'NoSuchKey' || error.name === 'NoSuchVersion');

const validateVersion = (versionId?: string) => {
  if (!versionId || versionId === 'null') {
    throw new TypeError('An explicit object version is required');
  }
};

export class S3ObjectStoreRepository implements ObjectStore {
  constructor(
    private client: S3Client,
    private options: S3ObjectStoreOptions,
  ) {}

  async get(object: ObjectIdentity, options: ObjectReadOptions = {}) {
    validateObjectIdentity(object);
    validateVersion(object.versionId);
    validateObjectRange(options.range);
    const result = await this.client
      .send(
        new GetObjectCommand({
          Bucket: this.options.bucket,
          Key: object.key,
          VersionId: object.versionId,
          Range: options.range ? `bytes=${options.range.start}-${options.range.end}` : undefined,
        }),
        { abortSignal: options.signal },
      )
      .catch((error: unknown) => {
        if (isObjectMissing(error)) {
          return;
        }
        if (
          error instanceof S3ServiceException &&
          error.name === 'InvalidRange' &&
          error.$metadata.httpStatusCode === 416
        ) {
          throw new RangeError('Unsatisfiable object byte range', { cause: error });
        }
        throw error;
      });
    if (!result) {
      return;
    }
    const body = result.Body;
    if (!(body instanceof Readable)) {
      throw new TypeError('Expected a Node.js object response stream');
    }
    try {
      if (
        result.VersionId !== object.versionId ||
        !Number.isSafeInteger(result.ContentLength) ||
        result.ContentLength! < 0
      ) {
        throw new TypeError('Invalid object response version or length');
      }
      const range = options.range;
      if (range) {
        const match = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(result.ContentRange ?? '');
        if (
          result.$metadata.httpStatusCode !== 206 ||
          !match ||
          Number(match[1]) !== range.start ||
          Number(match[2]) !== range.end ||
          Number(match[3]) <= range.end ||
          result.ContentLength !== range.end - range.start + 1
        ) {
          throw new RangeError('Object response does not match requested range');
        }
      }
      return {
        object: { key: object.key, versionId: result.VersionId },
        stream: options.signal ? addAbortSignal(options.signal, body) : body,
        size: result.ContentLength!,
        etag: result.ETag,
      };
    } catch (error) {
      body.destroy();
      throw error;
    }
  }

  async putFile(object: ObjectIdentity, stagingPath: string, options: ObjectWriteOptions) {
    validateObjectIdentity(object);
    if (object.versionId !== undefined) {
      throw new TypeError('A PUT creates a new object version');
    }
    validateObjectWrite(options);
    if (options.size > 5 * 1024 ** 3) {
      throw new RangeError('Object exceeds single PUT limit');
    }
    const source = await openObjectFile(stagingPath, options);
    const owned: Parameters<typeof closeObjectStreams> = [source];
    let detachSignal: (() => void) | undefined;
    try {
      const body = createObjectValidator(options);
      owned.push(body);
      const controller = new AbortController();
      const signal = options.signal ? AbortSignal.any([options.signal, controller.signal]) : controller.signal;
      let failure: { reason: unknown } | undefined;
      const fail = (error: unknown) => {
        if (failure === undefined) {
          failure = { reason: error };
        }
        controller.abort(error);
      };
      const onAbort = () => {
        if (failure === undefined) {
          failure = { reason: signal.reason };
        }
        source.destroy(signal.reason);
        body.destroy(signal.reason);
      };
      source.on('error', fail);
      body.on('error', fail);
      signal.addEventListener('abort', onAbort, { once: true });
      detachSignal = () => signal.removeEventListener('abort', onAbort);
      if (signal.aborted) {
        onAbort();
      }
      const pumping = pipeline(source, body).catch((error: unknown) => {
        fail(error);
      });
      const transfer = Promise.try(async () => {
        signal.throwIfAborted();
        const result = await this.client.send(
          new PutObjectCommand({
            Bucket: this.options.bucket,
            Key: object.key,
            Body: body,
            ContentLength: options.size,
          }),
          { abortSignal: signal },
        );
        if (!body.readableEnded) {
          throw new Error('Upload response arrived before complete source validation and consumption');
        }
        validateVersion(result.VersionId);
        return { object: { key: object.key, versionId: result.VersionId! }, etag: result.ETag };
      }).catch((error: unknown) => {
        fail(error);
      });
      const [result] = await Promise.all([transfer, pumping]);
      if (failure !== undefined) {
        throw failure.reason;
      }
      return result!;
    } finally {
      await closeObjectStreams(...owned);
      detachSignal?.();
    }
  }

  async delete(object: ObjectIdentity): Promise<void> {
    validateObjectIdentity(object);
    validateVersion(object.versionId);
    try {
      await this.client.send(
        new DeleteObjectCommand({
          Bucket: this.options.bucket,
          Key: object.key,
          VersionId: object.versionId,
        }),
      );
    } catch (error) {
      if (!isObjectMissing(error)) {
        throw error;
      }
    }
  }
}
