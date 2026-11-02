import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  S3Client,
  UploadPartCommand,
} from '@aws-sdk/client-s3';
import { pipeline } from 'node:stream/promises';
import type {
  S3MultipartBoundary,
  S3MultipartJournalEvent,
  S3MultipartOptions,
  S3ObjectWriteOptions,
} from 'src/repositories/s3-object-store.repository.js';
import {
  ObjectIdentity,
  closeObjectStreams,
  createObjectValidator,
  openObjectFile,
} from 'src/repositories/object-store.repository.js';

export const validateMultipartOptions = (config: S3MultipartOptions, size: number) => {
  if (
    [config.threshold, config.partSize, config.maxObjectSize, config.cleanupTimeoutMs].some(
      (value) => !Number.isSafeInteger(value),
    ) ||
    config.partSize < 5 * 1024 ** 2 ||
    config.partSize > 64 * 1024 ** 2 ||
    config.threshold < config.partSize ||
    config.threshold > 5 * 1024 ** 3 ||
    config.maxObjectSize < config.threshold ||
    config.maxObjectSize > 5 * 1024 ** 4 ||
    config.cleanupTimeoutMs < 1 ||
    config.cleanupTimeoutMs > 60_000 ||
    size > config.maxObjectSize ||
    Math.ceil(size / config.partSize) > 10_000
  ) {
    throw new RangeError('Invalid multipart bounds or object exceeds configured limits');
  }
};

export const putMultipartFile = async (
  client: S3Client,
  bucket: string,
  object: ObjectIdentity,
  stagingPath: string,
  options: S3ObjectWriteOptions,
  config: S3MultipartOptions,
) => {
  const journal = options.multipartJournal!;
  const intent = { bucket, key: object.key, attemptToken: options.multipartAttemptToken! };
  const controller = new AbortController();
  const signal = options.signal ? AbortSignal.any([options.signal, controller.signal]) : controller.signal;
  signal.throwIfAborted();
  if ((await client.config.maxAttempts()) !== 1) {
    throw new TypeError('Multipart requires a single attempt S3 client');
  }
  const source = await openObjectFile(stagingPath, options);
  const owned: Parameters<typeof closeObjectStreams> = [source];
  let at: S3MultipartBoundary = 'beforeCreate';
  let receivedResult: { versionId?: string; etag?: string } | undefined;
  const secondary: unknown[] = [];
  let failure: { reason: unknown } | undefined;
  let uploadId: string | undefined;
  let completeDispatched = false;
  let pumping: Promise<void> | undefined;
  const fail = (reason: unknown) => {
    failure ??= { reason };
    controller.abort(reason);
  };
  const onAbort = () => {
    failure ??= { reason: signal.reason };
    for (const stream of owned) {
      stream.destroy();
    }
  };
  const record = async (event: S3MultipartJournalEvent) => {
    signal.throwIfAborted();
    await journal(event);
    signal.throwIfAborted();
  };
  try {
    source.on('error', fail);
    signal.addEventListener('abort', onAbort, { once: true });
    const body = createObjectValidator(options);
    owned.push(body);
    body.on('error', fail);
    if (signal.aborted) {
      onAbort();
    }
    await record({
      ...intent,
      phase: 'beforeCreate',
      size: options.size,
      expectedHash: Object.freeze({ ...options.expectedHash }),
      partSize: config.partSize,
      partCount: Math.ceil(options.size / config.partSize),
    });
    at = 'create';
    const created = await client.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: object.key }), {
      abortSignal: signal,
    });
    uploadId = created.UploadId;
    if (!uploadId) {
      throw new TypeError('Multipart Create response requires UploadId');
    }
    at = 'uploadCreated';
    await record({ ...intent, phase: 'uploadCreated', uploadId });
    at = 'parts';
    const parts: { PartNumber: number; ETag: string }[] = [];
    let part: Buffer | undefined;
    let filled = 0;
    const upload = async () => {
      signal.throwIfAborted();
      const number = parts.length + 1;
      const result = await client.send(
        new UploadPartCommand({
          Bucket: bucket,
          Key: object.key,
          UploadId: uploadId,
          PartNumber: number,
          Body: part!.subarray(0, filled),
          ContentLength: filled,
        }),
        { abortSignal: signal },
      );
      if (!result.ETag) {
        throw new TypeError('Multipart part response requires ETag');
      }
      parts.push({ PartNumber: number, ETag: result.ETag });
      part = undefined;
      filled = 0;
    };
    pumping = pipeline(source, body).catch((error: unknown) => {
      fail(error);
    });
    for await (const chunk of body) {
      let offset = 0;
      while (offset < chunk.length) {
        part ??= Buffer.allocUnsafe(config.partSize);
        const count = Math.min(chunk.length - offset, config.partSize - filled);
        chunk.copy(part, filled, offset, offset + count);
        filled += count;
        offset += count;
        if (filled === config.partSize) {
          await upload();
        }
      }
    }
    await pumping;
    if (failure !== undefined) {
      throw failure.reason;
    }
    if (filled > 0) {
      await upload();
    }
    for (const entry of parts) {
      Object.freeze(entry);
    }
    Object.freeze(parts);
    at = 'beforeComplete';
    await record({ ...intent, phase: 'beforeComplete', uploadId, parts });
    signal.throwIfAborted();
    completeDispatched = true;
    at = 'complete';
    const result = await client.send(
      new CompleteMultipartUploadCommand({
        Bucket: bucket,
        Key: object.key,
        UploadId: uploadId,
        MultipartUpload: { Parts: parts },
      }),
      { abortSignal: signal },
    );
    at = 'receivedResult';
    receivedResult = { versionId: result.VersionId, etag: result.ETag };
    await journal({ ...intent, phase: 'receivedResult', uploadId, ...receivedResult });
    signal.throwIfAborted();
    if (!result.ETag) {
      throw new TypeError('Multipart Complete response requires ETag');
    }
    if (!result.VersionId || result.VersionId === 'null') {
      throw new TypeError('An explicit object version is required');
    }
    return { object: { key: object.key, versionId: result.VersionId }, etag: result.ETag };
  } catch (error) {
    fail(error);
    await pumping;
    if (uploadId && !completeDispatched) {
      const cleanup = new AbortController();
      const timer = setTimeout(
        () => cleanup.abort(new Error('Multipart cleanup deadline exceeded')),
        config.cleanupTimeoutMs,
      );
      let outcome: { ok: true } | { ok: false; reason: unknown };
      try {
        await client.send(new AbortMultipartUploadCommand({ Bucket: bucket, Key: object.key, UploadId: uploadId }), {
          abortSignal: cleanup.signal,
        });
        outcome = { ok: true };
      } catch (error_) {
        outcome = { ok: false, reason: error_ };
        secondary.push(error_);
      } finally {
        clearTimeout(timer);
      }
      try {
        await journal({ ...intent, phase: 'abortResult', uploadId, outcome });
      } catch (error_) {
        secondary.push(error_);
      }
    }
    await Promise.try(() =>
      journal({
        ...intent,
        phase: 'failure',
        at,
        uploadId,
        completeDispatched,
        reason: failure!.reason,
        secondary,
        receivedResult,
      }),
    ).catch(() => {});
    throw failure!.reason;
  } finally {
    await closeObjectStreams(...owned);
    signal.removeEventListener('abort', onAbort);
  }
};
