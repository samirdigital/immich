import { ServiceUnavailableException } from '@nestjs/common';
import { NextFunction, Response } from 'express';
import { once } from 'node:events';
import { Transform } from 'node:stream';
import { finished, pipeline } from 'node:stream/promises';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { ObjectByteRange, ObjectRead, closeObjectStreams } from 'src/repositories/object-store.repository.js';
import { OriginalResponse } from 'src/services/original-reader.js';
import { sendFile } from 'src/utils/file.js';

// This sender is deliberately limited to original downloads. Local legacy behavior is unchanged.
export const sendOriginal = async (
  res: Response,
  next: NextFunction,
  handler: () => Promise<OriginalResponse>,
  logger: LoggingRepository,
) => {
  const controller = new AbortController();
  let disconnected = false;
  let deadline: NodeJS.Timeout | undefined;
  const abort = () => {
    if (res.writableFinished) {
      return;
    }

    disconnected = true;
    controller.abort();
  };
  res.req.once('aborted', abort);
  res.once('close', abort);
  let read: ObjectRead | undefined;
  let validator: Transform | undefined;
  let cloud = false;
  let pumping: Promise<void> | undefined;
  try {
    if (res.req.aborted || res.destroyed) {
      disconnected = true;
      controller.abort();
    }
    controller.signal.throwIfAborted();
    const result = await handler();
    if (result.kind === 'local') {
      return await sendFile(res, next, () => result.file, logger);
    }
    cloud = true;
    deadline = setTimeout(() => controller.abort(), result.readTimeoutMs);
    deadline.unref();
    controller.signal.throwIfAborted();
    const req = res.req;
    const modified = Math.floor(result.modified.getTime() / 1000) * 1000;
    res.setHeader('Content-Type', result.file.contentType);
    if (result.file.fileName) {
      res.setHeader('Content-Disposition', `inline; filename*=UTF-8''${encodeURIComponent(result.file.fileName)}`);
    }
    res.setHeader(
      'Cache-Control',
      'private, max-age=86400, no-transform, stale-while-revalidate=2592000, stale-if-error=2592000',
    );
    res.setHeader('ETag', result.etag);
    res.setHeader('Last-Modified', result.modified.toUTCString());
    res.setHeader('Accept-Ranges', 'bytes');
    const ifMatch = req.get('If-Match');
    const unmodified = req.get('If-Unmodified-Since');
    if (
      (ifMatch && ifMatch !== '*' && !ifMatch.split(/\s*,\s*/).includes(result.etag)) ||
      (!ifMatch && unmodified && modified > Date.parse(unmodified))
    ) {
      res.status(412).end();
      return;
    }
    if (req.fresh) {
      res.status(304).removeHeader('Content-Type');
      res.removeHeader('Content-Disposition');
      res.end();
      return;
    }
    let range: ObjectByteRange | undefined;
    const ifRange = req.get('If-Range');
    const rangeFresh =
      !ifRange || ifRange === result.etag || (!ifRange.includes('"') && Date.parse(ifRange) >= modified);
    if (/^ *bytes=/.test(req.get('Range') ?? '') && rangeFresh) {
      const ranges = req.range(result.size, { combine: true });
      if (ranges === -1) {
        res.setHeader('Content-Range', `bytes */${result.size}`);
        res.status(416).end();
        return;
      }
      if (ranges && ranges !== -2 && ranges.length === 1) {
        range = ranges[0];
      }
    }
    const length = range ? range.end - range.start + 1 : result.size;
    res.status(range ? 206 : 200);
    if (range) {
      res.setHeader('Content-Range', `bytes ${range.start}-${range.end}/${result.size}`);
    }
    res.setHeader('Content-Length', length);
    if (req.method === 'HEAD') {
      res.end();
      return;
    }
    read = await result.open(controller.signal, range);
    if (!read) {
      throw new Error('Missing original');
    }
    // Take ownership immediately on arrival, before any validation or cancellation check.
    read.stream.on('error', () => {});
    controller.signal.throwIfAborted();
    if (
      read.size !== length ||
      read.totalSize !== result.size ||
      read.object.key !== result.object.key ||
      read.object.versionId !== result.object.versionId
    ) {
      throw new Error('Unexpected original identity or length');
    }
    let count = 0;
    let held: Buffer | undefined;
    // Withhold one byte until EOF: an overlong response must not look complete to an HTTP client.
    validator = new Transform({
      transform(chunk: Buffer, _encoding, callback) {
        count += chunk.length;
        if (count > length) {
          callback(new Error('Original overrun'));
          return;
        }
        if (chunk.length > 0) {
          if (held) {
            validator!.push(held);
          }
          validator!.push(chunk.subarray(0, -1));
          held = Buffer.from(chunk.subarray(-1));
        }
        callback();
      },
      flush(callback) {
        if (count !== length) {
          callback(new Error('Original truncated'));
          return;
        }
        if (held) {
          validator!.push(held);
        }
        callback();
      },
    });
    let pumpFailure: { reason: unknown } | undefined;
    pumping = pipeline(read.stream, validator, { signal: controller.signal }).catch((error: unknown) => {
      pumpFailure = { reason: error };
    });
    for await (const chunk of validator) {
      controller.signal.throwIfAborted();
      if (!res.write(chunk)) {
        await once(res, 'drain', { signal: controller.signal });
      }
    }
    await pumping;
    if (pumpFailure) {
      throw pumpFailure.reason;
    }
    const completed = finished(res, { cleanup: true });
    res.end();
    await completed;
  } catch (error) {
    if (res.headersSent || res.destroyed || disconnected) {
      res.destroy();
    } else if (error instanceof ServiceUnavailableException || cloud) {
      for (const header of [
        'Content-Length',
        'Content-Type',
        'Content-Range',
        'Content-Disposition',
        'Cache-Control',
        'ETag',
        'Last-Modified',
        'Accept-Ranges',
      ]) {
        res.removeHeader(header);
      }
      next(new ServiceUnavailableException('Original unavailable'));
    } else {
      await sendFile(
        res,
        next,
        () => {
          throw error;
        },
        logger,
      );
    }
  } finally {
    clearTimeout(deadline);
    controller.abort();
    await pumping;
    await closeObjectStreams(...(read ? [read.stream] : []), ...(validator ? [validator] : []));
    res.req.off('aborted', abort);
    res.off('close', abort);
  }
};
