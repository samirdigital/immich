import { S3Client, S3ServiceException } from '@aws-sdk/client-s3';
import { randomUUID } from 'node:crypto';
import { readFile, rm, stat } from 'node:fs/promises';
import { Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import {
  createQualificationClient,
  parseQualificationArgs,
  qualificationPlan,
  readQualificationCredentials,
  runQualification,
} from 'src/utils/b2-qualification.js';

const fixture = () => {
  const calls: { name: string; input: Record<string, any> }[] = [];
  const parts: Buffer[] = [];
  let object: Buffer | undefined;
  let aborted = false;
  const client = {
    send: async (command: { constructor: { name: string }; input: Record<string, any> }) => {
      await Promise.resolve();
      const { input } = command;
      const name = command.constructor.name;
      calls.push({ name, input });
      switch (name) {
        case 'HeadObjectCommand': {
          throw new S3ServiceException({
            name: 'NotFound',
            message: 'synthetic missing',
            $fault: 'client',
            $metadata: { httpStatusCode: 404 },
          });
        }
        case 'HeadBucketCommand': {
          return {};
        }
        case 'CreateMultipartUploadCommand': {
          return { UploadId: input.Key.endsWith('incomplete') ? 'incomplete-id' : 'complete-id' };
        }
        case 'UploadPartCommand': {
          parts.push(Buffer.from(input.Body));
          return { ETag: `etag-${input.PartNumber}` };
        }
        case 'CompleteMultipartUploadCommand': {
          object = Buffer.concat(parts);
          return { VersionId: 'owned-version', ETag: 'complete-etag' };
        }
        case 'GetObjectCommand': {
          if (!object)
            throw new S3ServiceException({
              name: 'NoSuchVersion',
              message: 'private-secret',
              $fault: 'client',
              $metadata: { httpStatusCode: 404 },
            });
          const match = input.Range?.match(/^bytes=(\d+)-(\d+)$/);
          const data = match ? object.subarray(Number(match[1]), Number(match[2]) + 1) : object;
          return {
            Body: Readable.from([data]),
            ContentLength: data.length,
            VersionId: 'owned-version',
            ServerSideEncryption: 'AES256',
            ContentRange: match ? `bytes ${match[1]}-${match[2]}/${object.length}` : undefined,
            $metadata: { httpStatusCode: match ? 206 : 200 },
          };
        }
        case 'DeleteObjectCommand': {
          expect(input.VersionId).toBe('owned-version');
          object = undefined;
          return {};
        }
        case 'AbortMultipartUploadCommand': {
          aborted = true;
          return {};
        }
        case 'ListPartsCommand': {
          if (aborted)
            throw new S3ServiceException({
              name: 'NoSuchUpload',
              message: 'private-secret',
              $fault: 'client',
              $metadata: { httpStatusCode: 404 },
            });
          return {};
        }
        default: {
          throw new Error('Unexpected command');
        }
      }
    },
  } as unknown as S3Client;
  return { client, calls, parts };
};

it('produces three real parts, validates full bytes and both boundary ranges, and verifies exact cleanup', async () => {
  const c = { ...config(), syntheticTestOptIn: true };
  const f = fixture();
  try {
    const result = await runQualification(c, { client: f.client });
    expect(result.status).toBe('synthetic-qualified');
    expect(f.parts.slice(0, 3).map((part) => part.length)).toEqual([8 * 1024 ** 2, 8 * 1024 ** 2, 1024 ** 2]);
    expect(f.calls.filter(({ name }) => name === 'GetObjectCommand').map(({ input }) => input.Range)).toEqual([
      undefined,
      'bytes=8388607-8388608',
      'bytes=16777215-16777216',
      'bytes=0-0',
    ]);
    const text = await readFile(`${c.workspace}/manifest.jsonl`, 'utf8');
    const events = text
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line));
    expect(events.filter((event) => event.kind === 'intent')).toHaveLength(f.calls.length);
    const directoryStats = await stat(c.workspace);
    const manifestStats = await stat(`${c.workspace}/manifest.jsonl`);
    expect(directoryStats.mode & 0o777).toBe(0o700);
    expect(manifestStats.mode & 0o777).toBe(0o600);
    expect(result.remaining).toEqual([]);
  } finally {
    await rm(c.workspace, { recursive: true, force: true });
  }
});

const config = () => ({
  bucket: 'synthetic-existing-bucket',
  endpoint: 'https://s3.us-west-004.backblazeb2.com',
  region: 'us-west-004',
  prefix: `immich-b2-qualification/${randomUUID()}/`,
  workspace: `/tmp/b2-qualification-${randomUUID()}`,
});

describe('B2 qualification guards', () => {
  it('accepts only explicit nonsecret CLI options and one bounded stdin credential payload', async () => {
    const c = config();
    const args = Object.entries(c).flatMap(([key, value]) => [`--${key}`, value]);
    expect(parseQualificationArgs(args)).toEqual(c);
    expect(() => parseQualificationArgs([...args, '--secretAccessKey', 'private-secret'])).toThrow('InvalidArguments');
    expect(() => parseQualificationArgs([...args, '--bucket', 'duplicate'])).toThrow('InvalidArguments');
    const payload = JSON.stringify({ accessKeyId: 'synthetic-id', secretAccessKey: 'private-secret' });
    const input = Readable.from([`${payload}\n`]);
    expect(await readQualificationCredentials(input)).toEqual(JSON.parse(payload));
    expect(input.destroyed).toBe(true);
    for (const text of [
      payload + payload,
      JSON.stringify({ accessKeyId: 'id', secretAccessKey: 'secret', extra: 'no' }),
      'x'.repeat(8193),
    ]) {
      await expect(readQualificationCredentials(Readable.from([text]))).rejects.toThrow('InvalidCredentials');
    }
    const client = createQualificationClient(c, JSON.parse(payload));
    expect(await client.config.maxAttempts()).toBe(1);
    expect(client.config.followRegionRedirects).toBe(false);
    expect(client.config.forcePathStyle).toBe(true);
    client.destroy();
  });
  it('refuses budgets before any call, and dry plan never touches SDK or workspace', async () => {
    const f = fixture();
    const result = await runQualification(config(), { client: f.client });
    expect(result.status).toBe('dry-plan');
    for (const limits of [{ maxRequests: 0 }, { maxBytes: 0 }, { maxRuntimeMs: 0 }]) {
      await expect(
        runQualification({ ...config(), ...limits, syntheticTestOptIn: true }, { client: f.client }),
      ).rejects.toThrow('Budget');
    }
    expect(f.calls).toHaveLength(0);
  });

  it('stops immediately if the fsync barrier fails and never persists request bodies', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    try {
      const result = await runQualification(c, {
        client: f.client,
        beforePersist: (event) => {
          expect(JSON.stringify(event)).not.toContain('Body');
          if (event.kind === 'intent' && event.phase === 'upload-part') throw new Error('private-secret');
        },
      });
      expect(result.errorClass).toBe('JournalFailure');
      expect(f.calls.map(({ name }) => name)).toEqual([
        'HeadBucketCommand',
        'HeadObjectCommand',
        'HeadObjectCommand',
        'CreateMultipartUploadCommand',
      ]);
      expect(JSON.stringify(result)).not.toContain('private-secret');
    } finally {
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('retains received Create identity when its ACK fails without clearing ambiguity or sending cleanup', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    try {
      const result = await runQualification(c, {
        client: f.client,
        beforePersist: (event) => {
          if (event.kind === 'ack' && event.phase === 'create') throw new Error('disk');
        },
      });
      expect(result.errorClass).toBe('JournalFailure');
      expect(result.remaining).toEqual([
        { key: `${c.prefix}sample`, uploadId: 'complete-id', unknown: 'create-outcome' },
      ]);
      expect(f.calls.map(({ name }) => name)).toEqual([
        'HeadBucketCommand',
        'HeadObjectCommand',
        'HeadObjectCommand',
        'CreateMultipartUploadCommand',
      ]);
      const journal = await readFile(`${c.workspace}/manifest.jsonl`, 'utf8');
      expect(journal).not.toContain('"kind":"ack","phase":"create"');
      expect(journal).not.toContain('owned-upload');
    } finally {
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('retains received Complete identity when its ACK fails without clearing ambiguity or sending cleanup', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    try {
      const result = await runQualification(c, {
        client: f.client,
        beforePersist: (event) => {
          if (event.kind === 'ack' && event.phase === 'complete') throw new Error('disk');
        },
      });
      expect(result.errorClass).toBe('JournalFailure');
      expect(result.remaining).toEqual([
        { key: `${c.prefix}sample`, uploadId: 'complete-id', versionId: 'owned-version', unknown: 'complete-outcome' },
      ]);
      expect(f.calls.map(({ name }) => name)).toEqual([
        'HeadBucketCommand',
        'HeadObjectCommand',
        'HeadObjectCommand',
        'CreateMultipartUploadCommand',
        'UploadPartCommand',
        'UploadPartCommand',
        'UploadPartCommand',
        'CompleteMultipartUploadCommand',
      ]);
      const journal = await readFile(`${c.workspace}/manifest.jsonl`, 'utf8');
      expect(journal).not.toContain('"kind":"ack","phase":"complete"');
      expect(journal).not.toContain('owned-version');
    } finally {
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('durably records exact response IDs in acknowledgements before any later barrier can fail', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    try {
      await runQualification(c, {
        client: f.client,
        beforePersist: (event) => {
          if (event.kind === 'owned-upload') throw new Error('private-secret');
        },
      });
      const text = await readFile(`${c.workspace}/manifest.jsonl`, 'utf8');
      const events = text
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line));
      expect(events.find((e) => e.kind === 'ack' && e.phase === 'create')).toMatchObject({ uploadId: 'complete-id' });
    } finally {
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('holds unknown Complete outcomes without retries or deletion', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    const send = f.client.send.bind(f.client);
    f.client.send = (async (command: any, options: any) => {
      if (command.constructor.name === 'CompleteMultipartUploadCommand') throw new Error('private-secret');
      return send(command, options);
    }) as S3Client['send'];
    try {
      const result = await runQualification(c, { client: f.client });
      expect(result.status).toBe('failed');
      expect(result.remaining).toEqual([
        { key: `${c.prefix}sample`, uploadId: 'complete-id', unknown: 'complete-outcome' },
      ]);
      expect(f.calls.some(({ name }) => name === 'DeleteObjectCommand' || name === 'AbortMultipartUploadCommand')).toBe(
        false,
      );
      expect(await readFile(`${c.workspace}/manifest.jsonl`, 'utf8')).not.toContain('private-secret');
    } finally {
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('does not mistake Abort ACK for absence and never retries a failed cleanup', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    const send = f.client.send.bind(f.client);
    f.client.send = (async (command: any, options: any) =>
      command.constructor.name === 'ListPartsCommand' ? {} : send(command, options)) as S3Client['send'];
    try {
      const result = await runQualification(c, { client: f.client });
      expect(result.status).toBe('failed');
      expect(result.remaining).toEqual([{ key: `${c.prefix}incomplete`, uploadId: 'incomplete-id' }]);
      expect(f.calls.filter(({ name }) => name === 'AbortMultipartUploadCommand')).toHaveLength(1);
    } finally {
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('closes the received GET body when its ACK fails before handoff', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    let body: Readable | undefined;
    const send = f.client.send.bind(f.client);
    f.client.send = (async (command: any, options: any) => {
      const result = (await send(command, options)) as any;
      if (command.constructor.name === 'GetObjectCommand') body = result.Body;
      return result;
    }) as S3Client['send'];
    try {
      const result = await runQualification(c, {
        client: f.client,
        beforePersist: (event) => {
          if (event.kind === 'ack' && event.phase === 'get') throw new Error('disk');
        },
      });
      expect(result.errorClass).toBe('JournalFailure');
      expect(body?.destroyed).toBe(true);
      expect(body?.closed).toBe(true);
      expect(body?.eventNames()).toEqual([]);
      expect(f.calls.at(-1)?.name).toBe('GetObjectCommand');
    } finally {
      body?.destroy();
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('fails closed on a source error during delayed GET ACK even if bytes remain readable', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    let body: Readable | undefined;
    const send = f.client.send.bind(f.client);
    f.client.send = (async (command: any, options: any) => {
      const result = (await send(command, options)) as any;
      if (command.constructor.name === 'GetObjectCommand') body = result.Body;
      return result;
    }) as S3Client['send'];
    try {
      const result = await runQualification(c, {
        client: f.client,
        beforePersist: async (event) => {
          if (event.kind !== 'ack' || event.phase !== 'get') return;
          await new Promise<void>((resolve) => setImmediate(resolve));
          body!.emit('error', new Error('source-private'));
        },
      });
      expect(result.status).toBe('failed');
      expect(result.phase).toBe('get');
      expect(result.errorClass).toBe('QualificationFailure');
      expect(body?.closed).toBe(true);
      expect(body?.eventNames()).toEqual([]);
      expect(f.calls.filter(({ name }) => name === 'GetObjectCommand')).toHaveLength(2);
      expect(JSON.stringify(result)).not.toContain('source-private');
      expect(await readFile(`${c.workspace}/manifest.jsonl`, 'utf8')).not.toContain('full-hash');
    } finally {
      body?.destroy();
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('releases all body listeners after successful bounded reads and metadata rejection', async () => {
    for (const invalidMetadata of [false, true]) {
      const c = { ...config(), syntheticTestOptIn: true };
      const f = fixture();
      const bodies: Readable[] = [];
      const send = f.client.send.bind(f.client);
      f.client.send = (async (command: any, options: any) => {
        const result = (await send(command, options)) as any;
        if (command.constructor.name === 'GetObjectCommand') {
          bodies.push(result.Body);
          if (invalidMetadata) delete result.ServerSideEncryption;
        }
        return result;
      }) as S3Client['send'];
      try {
        const result = await runQualification(c, { client: f.client });
        expect(result.status).toBe(invalidMetadata ? 'failed' : 'synthetic-qualified');
        expect(bodies.length).toBeGreaterThan(0);
        for (const body of bodies) {
          expect(body.closed).toBe(true);
          expect(body.eventNames()).toEqual([]);
        }
      } finally {
        await rm(c.workspace, { recursive: true, force: true });
      }
    }
  });

  it('observes asynchronous body destruction errors through close without overriding primary journal failure', async () => {
    for (const scenario of ['valid', 'metadata', 'ack-failure'] as const) {
      const c = { ...config(), syntheticTestOptIn: true };
      const f = fixture();
      let body: Readable | undefined;
      const send = f.client.send.bind(f.client);
      f.client.send = (async (command: any, options: any) => {
        const result = (await send(command, options)) as any;
        if (command.constructor.name === 'GetObjectCommand') {
          const original = result.Body as Readable;
          const data = (await Array.fromAsync(original)) as Buffer[];
          body = result.Body = new Readable({
            autoDestroy: false,
            read() {
              body!.push(Buffer.concat(data));
              body!.push(null);
            },
            destroy(_error, callback) {
              setImmediate(() => callback(new Error('close-private')));
            },
          });
          if (scenario === 'metadata') delete result.ServerSideEncryption;
        }
        return result;
      }) as S3Client['send'];
      try {
        const result = await runQualification(c, {
          client: f.client,
          beforePersist: (event) => {
            if (scenario === 'ack-failure' && event.kind === 'ack' && event.phase === 'get') throw new Error('disk');
          },
        });
        expect(result.status).toBe('failed');
        expect(result.phase).toBe('get');
        expect(result.errorClass).toBe(scenario === 'ack-failure' ? 'JournalFailure' : 'QualificationFailure');
        expect(body?.closed).toBe(true);
        expect(body?.eventNames()).toEqual([]);
        expect(JSON.stringify(result)).not.toContain('close-private');
        if (scenario === 'ack-failure') expect(f.calls.at(-1)?.name).toBe('GetObjectCommand');
      } finally {
        await rm(c.workspace, { recursive: true, force: true });
      }
    }
  });

  it('requires SSE response evidence, fails corrupted bytes, and keeps cancellation cleanup exact', async () => {
    for (const scenario of ['missing-sse', 'corrupt', 'cancel'] as const) {
      const c = { ...config(), syntheticTestOptIn: true };
      const f = fixture();
      const controller = new AbortController();
      const send = f.client.send.bind(f.client);
      f.client.send = (async (command: any, options: any) => {
        const result = (await send(command, options)) as any;
        if (command.constructor.name === 'GetObjectCommand') {
          if (scenario === 'missing-sse') delete result.ServerSideEncryption;
          else if (scenario === 'corrupt') result.Body = Readable.from([Buffer.alloc(result.ContentLength)]);
        }
        if (scenario === 'cancel' && command.constructor.name === 'UploadPartCommand') controller.abort();
        return result;
      }) as S3Client['send'];
      try {
        const result = await runQualification(c, { client: f.client, signal: controller.signal });
        expect(result.status).toBe('failed');
        expect(result.remaining).toEqual([]);
        for (const { input } of f.calls) {
          if (input.Key) expect([`${c.prefix}sample`, `${c.prefix}incomplete`]).toContain(input.Key);
          if (input.VersionId) expect(input.VersionId).toBe('owned-version');
        }
      } finally {
        await rm(c.workspace, { recursive: true, force: true });
      }
    }
  });

  it('holds unknown Create outcomes and refuses workspace reuse without touching SDK', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    const send = f.client.send.bind(f.client);
    f.client.send = (async (command: any, options: any) => {
      if (command.constructor.name === 'CreateMultipartUploadCommand') throw new Error('private-secret');
      return send(command, options);
    }) as S3Client['send'];
    try {
      const result = await runQualification(c, { client: f.client });
      expect(result.remaining).toEqual([{ key: `${c.prefix}sample`, unknown: 'create-outcome' }]);
      expect(f.calls.map(({ name }) => name)).toEqual(['HeadBucketCommand', 'HeadObjectCommand', 'HeadObjectCommand']);
      await expect(runQualification(c, { client: f.client })).rejects.toThrow();
      expect(f.calls).toHaveLength(3);
    } finally {
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('refuses an already-existing generated key before Create and sends no object mutations', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    const send = f.client.send.bind(f.client);
    f.client.send = (async (command: any, options: any) =>
      command.constructor.name === 'HeadObjectCommand' ? {} : send(command, options)) as S3Client['send'];
    try {
      const result = await runQualification(c, { client: f.client });
      expect(result.status).toBe('failed');
      expect(f.calls.some(({ name }) => name === 'CreateMultipartUploadCommand')).toBe(false);
      expect(result.remaining).toEqual([]);
    } finally {
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('reserves requests and time for exact cleanup when the forward deadline is exhausted', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    let now = 0;
    const clock = vi.spyOn(performance, 'now').mockImplementation(() => now);
    try {
      const result = await runQualification(c, {
        client: f.client,
        beforePersist: (event) => {
          if (event.kind === 'owned-upload') now = 176_000;
        },
      });
      expect(result.status).toBe('failed');
      expect(result.remaining).toEqual([]);
      expect(f.calls.some(({ name }) => name === 'UploadPartCommand')).toBe(false);
      expect(f.calls.slice(-2).map(({ name }) => name)).toEqual(['AbortMultipartUploadCommand', 'ListPartsCommand']);
    } finally {
      clock.mockRestore();
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('rechecks the runtime budget after a slow intent fsync before dispatching', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const f = fixture();
    let now = 0;
    const clock = vi.spyOn(performance, 'now').mockImplementation(() => now);
    try {
      const result = await runQualification(c, {
        client: f.client,
        beforePersist: (event) => {
          if (event.kind === 'intent' && event.phase === 'upload-part') now = 176_000;
        },
      });
      expect(result.status).toBe('failed');
      expect(f.calls.some(({ name }) => name === 'UploadPartCommand')).toBe(false);
      expect(result.remaining).toEqual([]);
    } finally {
      clock.mockRestore();
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('uses the real SDK protocol offline without following credential-exfiltration redirects or retries', async () => {
    const c = { ...config(), syntheticTestOptIn: true };
    const client = createQualificationClient(c, { accessKeyId: 'synthetic-id', secretAccessKey: 'private-secret' });
    let requests = 0;
    client.config.requestHandler = {
      handle: async (request: any) => {
        await Promise.resolve();
        requests++;
        expect(request.hostname).toBe('s3.us-west-004.backblazeb2.com');
        expect(request.protocol).toBe('https:');
        return {
          response: {
            statusCode: 307,
            headers: { location: 'https://evil.example/steal' },
            body: Readable.from(['<Error><Code>TemporaryRedirect</Code><Message>private-secret</Message></Error>']),
          },
        };
      },
    } as any;
    try {
      const result = await runQualification(c, { client });
      expect(result.status).toBe('failed');
      expect(requests).toBe(1);
      expect(JSON.stringify(result)).not.toContain('private-secret');
      expect(await readFile(`${c.workspace}/manifest.jsonl`, 'utf8')).not.toContain('private-secret');
    } finally {
      client.destroy();
      await rm(c.workspace, { recursive: true, force: true });
    }
  });

  it('defaults to a non-network plan and rejects non-B2 credential destinations', () => {
    expect(qualificationPlan(config()).live).toBe(false);
    for (const endpoint of [
      'https://evil.example',
      config().endpoint.replace('https', 'http'),
      'https://user@s3.us-west-004.backblazeb2.com',
      'https://s3.us-west-004.backblazeb2.com/redirect',
    ]) {
      expect(() => qualificationPlan({ ...config(), endpoint })).toThrow('InvalidConfig');
    }
    expect(() => qualificationPlan({ ...config(), prefix: 'ente/existing/' })).toThrow('InvalidConfig');
    expect(() => qualificationPlan({ ...config(), region: 'us-east-005' })).toThrow('InvalidConfig');
  });
});
