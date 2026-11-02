import { S3Client } from '@aws-sdk/client-s3';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { buffer } from 'node:stream/consumers';
import postgres from 'postgres';
import { runCatalogQualification } from 'src/utils/b2-catalog-qualification.js';

if (process.env.CATALOG_TEST_DISPOSABLE !== 'YES' || !process.env.CATALOG_TEST_SOCKET)
  throw new Error('PrivatePGRequired');

async function fixture(mode = 'happy') {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'catalog-qualification-'));
  const config = {
    bucket: 'synthetic-bucket',
    region: 'us-west-004',
    endpoint: 'https://s3.us-west-004.backblazeb2.com',
    prefix: `immich-b2-qualification/${randomUUID()}/`,
    workspace: path.join(directory, 'evidence'),
    pgSocket: process.env.CATALOG_TEST_SOCKET!,
    pgPort: Number(process.env.CATALOG_TEST_PORT || 55_441),
    disposableDatabaseOptIn: true,
    syntheticTestOptIn: true,
  };
  const requests: string[] = [];
  const parts: Buffer[] = [];
  let deleted = false;
  const server = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url!, 'http://localhost');
      requests.push(`${request.method} ${url.search}`);
      expect(decodeURIComponent(url.pathname).startsWith(`/synthetic-bucket/${config.prefix}`)).toBe(true);
      if (url.searchParams.has('uploads'))
        response.end(
          '<InitiateMultipartUploadResult><UploadId>fixture-upload</UploadId></InitiateMultipartUploadResult>',
        );
      else
        switch (request.method) {
          case 'PUT': {
            parts.push(await buffer(request));
            response.setHeader('etag', `"part-${parts.length}"`);
            response.end();

            break;
          }
          case 'POST': {
            await buffer(request);
            if (mode === 'unknown-complete') {
              request.socket.destroy();
              return;
            }
            response.setHeader('x-amz-version-id', 'fixture-version');
            response.end('<CompleteMultipartUploadResult><ETag>"completed"</ETag></CompleteMultipartUploadResult>');

            break;
          }
          case 'DELETE': {
            expect(url.searchParams.get('versionId')).toBe('fixture-version');
            deleted = true;
            response.statusCode = 204;
            response.end();

            break;
          }
          case 'GET': {
            expect(url.searchParams.get('versionId')).toBe('fixture-version');
            if (deleted) {
              expect(request.headers.range).toBe('bytes=0-0');
              response.statusCode = 404;
              response.end('<Error><Code>NoSuchVersion</Code></Error>');
            } else {
              const bytes = Buffer.concat(parts);
              if (mode === 'corrupt') bytes[0] ^= 1;
              response.setHeader('content-length', bytes.length);
              response.setHeader('x-amz-version-id', 'fixture-version');
              response.end(bytes);
            }

            break;
          }
          default: {
            throw new Error('UnexpectedOperation');
          }
        }
    })().catch(() => {
      response.statusCode = 500;
      response.end();
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const client = new S3Client({
    endpoint: `http://127.0.0.1:${(server.address() as { port: number }).port}`,
    region: config.region,
    forcePathStyle: true,
    maxAttempts: 1,
    credentials: { accessKeyId: 'synthetic', secretAccessKey: 'synthetic' },
    requestChecksumCalculation: 'WHEN_REQUIRED',
    responseChecksumValidation: 'WHEN_REQUIRED',
  });
  const bodies: Readable[] = [];
  client.middlewareStack.add(
    (next) => async (args) => {
      const result = await next(args);
      if ('Body' in result.output && result.output.Body instanceof Readable) bodies.push(result.output.Body);
      return result;
    },
    { step: 'initialize', name: 'captureFixtureBodies' },
  );
  return {
    config,
    client,
    bodies,
    requests,
    parts,
    close: async () => {
      client.destroy();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

async function evidence(workspace: string) {
  const manifestText = await readFile(path.join(workspace, 'manifest.jsonl'), 'utf8');
  const events = manifestText
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line));
  const database = events.find((event) => event.type === 'database-intent').database;
  const db = postgres({
    host: process.env.CATALOG_TEST_SOCKET,
    port: Number(process.env.CATALOG_TEST_PORT || 55_441),
    user: os.userInfo().username,
    database,
    max: 1,
  });
  return { events, db };
}

async function observed(
  db: ReturnType<typeof postgres>,
  table: 'staged_object' | 'object_upload_intent' | 'object_version',
  field: 'state' | 'verification',
) {
  const [row] = await db.unsafe(`SELECT ${field} FROM ${table}`);
  return row[field];
}

it('qualifies one actual-service 17MiB multipart object with durable exact-key enrollment and exact deletion', async () => {
  const f = await fixture();
  try {
    expect(await runCatalogQualification(f.config, { client: f.client })).toMatchObject({
      status: 'catalog-synthetic-qualified',
      requests: 8,
      bytes: 34 * 1024 ** 2,
    });
    for (const [name, mode] of [
      ['', 0o700],
      ['manifest.jsonl', 0o600],
      ['source', 0o400],
      ['stage', 0o700],
    ] as const) {
      const info = await stat(path.join(f.config.workspace, name));
      expect(info.mode & 0o777).toBe(mode);
    }
    expect(f.parts.map((part) => part.length)).toEqual([8 * 1024 ** 2, 8 * 1024 ** 2, 1024 ** 2]);
    const { events, db } = await evidence(f.config.workspace);
    try {
      expect(events.findIndex((event) => event.type === 'allow-key')).toBeLessThan(
        events.findIndex((event) => event.type === 'remote-intent'),
      );
      expect(events.some((event) => event.type === 'cleanup-blocked')).toBe(true);
      expect(await observed(db, 'staged_object', 'state')).toBe('deleted');
      expect(await observed(db, 'object_upload_intent', 'state')).toBe('acknowledged');
      expect(await observed(db, 'object_version', 'verification')).toBe('verified');
    } finally {
      await db.end();
    }
  } finally {
    await f.close();
  }
}, 30_000);

it.each([
  ['CreateMultipartUploadCommand', 1],
  ['CompleteMultipartUploadCommand', 5],
  ['GetObjectCommand', 6],
] as const)(
  'holds all evidence and freezes cleanup when %s ACK cannot be fsynced',
  async (command, requests) => {
    const f = await fixture();
    let failed = false;
    try {
      const result = await runCatalogQualification(f.config, {
        client: f.client,
        beforePersist: (event) => {
          if (event.type === 'remote-ack' && event.command === command) {
            failed = true;
            throw new Error('InjectedManifestFailure');
          }
          if (failed) throw new Error('PersistAfterFreeze');
        },
      });
      expect(result).toMatchObject({ status: 'held', requests });
      if (command !== 'GetObjectCommand') {
        const receivedText = await readFile(path.join(f.config.workspace, 'received-observations.jsonl'), 'utf8');
        const received = JSON.parse(receivedText.trim().split('\n').at(-1)!);
        const manifest = await readFile(path.join(f.config.workspace, 'manifest.jsonl'), 'utf8');
        const key = manifest
          .split('\n')
          .filter(Boolean)
          .map((line) => JSON.parse(line))
          .find((event) => event.type === 'allow-key').key;
        expect(received).toEqual({
          key,
          receivedCreateUploadId: 'fixture-upload',
          ...(command === 'CompleteMultipartUploadCommand' && { receivedCompleteVersionId: 'fixture-version' }),
        });
        expect(result).toMatchObject({ receivedObservation: { ...received, persisted: true } });
        const observationInfo = await stat(path.join(f.config.workspace, 'received-observations.jsonl'));
        expect(observationInfo.mode & 0o777).toBe(0o600);
      }
      expect(f.requests).toHaveLength(requests);
      expect(f.requests.some((request) => request.startsWith('DELETE'))).toBe(false);
      expect(f.bodies.every((body) => body.destroyed)).toBe(true);
      const { events, db } = await evidence(f.config.workspace);
      try {
        expect(events.at(-1).type).toBe('remote-intent');
        expect(await observed(db, 'staged_object', 'state')).toBe('ready');
        const retainedSource = await readFile(path.join(f.config.workspace, 'source'));
        expect(retainedSource.length).toBe(17 * 1024 ** 2);
        if (command === 'GetObjectCommand') {
          expect(await observed(db, 'object_version', 'verification')).toBe('failed');
        } else {
          expect(await observed(db, 'object_upload_intent', 'state')).toBe('ambiguous');
          expect(await db`select * from object_version`).toHaveLength(0);
        }
      } finally {
        await db.end();
      }
    } finally {
      await f.close();
    }
  },
  30_000,
);

it('cancels at the GET ACK barrier, awaits stream disposal and holds the real stage', async () => {
  const f = await fixture();
  const controller = new AbortController();
  try {
    const result = await runCatalogQualification(f.config, {
      client: f.client,
      signal: controller.signal,
      beforePersist: (event) => {
        if (event.type === 'remote-ack' && event.command === 'GetObjectCommand') controller.abort();
      },
    });
    expect(result).toMatchObject({ status: 'held' });
    expect(f.bodies).toHaveLength(1);
    expect(f.bodies[0].destroyed).toBe(true);
    expect(f.requests).toHaveLength(6);
    const { db } = await evidence(f.config.workspace);
    try {
      expect(await observed(db, 'staged_object', 'state')).toBe('ready');
    } finally {
      await db.end();
    }
  } finally {
    await f.close();
  }
}, 30_000);

it.each(['corrupt', 'unknown-complete'])(
  'holds the actual stage and never guesses cleanup after %s',
  async (mode) => {
    const f = await fixture(mode);
    try {
      expect(await runCatalogQualification(f.config, { client: f.client })).toMatchObject({
        status: 'held',
        phase: mode === 'corrupt' ? 'verify' : 'upload',
        requests: mode === 'corrupt' ? 6 : 5,
      });
      expect(f.requests).toHaveLength(mode === 'corrupt' ? 6 : 5);
      expect(f.requests.some((request) => request.startsWith('DELETE'))).toBe(false);
      const { db } = await evidence(f.config.workspace);
      try {
        expect(await observed(db, 'staged_object', 'state')).toBe('ready');
        if (mode === 'corrupt') expect(await observed(db, 'object_version', 'verification')).toBe('failed');
        else {
          expect(await observed(db, 'object_upload_intent', 'state')).toBe('ambiguous');
          expect(await db`select * from object_version`).toHaveLength(0);
        }
      } finally {
        await db.end();
      }
    } finally {
      await f.close();
    }
  },
  30_000,
);

it.each([1, 2])(
  'returns private memory observations without claiming persistence when observation write %s fails',
  async (failedWrite) => {
    const f = await fixture();
    let writes = 0;
    try {
      const result = await runCatalogQualification(f.config, {
        client: f.client,
        beforeObservationPersist: () => {
          if (++writes === failedWrite) throw new Error('ObservationStorageFailed');
        },
      });
      expect(result).toMatchObject({
        status: 'held',
        receivedObservation: {
          receivedCreateUploadId: 'fixture-upload',
          ...(failedWrite === 2 && { receivedCompleteVersionId: 'fixture-version' }),
          persisted: false,
        },
      });
      expect(f.requests).toHaveLength(failedWrite === 1 ? 1 : 5);
      const text = await readFile(path.join(f.config.workspace, 'received-observations.jsonl'), 'utf8');
      expect(text.includes('fixture-version')).toBe(false);
      expect(text.includes('fixture-upload')).toBe(failedWrite === 2);
      const { events, db } = await evidence(f.config.workspace);
      try {
        expect(events.at(-1).type).toBe('remote-intent');
        expect(await observed(db, 'object_upload_intent', 'state')).toBe('ambiguous');
        expect(await db`select * from object_version`).toHaveLength(0);
      } finally {
        await db.end();
      }
    } finally {
      await f.close();
    }
  },
  30_000,
);
