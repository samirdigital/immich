import { S3Client } from '@aws-sdk/client-s3';
import { Kysely, sql } from 'kysely';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { ObjectCatalogRepository } from 'src/repositories/object-catalog.repository.js';
import { OwnedSourceRepository } from 'src/repositories/owned-source.repository.js';
import { S3ObjectStoreRepository } from 'src/repositories/s3-object-store.repository.js';
import { UploadPublicationRepository } from 'src/repositories/upload-publication.repository.js';
import { DB } from 'src/schema/index.js';
import { ObjectCatalogService } from 'src/services/object-catalog.service.js';
import { OwnedSourceService } from 'src/services/owned-source.service.js';
import { PublicationRequest, UploadPublicationService } from 'src/services/upload-publication.service.js';

export const publicationFixture = async (db: Kysely<DB>, multipart = false) => {
  const ownerId = randomUUID();
  const sessionId = randomUUID();
  await sql`INSERT INTO "user" (id) VALUES (${ownerId}::uuid)`.execute(db);
  await sql`INSERT INTO session (id, "userId") VALUES (${sessionId}::uuid, ${ownerId}::uuid)`.execute(db);
  const root = await mkdtemp(path.join(os.tmpdir(), 'publication-fixture-'));
  const stageRoot = path.join(root, 'stage');
  await mkdir(stageRoot, { mode: 0o700 });
  const sources = new OwnedSourceService(new OwnedSourceRepository(db), root);
  const makeSource = async (bytes = 'publication bytes', role: 'original' | 'sidecar' = 'original') => {
    const received = await sources.receive(
      ownerId,
      role,
      role === 'original' ? '.jpg' : '.xmp',
      Readable.from([bytes]),
    );
    return sources.seal(ownerId, received.id, new Date('2020-01-01T00:00:00Z'));
  };
  const source = await makeSource(multipart ? 'x'.repeat(5_242_880) : 'publication bytes');
  const requests: { method: string; url: string }[] = [];
  const objects = new Map<string, Buffer>();
  const validatedBodies = new Map<string, Promise<void>>();
  const controls = {
    corruptGet: false,
    unknownPut: false,
    unknownComplete: false,
    beforePut: undefined as (() => Promise<void>) | undefined,
    beforeGet: undefined as (() => Promise<void>) | undefined,
  };
  const server = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url!, 'http://localhost');
      requests.push({ method: request.method!, url: request.url! });
      if (request.method === 'POST' && url.searchParams.has('uploads')) {
        response.writeHead(200, { 'content-type': 'application/xml' });
        response.end(
          '<InitiateMultipartUploadResult><UploadId>owned-upload</UploadId></InitiateMultipartUploadResult>',
        );
      } else if (request.method === 'POST' && url.searchParams.has('uploadId')) {
        for await (const _chunk of request) {
          void _chunk;
        }
        if (controls.unknownComplete) {
          request.socket.destroy();
          return;
        }
        response.writeHead(200, { 'x-amz-version-id': 'exact-version', 'content-type': 'application/xml' });
        response.end('<CompleteMultipartUploadResult><ETag>opaque</ETag></CompleteMultipartUploadResult>');
      } else if (request.method === 'PUT') {
        await controls.beforePut?.();
        const chunks: Buffer[] = [];
        for await (const chunk of request) {
          chunks.push(Buffer.from(chunk));
        }
        objects.set(url.pathname, Buffer.concat(chunks));
        await validatedBodies.get(decodeURIComponent(url.pathname.slice('/fixture/'.length)));
        if (controls.unknownPut) {
          request.socket.destroy();
          return;
        }
        response.writeHead(200, { 'x-amz-version-id': 'exact-version', etag: 'opaque' });
        response.end();
      } else if (request.method === 'GET') {
        await controls.beforeGet?.();
        const bytes = objects.get(url.pathname)!;
        response.writeHead(200, { 'x-amz-version-id': 'exact-version', 'content-length': bytes.length });
        response.end(controls.corruptGet ? Buffer.alloc(bytes.length) : bytes);
      } else {
        response.writeHead(500);
        response.end();
      }
    })().catch((error) => response.destroy(error));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const endpoint = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const client = new S3Client({
    endpoint,
    region: 'us-east-1',
    forcePathStyle: true,
    credentials: { accessKeyId: 'fixture', secretAccessKey: 'fixture' },
    maxAttempts: 1,
    requestChecksumCalculation: 'WHEN_REQUIRED',
    responseChecksumValidation: 'WHEN_REQUIRED',
  });
  client.middlewareStack.add(
    (next) => async (args) => {
      const input = args.input as { Key?: string; Body?: unknown };
      if (input.Key && input.Body instanceof Readable) {
        const ended = input.Body.readableEnded ? Promise.resolve() : once(input.Body, 'end').then(() => {});
        void ended.catch(() => {});
        validatedBodies.set(input.Key, ended);
      }
      return next(args);
    },
    { step: 'initialize', name: 'fixtureValidatedBodyBarrier' },
  );
  const binding = {
    provider: 's3' as const,
    storeId: randomUUID(),
    endpoint,
    bucket: 'fixture',
    store: new S3ObjectStoreRepository(client, {
      bucket: 'fixture',
      ...(multipart && {
        multipart: { threshold: 5_242_880, partSize: 5_242_880, maxObjectSize: 5_242_880, cleanupTimeoutMs: 1000 },
      }),
    }),
  };
  const catalog = new ObjectCatalogRepository(db);
  const stages = new ObjectCatalogService(catalog, stageRoot);
  const publications = new UploadPublicationRepository(db);
  const service = new UploadPublicationService(publications, catalog, stages, binding);
  const input: PublicationRequest = {
    ownerId,
    sourceId: source.id,
    auth: { kind: 'session', id: sessionId },
    options: {
      filename: 'photo.jpg',
      type: 'IMAGE',
      fileCreatedAt: '2020-01-01T00:00:00.000Z',
      fileModifiedAt: '2020-01-01T00:00:00.000Z',
      isFavorite: false,
      visibility: 'timeline',
      duration: null,
      livePhotoVideoId: null,
      metadata: [],
    },
  };
  const receipt = () =>
    db.selectFrom('upload_publication').selectAll().where('sourceId', '=', source.id).executeTakeFirstOrThrow();
  const usage = async () => {
    const result = await sql<{
      usage: string;
    }>`SELECT "quotaUsageInBytes"::text AS usage FROM "user" WHERE id=${ownerId}::uuid`.execute(db);
    return result.rows[0].usage;
  };
  const close = async () => {
    client.destroy();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(root, { recursive: true, force: true });
  };
  return {
    ownerId,
    sessionId,
    root,
    stageRoot,
    sources,
    source,
    makeSource,
    requests,
    controls,
    binding,
    catalog,
    stages,
    publications,
    service,
    input,
    receipt,
    usage,
    close,
  };
};
