import { Selectable } from 'kysely';
import { createHash, randomUUID } from 'node:crypto';
import { BigIntStats, constants } from 'node:fs';
import { lstat, open, readdir, realpath, unlink } from 'node:fs/promises';
import path from 'node:path';
import { ObjectCatalogRepository } from 'src/repositories/object-catalog.repository.js';
import { ObjectStore, ObjectWriteResult, closeObjectStreams } from 'src/repositories/object-store.repository.js';
import { StagedObjectTable } from 'src/schema/tables/staged-object.table.js';

export interface CatalogStoreBinding {
  provider: 'local' | 's3';
  storeId: string;
  store: ObjectStore;
}

const snapshot = (stats: BigIntStats) => ({
  dev: stats.dev.toString(),
  ino: stats.ino.toString(),
  size: stats.size.toString(),
  mtimeNs: stats.mtimeNs.toString(),
  ctimeNs: stats.ctimeNs.toString(),
  mode: stats.mode.toString(),
  uid: stats.uid.toString(),
  gid: stats.gid.toString(),
  nlink: stats.nlink.toString(),
});

export class ObjectCatalogService {
  constructor(
    private catalog: ObjectCatalogRepository,
    private root: string,
  ) {}

  private async rootStat() {
    if (!path.isAbsolute(this.root) || (await realpath(this.root)) !== this.root) {
      throw new Error('Staging root must be an absolute canonical private directory');
    }
    const stats = await lstat(this.root, { bigint: true });
    if (!stats.isDirectory() || (stats.mode & 0o777n) !== 0o700n || stats.uid !== BigInt(process.getuid!())) {
      throw new Error('Staging root must be owned and private');
    }
    return stats;
  }

  private async syncDirectory() {
    const directory = await open(this.root, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  }

  private async validateStage(stage: Selectable<StagedObjectTable>) {
    const root = await this.rootStat();
    if (
      stage.root !== this.root ||
      !stage.identity ||
      root.dev.toString() !== stage.identity.rootDev ||
      root.ino.toString() !== stage.identity.rootIno
    ) {
      throw new Error('Staging root identity mismatch');
    }
    const file = await open(
      path.join(this.root, stage.id),
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
    try {
      const before = await file.stat({ bigint: true });
      const identity = stage.identity;
      if (
        !before.isFile() ||
        before.nlink !== 1n ||
        Object.entries(snapshot(before)).some(([key, value]) => identity[key as keyof typeof identity] !== value)
      ) {
        throw new Error('Staged inode identity mismatch');
      }
      const hash = createHash('sha256');
      for await (const chunk of file.createReadStream({ autoClose: false })) {
        hash.update(chunk);
      }
      const after = await file.stat({ bigint: true });
      const named = await lstat(path.join(this.root, stage.id), { bigint: true });
      if (
        hash.digest('hex') !== stage.hash ||
        JSON.stringify(snapshot(before)) !== JSON.stringify(snapshot(after)) ||
        named.ino !== before.ino ||
        named.dev !== before.dev ||
        named.nlink !== 1n
      ) {
        throw new Error('Staged bytes or identity changed');
      }
    } finally {
      await file.close();
    }
  }

  async verify(stageId: string, versionId: string, binding: CatalogStoreBinding) {
    const stage = await this.catalog.getStage(stageId);
    const versions = await this.catalog.listVersions(stageId);
    const version = versions.find((version) => version.id === versionId);
    if (
      !stage ||
      stage.root !== this.root ||
      !version ||
      version.provider !== binding.provider ||
      version.storeId !== binding.storeId
    ) {
      throw new Error('Store/version identity mismatch');
    }
    const pin = await this.catalog.pin(stageId);
    let detail: string;
    let verified = false;
    try {
      const object = { key: version.key, ...(version.versionId !== null && { versionId: version.versionId }) };
      const read = await binding.store.get(object);
      if (!read) {
        throw new Error('Exact version missing');
      }
      try {
        if (
          read.object.key !== object.key ||
          read.object.versionId !== object.versionId ||
          read.size !== Number(stage.size)
        ) {
          throw new Error('Remote identity or size mismatch');
        }
        const hash = createHash('sha256');
        let count = 0;
        for await (const chunk of read.stream) {
          count += Buffer.byteLength(chunk);
          if (count > Number(stage.size)) {
            throw new Error('Remote size exceeded');
          }
          hash.update(chunk);
        }
        const digest = hash.digest('hex');
        detail = `Full GET size=${count} sha256=${digest}`;
        if (count !== Number(stage.size) || digest !== stage.hash) {
          throw new Error(`Remote bytes mismatch: ${detail}`);
        }
        verified = true;
      } finally {
        await closeObjectStreams(read.stream);
      }
    } catch (error) {
      detail = String(error);
    }
    await this.catalog.observeVersion(versionId, { outcome: verified ? 'verified' : 'failed', detail });
    await this.catalog.unpin(stageId, pin);
    return verified;
  }

  private async removeStage(
    catalog: ObjectCatalogRepository,
    stage: Selectable<StagedObjectTable>,
    recovering = false,
  ) {
    try {
      await this.validateStage(stage);
    } catch (error) {
      if (!recovering || (error as NodeJS.ErrnoException).code !== 'ENOENT') {
        await catalog.quarantine(stage.id, String(error));
        throw error;
      }
    }
    await unlink(path.join(this.root, stage.id)).catch((error: NodeJS.ErrnoException) => {
      if (!recovering || error.code !== 'ENOENT') {
        throw error;
      }
    });
    await this.syncDirectory();
    await catalog.finishCleanup(stage.id);
  }

  async cleanup(stageId: string) {
    return this.catalog.withCleanupLock(stageId, async (catalog) => {
      const current = await catalog.getStage(stageId);
      if (!current || current.root !== this.root) {
        throw new Error('Stage root mismatch');
      }
      const stage = await catalog.claimCleanup(stageId);
      if (!stage) {
        return false;
      }
      await this.removeStage(catalog, stage);
      return true;
    });
  }

  async upload(stageId: string, binding: CatalogStoreBinding, leaseMs = 60_000) {
    const stage = await this.catalog.getStage(stageId);
    if (!stage || stage.root !== this.root || stage.state !== 'ready') {
      throw new Error('Stage unavailable');
    }
    const intent = await this.catalog.prepare(stageId, binding.provider, binding.storeId);
    return this.sendPrepared(intent.id, binding, leaseMs);
  }

  async sendPrepared(intentId: string, binding: CatalogStoreBinding, leaseMs = 60_000) {
    const intent = await this.catalog.getIntent(intentId);
    const stage = await this.catalog.getStage(intent.stageId);
    if (
      intent.state !== 'prepared' ||
      intent.provider !== binding.provider ||
      intent.storeId !== binding.storeId ||
      !stage ||
      stage.root !== this.root ||
      stage.state !== 'ready'
    ) {
      throw new Error('Prepared intent/store/stage unavailable');
    }
    const claim = await this.catalog.startSending(intent.id, leaseMs);
    if (!claim?.token) {
      throw new Error('Claim lost');
    }
    const controller = new AbortController();
    let stopped = false;
    let renewal = Promise.resolve();
    let timer: ReturnType<typeof setTimeout>;
    const schedule = () => {
      timer = setTimeout(
        () => {
          const deadline = setTimeout(
            () => controller.abort(new Error('Upload renewal deadline exceeded')),
            Math.max(1, Math.floor(leaseMs / 3)),
          );
          renewal = this.catalog
            .renewSending(intent.id, claim.token!, leaseMs)
            .then((valid) => {
              if (!valid) {
                throw new Error('Upload lease lost');
              }
            })
            .catch(() => {
              controller.abort(new Error('Upload lease lost or renewal failed'));
            })
            .finally(() => {
              clearTimeout(deadline);
              if (!stopped && !controller.signal.aborted) {
                schedule();
              }
            });
        },
        Math.max(1, Math.min(10_000, Math.floor(leaseMs / 3))),
      );
    };
    const stopRenewal = async () => {
      stopped = true;
      clearTimeout(timer);
      await renewal;
    };
    schedule();
    let result: ObjectWriteResult | undefined;
    try {
      try {
        await this.validateStage(stage);
      } catch (error) {
        await this.catalog.quarantine(stage.id, String(error));
        throw error;
      }
      const options = {
        signal: controller.signal,
        size: Number(stage.size),
        expectedHash: { algorithm: 'sha256' as const, hex: stage.hash! },
        expectedFileIdentity: Object.freeze({ ...stage.identity! }),
      };
      const multipart = binding.store.s3Multipart;
      if (multipart && binding.provider !== 's3') {
        throw new Error('Multipart provider binding mismatch');
      }
      result = multipart
        ? await multipart.putFile({ key: intent.key }, path.join(this.root, stage.id), {
            ...options,
            multipartAttemptToken: claim.token,
            multipartJournal: (event) =>
              this.catalog.recordMultipart(
                {
                  intentId: intent.id,
                  storeId: intent.storeId,
                  bucket: multipart.bucket,
                  key: intent.key,
                  token: claim.token!,
                },
                event,
              ),
          })
        : await binding.store.putFile({ key: intent.key }, path.join(this.root, stage.id), options);
      await stopRenewal();
      controller.signal.throwIfAborted();
      return await this.catalog.acknowledge(intent.id, claim.token, result);
    } catch (error) {
      await this.catalog
        .holdAttempt(
          intent.id,
          claim.token,
          JSON.stringify({
            error: binding.store.s3Multipart ? 'Upload failed; uncertain outcome; hold for review' : String(error),
            receivedResult: result,
          }),
        )
        .catch(() => {});
      throw error;
    } finally {
      await stopRenewal();
    }
  }

  async recover() {
    await this.rootStat();
    const stages = await this.catalog.listStages(this.root);
    for (const stage of stages) {
      await this.catalog.expireSending(stage.id);
      if (stage.state === 'deleting') {
        await this.catalog.withCleanupLock(stage.id, async (catalog) => {
          const current = await catalog.getStage(stage.id);
          if (current?.state === 'deleting' && current.root === this.root) {
            await this.removeStage(catalog, current, true);
          }
        });
      } else if (stage.state === 'creating') {
        await this.catalog.quarantine(stage.id, 'Incomplete creation: operator review required');
      }
    }
    const known = new Set(stages.filter((stage) => stage.state !== 'deleted').map((stage) => stage.id));
    const held = [];
    for (const stage of await this.catalog.listStages(this.root)) {
      const intents = await this.catalog.listIntents(stage.id);
      const versions = await this.catalog.listVersions(stage.id);
      if (
        versions.some((version) => version.verification !== 'verified') ||
        stage.state === 'quarantined' ||
        stage.pins.length > 0 ||
        intents.some((intent) => intent.state !== 'acknowledged')
      ) {
        held.push({
          stageId: stage.id,
          state: stage.state,
          pins: stage.pins,
          evidence: stage.evidence,
          intents,
          multipart: await this.catalog.listMultipart(stage.id),
          versions,
        });
      }
    }
    const names = await readdir(this.root);
    return { unknown: names.filter((name) => !known.has(name)), held };
  }

  async stage(source: string) {
    const root = await this.rootStat();
    const id = randomUUID();
    await this.catalog.createStage(id, this.root);
    try {
      if (
        (await realpath(source)) !== path.resolve(source) ||
        path.resolve(source) === this.root ||
        path.resolve(source).startsWith(`${this.root}/`)
      ) {
        throw new Error('Source must be canonical and outside the owned staging root');
      }
      const input = await open(source, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
      try {
        const before = await input.stat({ bigint: true });
        if (!before.isFile() || before.nlink !== 1n) {
          throw new Error('Source must be a regular non-hardlinked file');
        }
        const output = await open(
          path.join(this.root, id),
          constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
          0o600,
        );
        try {
          const hash = createHash('sha256');
          for await (const chunk of input.createReadStream({ autoClose: false })) {
            hash.update(chunk);
            let offset = 0;
            while (offset < chunk.length) {
              const { bytesWritten } = await output.write(chunk, offset);
              offset += bytesWritten;
            }
          }
          const after = await input.stat({ bigint: true });
          const named = await lstat(source, { bigint: true });
          if (
            JSON.stringify(snapshot(before)) !== JSON.stringify(snapshot(after)) ||
            named.ino !== before.ino ||
            named.dev !== before.dev ||
            after.nlink !== 1n
          ) {
            throw new Error('Source changed during staging');
          }
          await output.chmod(0o400);
          await output.sync();
          const owned = await output.stat({ bigint: true });
          await this.syncDirectory();
          const namedOwned = await lstat(path.join(this.root, id), { bigint: true });
          if (
            owned.nlink !== 1n ||
            namedOwned.nlink !== 1n ||
            namedOwned.ino !== owned.ino ||
            namedOwned.dev !== owned.dev
          ) {
            throw new Error('Owned staged file replaced or hardlinked before sealing');
          }
          const currentRoot = await this.rootStat();
          if (root.ino !== currentRoot.ino || root.dev !== currentRoot.dev) {
            throw new Error('Staging root replaced');
          }
          return await this.catalog.sealStage(
            id,
            { ...snapshot(owned), rootDev: root.dev.toString(), rootIno: root.ino.toString() },
            hash.digest('hex'),
          );
        } finally {
          await output.close();
        }
      } finally {
        await input.close();
      }
    } catch (error) {
      await this.catalog.quarantine(id, String(error));
      throw error;
    }
  }
}
