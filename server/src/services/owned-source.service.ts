import { createHash, randomUUID } from 'node:crypto';
import { BigIntStats, constants } from 'node:fs';
import { lstat, open, realpath } from 'node:fs/promises';
import path from 'node:path';
import { Readable } from 'node:stream';
import { StageIdentity } from 'src/schema/tables/staged-object.table.js';

export interface SourceReservation {
  id: string;
  ownerId: string;
  role: 'original' | 'sidecar';
  root: string;
  name: string;
  rootDev: string;
  rootIno: string;
}

export interface SourceEvidence {
  identity: StageIdentity;
  sha256: string;
  sha1: string;
  size: string;
}

export interface OwnedSource extends SourceReservation {
  state: 'writing' | 'received' | 'sealing' | 'sealed' | 'held';
  identity: StageIdentity | null;
  sha256: string | null;
  sha1: string | null;
  size: string | null;
}

export interface SourceJournal {
  create(input: SourceReservation): Promise<OwnedSource>;
  received(id: string, ownerId: string, evidence: SourceEvidence): Promise<OwnedSource>;
  claimSeal(id: string, ownerId: string): Promise<OwnedSource>;
  sealed(id: string, ownerId: string, identity: StageIdentity): Promise<OwnedSource>;
  hold(id: string, ownerId: string): Promise<unknown>;
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

export class OwnedSourceService {
  constructor(
    private journal: SourceJournal,
    private root: string,
  ) {}

  private async rootStat() {
    if (!path.isAbsolute(this.root) || (await realpath(this.root)) !== this.root) {
      throw new Error('Source root must be canonical');
    }
    const root = await lstat(this.root, { bigint: true });
    if (!root.isDirectory() || (root.mode & 0o777n) !== 0o700n || root.uid !== BigInt(process.getuid!())) {
      throw new Error('Source root must be owned and private');
    }
    return root;
  }

  private async syncDirectory() {
    const directory = await open(this.root, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  }

  private async assertRoot(source: SourceReservation) {
    const root = await this.rootStat();
    if (
      source.root !== this.root ||
      root.dev.toString() !== source.rootDev ||
      root.ino.toString() !== source.rootIno ||
      !/^[a-f0-9-]{36}\.[a-z0-9]{1,16}$/.test(source.name) ||
      !source.name.startsWith(`${source.id}.`)
    ) {
      throw new Error('Source root or name identity mismatch');
    }
  }

  async receive(ownerId: string, role: 'original' | 'sidecar', extension: string, input: Readable) {
    if (!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(ownerId)) {
      throw new Error('Invalid source owner');
    }
    if (!['original', 'sidecar'].includes(role) || !/^\.[a-z0-9]{1,16}$/.test(extension)) {
      throw new Error('Invalid source role or extension');
    }
    const root = await this.rootStat();
    const id = randomUUID();
    const source = await this.journal.create({
      id,
      ownerId,
      role,
      root: this.root,
      name: `${id}${extension}`,
      rootDev: root.dev.toString(),
      rootIno: root.ino.toString(),
    });
    try {
      await this.assertRoot(source);
      const file = await open(
        path.join(this.root, source.name),
        constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
        0o600,
      );
      try {
        const sha256 = createHash('sha256');
        const sha1 = createHash('sha1');
        let size = 0;
        for await (const chunk of input) {
          const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          size += bytes.length;
          if (!Number.isSafeInteger(size)) {
            throw new TypeError('Source exceeds safe size');
          }
          sha256.update(bytes);
          sha1.update(bytes);
          let offset = 0;
          while (offset < bytes.length) {
            const { bytesWritten } = await file.write(bytes, offset, bytes.length - offset);
            if (bytesWritten === 0) {
              throw new Error('Source write made no progress');
            }
            offset += bytesWritten;
          }
        }
        if (size === 0) {
          throw new Error('Source is empty');
        }
        await file.sync();
        await this.syncDirectory();
        await this.assertRoot(source);
        const stats = await file.stat({ bigint: true });
        const named = await lstat(path.join(this.root, source.name), { bigint: true });
        if (
          !stats.isFile() ||
          stats.nlink !== 1n ||
          stats.size !== BigInt(size) ||
          named.ino !== stats.ino ||
          named.dev !== stats.dev ||
          named.nlink !== 1n
        ) {
          throw new Error('Source changed while receiving');
        }
        return await this.journal.received(id, ownerId, {
          size: String(size),
          sha256: sha256.digest('hex'),
          sha1: sha1.digest('hex'),
          identity: { ...snapshot(stats), rootDev: source.rootDev, rootIno: source.rootIno },
        });
      } finally {
        await file.close();
      }
    } catch (error) {
      await this.journal.hold(id, ownerId).catch(() => {});
      throw error;
    }
  }

  async seal(ownerId: string, id: string, modifiedAt: Date) {
    if (!Number.isFinite(modifiedAt.getTime())) {
      throw new TypeError('Invalid source timestamp');
    }
    const source = await this.journal.claimSeal(id, ownerId);
    try {
      await this.assertRoot(source);
      const file = await open(
        path.join(this.root, source.name),
        constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
      );
      try {
        const before = await file.stat({ bigint: true });
        if (
          !before.isFile() ||
          before.nlink !== 1n ||
          !source.identity ||
          Object.entries(snapshot(before)).some(
            ([key, value]) => source.identity![key as keyof StageIdentity] !== value,
          )
        ) {
          throw new Error('Received source identity changed');
        }
        const sha256 = createHash('sha256');
        for await (const chunk of file.createReadStream({ autoClose: false })) {
          sha256.update(chunk);
        }
        const afterRead = await file.stat({ bigint: true });
        if (
          sha256.digest('hex') !== source.sha256 ||
          JSON.stringify(snapshot(before)) !== JSON.stringify(snapshot(afterRead))
        ) {
          throw new Error('Received source bytes changed');
        }
        await file.utimes(modifiedAt, modifiedAt);
        await file.chmod(0o400);
        await file.sync();
        await this.syncDirectory();
        await this.assertRoot(source);
        const sealed = await file.stat({ bigint: true });
        const named = await lstat(path.join(this.root, source.name), { bigint: true });
        if (named.ino !== sealed.ino || named.dev !== sealed.dev || sealed.nlink !== 1n || named.nlink !== 1n) {
          throw new Error('Source changed before sealing');
        }
        return await this.journal.sealed(id, ownerId, {
          ...snapshot(sealed),
          rootDev: source.rootDev,
          rootIno: source.rootIno,
        });
      } finally {
        await file.close();
      }
    } catch (error) {
      await this.journal.hold(id, ownerId).catch(() => {});
      throw error;
    }
  }
}
