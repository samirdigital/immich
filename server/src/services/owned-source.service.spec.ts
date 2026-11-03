import { createHash, randomUUID } from 'node:crypto';
import { lstat, mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { StageIdentity } from 'src/schema/tables/staged-object.table.js';
import { OwnedSource, SourceEvidence, SourceReservation } from 'src/services/owned-source.service.js';

it('journals owner identity before consuming bytes and seals timestamp before recording identity', async () => {
  const { OwnedSourceService } = await import('src/services/owned-source.service.js');
  const root = await mkdtemp(path.join(os.tmpdir(), 'owned-source-'));
  const ownerId = randomUUID();
  const rows = new Map<string, OwnedSource>();
  const repository = {
    create: vi.fn(async (row: SourceReservation) => {
      expect(await readdir(root)).toEqual([]);
      rows.set(row.id, { ...row, state: 'writing', size: null, sha256: null, sha1: null, identity: null });
      return rows.get(row.id)!;
    }),
    received: vi.fn((id: string, owner: string, evidence: SourceEvidence) => {
      expect(owner).toBe(ownerId);
      Object.assign(rows.get(id)!, evidence, { state: 'received' });
      return Promise.resolve(rows.get(id)!);
    }),
    claimSeal: vi.fn((id: string, owner: string) => {
      expect(owner).toBe(ownerId);
      const row = rows.get(id)!;
      if (row.state !== 'received') {
        throw new Error('Source unavailable');
      }
      const snapshot = { ...row };
      row.state = 'sealing';
      return Promise.resolve(snapshot);
    }),
    sealed: vi.fn(async (id: string, owner: string, identity: StageIdentity) => {
      expect(owner).toBe(ownerId);
      const stats = await lstat(path.join(root, rows.get(id)!.name), { bigint: true });
      expect(identity.mtimeNs).toBe(stats.mtimeNs.toString());
      Object.assign(rows.get(id)!, { identity, state: 'sealed' });
      return rows.get(id)!;
    }),
    hold: vi.fn(),
  };
  try {
    const service = new OwnedSourceService(repository, root);
    const input = Readable.from(
      (function* () {
        expect(repository.create).toHaveBeenCalledOnce();
        expect(rows.values().next().value).toMatchObject({ ownerId, role: 'original', state: 'writing' });
        yield Buffer.from('actual owned bytes');
      })(),
    );
    const received = await service.receive(ownerId, 'original', '.jpg', input);
    expect(received).toMatchObject({
      state: 'received',
      size: '18',
      sha256: createHash('sha256').update('actual owned bytes').digest('hex'),
      sha1: createHash('sha1').update('actual owned bytes').digest('hex'),
    });
    const modifiedAt = new Date('2020-03-04T05:06:07.000Z');
    const sealed = await service.seal(ownerId, received.id, modifiedAt);
    expect(sealed.state).toBe('sealed');
    const stats = await lstat(path.join(root, sealed.name));
    expect(stats.mtime.toISOString()).toBe(modifiedAt.toISOString());
    expect(await readFile(path.join(root, sealed.name), 'utf8')).toBe('actual owned bytes');
    expect(repository.hold).not.toHaveBeenCalled();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
