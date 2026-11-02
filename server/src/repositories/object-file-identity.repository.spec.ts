import { createHash } from 'node:crypto';
import { chmod, mkdtemp, rename, rm, stat, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { closeObjectStreams, openObjectFile } from 'src/repositories/object-store.repository.js';

it('rejects replacement with identical bytes before exposing the provider descriptor', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'sealed-source-'));
  const source = path.join(root, 'stage');
  try {
    await writeFile(source, 'same bytes', { mode: 0o400 });
    const stats = await stat(source, { bigint: true });
    const expectedFileIdentity = {
      dev: stats.dev.toString(),
      ino: stats.ino.toString(),
      size: stats.size.toString(),
      mtimeNs: stats.mtimeNs.toString(),
      ctimeNs: stats.ctimeNs.toString(),
      mode: stats.mode.toString(),
      uid: stats.uid.toString(),
      gid: stats.gid.toString(),
      nlink: stats.nlink.toString(),
    };
    await rename(source, path.join(root, 'old'));
    await writeFile(source, 'same bytes', { mode: 0o400 });
    const options = {
      size: 10,
      expectedHash: { algorithm: 'sha256' as const, hex: createHash('sha256').update('same bytes').digest('hex') },
      expectedFileIdentity,
    };
    let opened: Awaited<ReturnType<typeof openObjectFile>> | undefined;
    try {
      await expect(
        (async () => {
          opened = await openObjectFile(source, options);
        })(),
      ).rejects.toThrow('identity');
    } finally {
      if (opened) {
        await closeObjectStreams(opened);
      }
    }
    await chmod(source, 0o600);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('rejects an explicitly invalid fingerprint instead of silently using an unsealed source', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'sealed-source-'));
  const source = path.join(root, 'stage');
  try {
    await writeFile(source, '', { mode: 0o400 });
    let opened: Awaited<ReturnType<typeof openObjectFile>> | undefined;
    try {
      await expect(
        (async () => {
          opened = await openObjectFile(source, {
            size: 0,
            expectedHash: { algorithm: 'sha256', hex: createHash('sha256').digest('hex') },
            expectedFileIdentity: null as never,
          });
        })(),
      ).rejects.toThrow('identity');
    } finally {
      if (opened) {
        await closeObjectStreams(opened);
      }
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
