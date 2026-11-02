import { fstatSync, readlinkSync } from 'node:fs';
import { Readable } from 'node:stream';
import { pathToFileURL } from 'node:url';
import {
  catalogQualificationPlan,
  parseCatalogQualificationArgs,
  runCatalogQualification,
} from 'src/utils/b2-catalog-qualification.js';
import { createQualificationClient, readQualificationCredentials } from 'src/utils/b2-qualification.js';

/** Exported only to test that dry planning never touches stdin. */
export const catalogQualificationMain = async (
  args: string[],
  input: Readable = process.stdin,
  report: (result: Record<string, unknown>) => void = (result) => console.log(JSON.stringify(result)),
) => {
  const config = parseCatalogQualificationArgs(args);
  const plan = catalogQualificationPlan(config);
  if (!plan.live) {
    report({ status: 'dry-plan', ...plan });
    return 0;
  }
  const stdin = fstatSync(0);
  if (
    input !== process.stdin ||
    !stdin.isFIFO() ||
    stdin.uid !== process.getuid!() ||
    (stdin.mode & 0o077) !== 0 ||
    !/^pipe:\[\d+\]$/.test(readlinkSync('/proc/self/fd/0'))
  ) {
    throw new Error('PrivateStdinPipeRequired');
  }
  const credentials = await readQualificationCredentials(input);
  const client = createQualificationClient(config, credentials);
  const controller = new AbortController();
  const cancel = () => controller.abort();
  process.on('SIGINT', cancel);
  process.on('SIGTERM', cancel);
  try {
    const result = await runCatalogQualification(config, { client, signal: controller.signal });
    report({
      status: result.status,
      phase: 'phase' in result ? result.phase : undefined,
      requests: 'requests' in result ? result.requests : undefined,
      bytes: 'bytes' in result ? result.bytes : undefined,
      manifest: `${config.workspace}/manifest.jsonl`,
    });
    return result.status === 'catalog-synthetic-qualified' ? 0 : 1;
  } finally {
    client.destroy();
    process.off('SIGINT', cancel);
    process.off('SIGTERM', cancel);
  }
};

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  await catalogQualificationMain(process.argv.slice(2))
    .then((code) => {
      process.exitCode = code;
    })
    .catch(() => {
      console.error(JSON.stringify({ status: 'refused', errorClass: 'CatalogQualificationInputOrSetupFailure' }));
      process.exitCode = 1;
    });
}
