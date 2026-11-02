import {
  createQualificationClient,
  parseQualificationArgs,
  qualificationPlan,
  readQualificationCredentials,
  runQualification,
} from 'src/utils/b2-qualification.js';

const main = async () => {
  const config = parseQualificationArgs(process.argv.slice(2));
  const plan = qualificationPlan(config);
  if (!plan.live) {
    process.stdin.destroy();
    console.log(JSON.stringify({ status: 'dry-plan', ...plan }));
    return;
  }
  if (process.stdin.isTTY) throw new Error('PipedInputRequired');
  const credentials = await readQualificationCredentials(process.stdin);
  const client = createQualificationClient(config, credentials);
  const controller = new AbortController();
  const cancel = () => controller.abort();
  process.on('SIGINT', cancel);
  process.on('SIGTERM', cancel);
  try {
    const result = await runQualification(config, { client, signal: controller.signal });
    // Opaque cleanup handles are in the protected manifest, not stdout.
    console.log(
      JSON.stringify({
        status: result.status,
        phase: 'phase' in result ? result.phase : undefined,
        errorClass: 'errorClass' in result ? result.errorClass : undefined,
        remainingCount: result.remaining.length,
        manifest: `${config.workspace}/manifest.jsonl`,
      }),
    );
    if (result.status !== 'synthetic-qualified') process.exitCode = 1;
  } finally {
    client.destroy();
    process.off('SIGINT', cancel);
    process.off('SIGTERM', cancel);
  }
};

await main().catch(() => {
  process.stdin.destroy();
  console.error(JSON.stringify({ status: 'refused', errorClass: 'QualificationInputOrSetupFailure' }));
  process.exitCode = 1;
});
