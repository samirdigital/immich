export const withPublicationLease = async <T>(
  work: () => Promise<T>,
  renew: () => Promise<void>,
  intervalMs = 60_000,
): Promise<T> => {
  let stopped = false;
  let failed = false;
  let failure: unknown;
  let pending = Promise.resolve();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const schedule = () => {
    if (stopped || failed) {
      return;
    }
    timer = setTimeout(() => {
      pending = Promise.try(renew)
        .catch((error: unknown) => {
          failed = true;
          failure = error;
        })
        .then(schedule);
    }, intervalMs);
    timer.unref();
  };
  schedule();
  let result: T;
  try {
    result = await work();
  } finally {
    stopped = true;
    clearTimeout(timer);
    await pending;
  }
  if (failed) {
    throw failure;
  }
  return result;
};
