import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: { tsconfigPaths: true },
  test: {
    name: 'server:catalog',
    root: resolve(dirname(fileURLToPath(import.meta.url)), '..'),
    globals: true,
    include: ['test/catalog/**/*.spec.ts'],
    fileParallelism: false,
  },
  plugins: [swc.vite()],
});
