import { Kysely } from 'kysely';
import { ObjectCatalogRepository } from 'src/repositories/object-catalog.repository.js';
import { DB } from 'src/schema/index.js';

it.each([
  '',
  '/',
  '/catalog',
  'catalog/',
  'catalog//nested',
  '.',
  '..',
  'catalog/../other',
  'catalog/./other',
  String.raw`catalog\other`,
  'catalog%2Fother',
  'catalog?other',
  'catalog#other',
  'catalog\u{0000}other',
  'catalog\nother',
  'catalog\n',
  'catalog\r',
  ' catalog',
  'catalog ',
  'cátalog',
  'a'.repeat(951),
  null,
  42,
])('rejects unsafe prefix %j synchronously before accessing the database', (objectKeyPrefix) => {
  const accessed = vi.fn(() => {
    throw new Error('Unexpected database access');
  });
  const db = new Proxy({} as Kysely<DB>, { get: accessed });
  expect(() => Reflect.construct(ObjectCatalogRepository, [db, { objectKeyPrefix }])).toThrow(
    'Invalid catalog object key prefix',
  );
  expect(accessed).not.toHaveBeenCalled();
});
