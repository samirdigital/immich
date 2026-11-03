import {
  PublicationRequest,
  canonicalPublicationJson,
  publicationFingerprint,
  snapshotPublicationRequest,
} from 'src/utils/upload-publication.js';

const request = (): PublicationRequest => ({
  ownerId: 'owner',
  sourceId: 'source',
  auth: { kind: 'session', id: 'session' },
  options: {
    filename: 'photo.jpg',
    type: 'IMAGE',
    fileCreatedAt: '2020-01-01T00:00:00Z',
    fileModifiedAt: '2020-01-01T00:00:00Z',
    isFavorite: false,
    visibility: 'timeline',
    duration: null,
    livePhotoVideoId: null,
    metadata: [],
  },
});

describe('publication request sealing', () => {
  it('canonicalizes object keys without losing arrays, absence, false or null', () => {
    expect(canonicalPublicationJson({ b: [false, null, 0], a: 'value' })).toBe(
      canonicalPublicationJson({ a: 'value', b: [false, null, 0] }),
    );
    expect(publicationFingerprint({ sidecar: null })).not.toBe(publicationFingerprint({ sidecar: '' }));
    expect(publicationFingerprint({ a: 1 })).toMatch(/^v1:[a-f0-9]{64}$/);
  });

  it.each([NaN, Infinity, undefined, 1n, new Date()])('rejects unsupported JSON %s', (value) => {
    expect(() => canonicalPublicationJson(value)).toThrow('Invalid publication JSON');
  });

  it('normalizes timestamps, sorts metadata and snapshots mutable nested caller options', () => {
    const input = request();
    input.options.metadata = [
      { key: 'z', value: { b: 2, a: 1 } },
      { key: 'a', value: {} },
    ];
    const snapshot = snapshotPublicationRequest(input);
    input.options.filename = 'changed.jpg';
    input.options.metadata[0].value.b = 3;
    expect(snapshot.options.filename).toBe('photo.jpg');
    expect(snapshot.options.fileModifiedAt).toBe('2020-01-01T00:00:00.000Z');
    expect(snapshot.options.metadata).toEqual([
      { key: 'a', value: {} },
      { key: 'z', value: { a: 1, b: 2 } },
    ]);
  });

  it('whitelists authentication provenance without carrying credentials', () => {
    const input = request();
    Object.assign(input.auth, { token: 'must-not-persist', password: 'must-not-persist' });
    expect(snapshotPublicationRequest(input).auth).toEqual({ kind: 'session', id: 'session' });
  });

  it.each(['../escape.jpg', '/absolute.jpg', String.raw`bad\name.jpg`, '', 'a'.repeat(256)])(
    'rejects filename %s',
    (filename) => {
      const input = request();
      input.options.filename = filename;
      expect(() => snapshotPublicationRequest(input)).toThrow('Invalid publication options');
    },
  );

  it('rejects ambiguous metadata and invalid dates before reservation', () => {
    const input = request();
    input.options.metadata = [
      { key: 'same', value: {} },
      { key: 'same', value: {} },
    ];
    expect(() => snapshotPublicationRequest(input)).toThrow('Invalid publication metadata');
    input.options.metadata = [];
    input.options.fileCreatedAt = 'not-a-date';
    expect(() => snapshotPublicationRequest(input)).toThrow('Invalid publication date');
  });
});
