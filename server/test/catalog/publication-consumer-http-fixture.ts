import { APP_GUARD, APP_PIPE } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { Kysely } from 'kysely';
import { ZodValidationPipe } from 'nestjs-zod';
import request from 'supertest';
import { AssetMediaController } from 'src/controllers/asset-media.controller.js';
import { UserController } from 'src/controllers/user.controller.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { AssetUploadInterceptor } from 'src/middleware/asset-upload.interceptor.js';
import { AuthGuard } from 'src/middleware/auth.guard.js';
import { FileUploadInterceptor } from 'src/middleware/file-upload.interceptor.js';
import { AssetOriginalRepository } from 'src/repositories/asset-original.repository.js';
import { AssetRepository } from 'src/repositories/asset.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { StorageRepository } from 'src/repositories/storage.repository.js';
import { DB } from 'src/schema/index.js';
import { AssetMediaService } from 'src/services/asset-media.service.js';
import { AuthService } from 'src/services/auth.service.js';
import { OriginalReader } from 'src/services/original-reader.js';
import { UploadPublicationHttpAdapter } from 'src/services/upload-publication-http.adapter.js';
import { createManagedUpload } from 'src/services/upload-publication-http.factory.js';
import { UserService } from 'src/services/user.service.js';
import { publicationFixture } from 'test/catalog/upload-publication-fixture.js';
import { newTestService } from 'test/utils.js';

export const setupConsumerHttp = async (db: Kysely<DB>) => {
  const f = await publicationFixture(db);
  const { sut, mocks } = newTestService(AssetMediaService);
  const adapter = (await createManagedUpload(
    {
      IMMICH_OBJECT_UPLOAD_ENABLED: 'true',
      IMMICH_OBJECT_SOURCE_ROOT: f.root,
      IMMICH_OBJECT_CATALOG_ROOT: f.stageRoot,
    },
    db,
    { get: () => f.binding.store, uploadBinding: f.binding },
  ))!;
  const originals = new OriginalReader(new AssetOriginalRepository(db), { get: () => f.binding.store });
  Object.assign(sut, { managedUpload: adapter, originalReader: originals });
  mocks.asset.getForOriginal.mockImplementation((id, edited) => new AssetRepository(db).getForOriginal(id, edited));
  mocks.access.asset.checkOwnerAccess.mockImplementation((_owner, ids) => Promise.resolve(new Set(ids)));
  const auth = { user: { id: f.ownerId }, session: { id: f.sessionId } } as AuthDto;
  const authenticate = vi.fn().mockResolvedValue(auth);
  const profile = { createProfileImage: vi.fn().mockResolvedValue({ success: true }) };
  const module = await Test.createTestingModule({
    controllers: [AssetMediaController, UserController],
    providers: [
      { provide: UserService, useValue: profile },
      { provide: APP_GUARD, useClass: AuthGuard },
      { provide: APP_PIPE, useClass: ZodValidationPipe },
      { provide: AuthService, useValue: { authenticate } },
      { provide: AssetMediaService, useValue: sut },
      { provide: LoggingRepository, useValue: mocks.logger },
      { provide: StorageRepository, useValue: mocks.storage },
      { provide: UploadPublicationHttpAdapter, useValue: adapter },
      AssetUploadInterceptor,
      FileUploadInterceptor,
    ],
  }).compile();
  const app = module.createNestApplication();
  app.useLogger(false);
  await app.init();
  const post = (bytes = 'actual multipart bytes', key?: string) => {
    const upload = request(app.getHttpServer()).post('/assets');
    if (key) {
      upload.set('x-immich-upload-idempotency-key', key);
    }
    return upload
      .field('fileCreatedAt', '2020-01-01T00:00:00Z')
      .field('fileModifiedAt', '2020-01-01T00:00:00Z')
      .attach('assetData', Buffer.from(bytes), 'photo.jpg');
  };
  return {
    f,
    post,
    close: async () => {
      await app.close();
      await f.close();
    },
  };
};
