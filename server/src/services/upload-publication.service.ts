import { ObjectCatalogRepository } from 'src/repositories/object-catalog.repository.js';
import { UploadPublicationRepository } from 'src/repositories/upload-publication.repository.js';
import { ObjectCatalogService } from 'src/services/object-catalog.service.js';
import {
  PublicationHeld,
  PublicationRequest,
  PublicationStoreBinding,
  snapshotPublicationRequest,
} from 'src/utils/upload-publication.js';

export { PublicationHeld } from 'src/utils/upload-publication.js';
export type { PublicationRequest, PublicationStoreBinding } from 'src/utils/upload-publication.js';

export class UploadPublicationService {
  private readonly binding: Readonly<PublicationStoreBinding>;
  constructor(
    private readonly publications: UploadPublicationRepository,
    private readonly catalog: ObjectCatalogRepository,
    private readonly stages: ObjectCatalogService,
    binding: PublicationStoreBinding,
  ) {
    this.binding = Object.freeze({ ...binding });
    if (binding.provider !== 's3' || !binding.storeId || !binding.endpoint || !binding.bucket) {
      throw new Error('A fixed S3 publication binding is required');
    }
  }

  async publish(request: PublicationRequest) {
    const input = snapshotPublicationRequest(request);
    const receipt = await this.publications.reserve(input, this.binding);
    return this.publications
      .withWorker(receipt.id, async (db) => {
        const publications = new UploadPublicationRepository(db);
        const catalog = this.catalog.withDatabase(db);
        const stages = this.stages.withCatalog(catalog);
        let current = await publications.get(receipt.id, input.ownerId);
        if (current.result) {
          return current.result;
        }
        try {
          const stage = await stages.stageOwned(current.sourceId, current.ownerId, current.id);
          const intent = await catalog.prepareOwned(current.id, current.ownerId, this.binding.storeId);
          if (intent.state === 'prepared') {
            await stages.sendPrepared(intent.id, this.binding, 60_000, current.id);
          } else if (intent.state !== 'acknowledged') {
            throw new PublicationHeld(current.id);
          }
          const versions = await catalog.listVersions(stage.id);
          const version = versions.find((value) => value.intentId === intent.id);
          if (!version || !(await stages.verify(stage.id, version.id, this.binding))) {
            throw new PublicationHeld(current.id);
          }
          return await publications.complete(current.id, input.ownerId);
        } catch (error) {
          current = await publications.get(receipt.id, input.ownerId);
          if (current.result) {
            return current.result;
          }
          await publications.hold(receipt.id, input.ownerId);
          if (error instanceof PublicationHeld) {
            throw error;
          }
          throw new PublicationHeld(receipt.id);
        }
      })
      .catch(async () => {
        const persisted = await this.publications.get(receipt.id, input.ownerId).catch(() => {});
        if (persisted?.result) {
          return persisted.result;
        }
        throw new PublicationHeld(receipt.id);
      });
  }
}
