import { Injectable } from '@nestjs/common';
import { Kysely } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { randomUUID } from 'node:crypto';
import { DB } from 'src/schema/index.js';

@Injectable()
export class AssetOriginalRepository {
  constructor(@InjectKysely() private db: Kysely<DB>) {}

  async registerStore(input: { storeId: string; endpoint: string; bucket: string }) {
    input = Object.freeze({ storeId: input.storeId, endpoint: input.endpoint, bucket: input.bucket });
    await this.db.transaction().execute(async (db) => {
      await db
        .insertInto('original_store_identity')
        .values(input)
        .onConflict((conflict) => conflict.doNothing())
        .execute();
      const stored = await db
        .selectFrom('original_store_identity')
        .selectAll()
        .where('storeId', '=', input.storeId)
        .executeTakeFirstOrThrow();
      if (stored.endpoint !== input.endpoint || stored.bucket !== input.bucket) {
        throw new Error('Immutable store identity mismatch');
      }
    });
  }

  // Trusted coordinator only: authenticate owner and establish provenance of this newly sealed stage first.
  // Existing catalog stages are NOT owner-aware. Never adopt a preexisting upload/version as a claim.
  async registerClaim(input: { assetId: string; ownerId: string; stageId: string }) {
    input = Object.freeze({ assetId: input.assetId, ownerId: input.ownerId, stageId: input.stageId });
    return this.db.transaction().execute(async (db) => {
      const stage = await db
        .selectFrom('staged_object')
        .selectAll()
        .where('id', '=', input.stageId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const intent = await db
        .selectFrom('object_upload_intent')
        .select('id')
        .where('stageId', '=', stage.id)
        .executeTakeFirst();
      const owned = await db
        .selectFrom('owned_stage_receipt')
        .select('stageId')
        .where('stageId', '=', stage.id)
        .executeTakeFirst();
      if (stage.state !== 'ready' || stage.released || intent || owned) {
        throw new Error('Stage is not a fresh owned claim');
      }
      return db
        .insertInto('asset_original_claim')
        .values({ id: randomUUID(), ...input })
        .returningAll()
        .executeTakeFirstOrThrow();
    });
  }

  async bind(input: { claimId: string; catalogVersionId: string }) {
    input = Object.freeze({ claimId: input.claimId, catalogVersionId: input.catalogVersionId });
    await this.db.transaction().execute(async (db) => {
      const claim = await db
        .selectFrom('asset_original_claim')
        .selectAll()
        .where('id', '=', input.claimId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const asset = await db
        .selectFrom('asset')
        .select(['id', 'originalAuthority'])
        .where('id', '=', claim.assetId)
        .where('ownerId', '=', claim.ownerId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      if (asset.originalAuthority !== 'local') {
        throw new Error('Original already published');
      }
      const version = await db
        .selectFrom('object_version')
        .innerJoin('object_upload_intent', 'object_upload_intent.id', 'object_version.intentId')
        .selectAll('object_version')
        .where('object_version.id', '=', input.catalogVersionId)
        .where('object_upload_intent.stageId', '=', claim.stageId)
        .where('object_upload_intent.state', '=', 'acknowledged')
        .where('object_version.verification', '=', 'verified')
        .where('object_version.provider', '=', 's3')
        .forUpdate('object_version')
        .executeTakeFirstOrThrow();
      const stage = await db
        .selectFrom('staged_object')
        .selectAll()
        .where('id', '=', claim.stageId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      if (!stage.hash || stage.size === null || !version.versionId) {
        throw new Error('Incomplete publication');
      }
      const store = await db
        .selectFrom('original_store_identity')
        .selectAll()
        .where('storeId', '=', version.storeId)
        .executeTakeFirstOrThrow();
      await db
        .insertInto('asset_original')
        .values({
          endpoint: store.endpoint,
          bucket: store.bucket,
          assetId: claim.assetId,
          ownerId: claim.ownerId,
          stageId: claim.stageId,
          claimId: claim.id,
          intentId: version.intentId,
          catalogVersionId: version.id,
          provider: 's3',
          storeId: version.storeId,
          key: version.key,
          versionId: version.versionId,
          hash: stage.hash,
          size: stage.size,
        })
        .execute();
      await db.updateTable('asset').set({ originalAuthority: 'object' }).where('id', '=', asset.id).execute();
    });
  }

  async resolve(assetId: string) {
    const asset = await this.db
      .selectFrom('asset')
      .select('originalAuthority')
      .where('id', '=', assetId)
      .executeTakeFirstOrThrow();
    if (asset.originalAuthority === 'local') {
      return { authority: 'local' as const };
    }
    const mapping = await this.db
      .selectFrom('asset_original')
      .innerJoin('object_version', 'object_version.id', 'asset_original.catalogVersionId')
      .innerJoin('staged_object', 'staged_object.id', 'asset_original.stageId')
      .select([
        'object_version.provider',
        'object_version.storeId',
        'object_version.key',
        'object_version.versionId',
        'object_version.verification',
        'staged_object.size',
        'staged_object.hash',
        'asset_original.publishedAt',
      ])
      .where('asset_original.assetId', '=', assetId)
      .executeTakeFirst();
    return { authority: 'object' as const, mapping };
  }
}
