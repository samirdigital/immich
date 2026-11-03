import { Injectable } from '@nestjs/common';
import { Kysely } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { DB } from 'src/schema/index.js';
import { StageIdentity } from 'src/schema/tables/staged-object.table.js';
import { SourceEvidence, SourceJournal, SourceReservation } from 'src/services/owned-source.service.js';

@Injectable()
export class OwnedSourceRepository implements SourceJournal {
  constructor(@InjectKysely() private db: Kysely<DB>) {}

  create(input: SourceReservation) {
    return this.db
      .insertInto('source_receipt')
      .values({ ...input })
      .returningAll()
      .executeTakeFirstOrThrow();
  }

  get(id: string, ownerId: string) {
    return this.db
      .selectFrom('source_receipt')
      .selectAll()
      .where('id', '=', id)
      .where('ownerId', '=', ownerId)
      .executeTakeFirst();
  }

  received(id: string, ownerId: string, evidence: SourceEvidence) {
    return this.db
      .updateTable('source_receipt')
      .set({ ...evidence, state: 'received' })
      .where('id', '=', id)
      .where('ownerId', '=', ownerId)
      .where('state', '=', 'writing')
      .returningAll()
      .executeTakeFirstOrThrow();
  }

  claimSeal(id: string, ownerId: string) {
    return this.db
      .updateTable('source_receipt')
      .set({ state: 'sealing' })
      .where('id', '=', id)
      .where('ownerId', '=', ownerId)
      .where('state', '=', 'received')
      .returningAll()
      .executeTakeFirstOrThrow();
  }

  sealed(id: string, ownerId: string, identity: StageIdentity) {
    return this.db
      .updateTable('source_receipt')
      .set({ state: 'sealed', identity: { ...identity } })
      .where('id', '=', id)
      .where('ownerId', '=', ownerId)
      .where('state', '=', 'sealing')
      .returningAll()
      .executeTakeFirstOrThrow();
  }

  hold(id: string, ownerId: string) {
    return this.db
      .updateTable('source_receipt')
      .set({ state: 'held' })
      .where('id', '=', id)
      .where('ownerId', '=', ownerId)
      .where('state', 'in', ['writing', 'received', 'sealing', 'held'])
      .execute();
  }
}
