import { Injectable } from '@nestjs/common';
import { OnEvent } from 'src/decorators.js';
import { ImmichWorker, JobName } from 'src/enum.js';
import { EventRepository } from 'src/repositories/event.repository.js';
import { JobRepository } from 'src/repositories/job.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { Consumer, PublicationConsumerRepository } from 'src/repositories/publication-consumer.repository.js';

@Injectable()
export class PublicationConsumerService {
  private pending?: Promise<void>;
  private timer?: ReturnType<typeof setTimeout>;
  private started = false;
  private stopped = false;

  constructor(
    private readonly consumers: PublicationConsumerRepository,
    private readonly jobs: JobRepository,
    private readonly events: EventRepository,
    private readonly logger: LoggingRepository,
  ) {}

  @OnEvent({ name: 'AppBootstrap', workers: [ImmichWorker.Microservices], priority: 210 })
  onBootstrap() {
    if (process.env.IMMICH_OBJECT_UPLOAD_ENABLED !== 'true' || this.started || this.stopped) {
      return this.pending;
    }
    this.started = true;
    return this.poll();
  }

  private async poll() {
    try {
      await this.tick();
    } catch (error) {
      this.logger.error('Publication consumer dispatch failed', error);
    }
    if (this.stopped) {
      return;
    }

    this.timer = setTimeout(() => {
      void this.poll();
    }, 1000);
    this.timer.unref();
  }

  @OnEvent({ name: 'AppShutdown', workers: [ImmichWorker.Microservices] })
  async onShutdown() {
    this.stopped = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    await this.pending;
  }

  tick() {
    if (this.stopped) {
      return Promise.resolve();
    }
    if (!this.pending) {
      this.pending = this.dispatch().finally(() => {
        this.pending = undefined;
      });
    }
    return this.pending;
  }

  private identity(row: Consumer) {
    return { effectId: row.id, receiptId: row.receiptId, assetId: row.assetId, ownerId: row.ownerId };
  }

  private async dispatch() {
    await this.consumers.discover();
    for (const row of await this.consumers.takeDispatch()) {
      const publication = this.identity(row);
      if (row.kind === 'metadata') {
        await this.jobs.queue({
          name: JobName.AssetExtractMetadata,
          data: { id: row.assetId, source: 'upload', publication },
        });
        continue;
      }
      if (row.kind === 'workflow') {
        await this.consumers.freezeFanout(row.id);
        continue;
      }
      const claim = await this.consumers.claim(row.id, 300_000);
      if (!claim.acquired) {
        continue;
      }
      if (row.kind === 'event') {
        await this.events.emit('AssetCreate', { asset: { id: row.assetId, ownerId: row.ownerId }, publication });
        await this.consumers.finish(row.id, claim.token!, { delivered: true, semantics: 'at-least-once' });
      } else {
        await this.events.emit('AssetMetadataExtracted', {
          assetId: row.assetId,
          userId: row.ownerId,
          source: 'upload',
          publication,
        });
        await this.consumers.freezeFanout(row.id, claim.token!);
      }
    }
    for (const run of await this.consumers.takeRunDispatch()) {
      await this.jobs.queue({
        name: JobName.WorkflowAssetTrigger,
        data: {
          workflowId: run.workflowId,
          assetId: run.assetId,
          publication: this.identity(run),
          publicationRunId: run.runId,
        },
      });
    }
    await this.consumers.acknowledgeCompleted();
  }
}
