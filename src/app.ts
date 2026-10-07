import { AliquotService } from './services/aliquot-service.js';
import { BatchService } from './services/batch-service.js';
import { CustodyService } from './services/custody-service.js';
import { EvidenceFileAdapter } from './services/evidence-file-adapter.js';
import { EvidenceService } from './services/evidence-service.js';
import { QualityService } from './services/quality-service.js';
import { ReceivingService } from './services/receiving-service.js';
import { ResultService } from './services/result-service.js';
import { SamplingService } from './services/sampling-service.js';
import { SyncService } from './services/sync-service.js';
import { Store } from './store/store.js';

export class SampleChainApplication {
  readonly store = new Store();
  readonly sampling: SamplingService;
  readonly custody: CustodyService;
  readonly receiving: ReceivingService;
  readonly aliquots: AliquotService;
  readonly batches: BatchService;
  readonly results: ResultService;
  readonly quality: QualityService;
  readonly evidence: EvidenceService;
  readonly sync: SyncService;

  constructor(evidenceFileAdapter?: EvidenceFileAdapter) {
    this.sampling = new SamplingService(this.store);
    this.custody = new CustodyService(this.store);
    this.receiving = new ReceivingService(this.store);
    this.aliquots = new AliquotService(this.store);
    this.batches = new BatchService(this.store, this.aliquots);
    this.results = new ResultService(this.store);
    this.quality = new QualityService(this.store);
    this.evidence = new EvidenceService(this.store, evidenceFileAdapter);
    this.sync = new SyncService(this.store, this.sampling, this.custody, this.receiving, this.aliquots);
  }
}
