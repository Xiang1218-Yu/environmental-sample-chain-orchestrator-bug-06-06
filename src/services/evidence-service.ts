import type {
  Aliquot, AnalysisBatch, CustodyTransfer, EvidencePackage, InstrumentRun, QualityDecision,
  ReceivingRecord, ResultRevision, SampleContainer, SamplingRecord,
} from '../domain/types.js';
import { assertCondition, assertFound, DomainError } from '../domain/errors.js';
import { stableHash, stableStringify } from '../domain/hash.js';
import { Store } from '../store/store.js';
import { EvidenceFileAdapter, EvidenceFileError, InMemoryEvidenceFileAdapter } from './evidence-file-adapter.js';

export interface GenerateEvidenceInput {
  tenantId: string;
  projectId: string;
  requestKey: string;
  includeRejected?: boolean;
}

interface CapturedEntities {
  samples: SamplingRecord[];
  containers: SampleContainer[];
  transfers: CustodyTransfer[];
  receiving: ReceivingRecord[];
  aliquots: Aliquot[];
  batches: AnalysisBatch[];
  runs: InstrumentRun[];
  results: ResultRevision[];
  quality: QualityDecision[];
}

interface PinnedCapture {
  input: GenerateEvidenceInput;
  entities: CapturedEntities;
  sourceIds: EvidencePackage['sourceIds'];
  transferIds: string[];
  manifest: string;
  manifestHash: string;
  body: string;
  contentHash: string;
  snapshotVersion: number;
  fileKey: string;
}

export class EvidenceService {
  readonly files: EvidenceFileAdapter;
  /**
   * Pinned logical content per package id. The capture is taken once on the
   * first attempt and survives later source-object deletes/mutations, which is
   * what lets a frozen package be verified long after its inputs move on, and
   * makes a client retry after a file failure reproduce the same version.
   */
  private readonly captures = new Map<string, PinnedCapture>();

  constructor(private readonly store: Store, fileAdapter?: EvidenceFileAdapter) {
    this.files = fileAdapter ?? new InMemoryEvidenceFileAdapter();
  }

  async generate(input: GenerateEvidenceInput): Promise<EvidencePackage> {
    // The store mutex is held across the file-adapter await, so two clients
    // racing with the same request key can never each build their own capture.
    return this.store.lock.runExclusive(() => this.generateLocked(input));
  }

  private async generateLocked(input: GenerateEvidenceInput): Promise<EvidencePackage> {
    this.assertRequestShape(input);
    const existing = this.findRequest(input);

    if (existing) {
      if (existing.status === 'GENERATING') this.markFailed(existing, 'evidence.interrupted', 'previous generation was interrupted');
      if (existing.status === 'FAILED') return this.retryFailed(existing, input);
      this.assertSameFilter(existing, input);
      // READY / FROZEN: verify before returning; never regenerate, never
      // silently hand back a package whose file has disappeared.
      return this.verifyPublished(existing);
    }

    this.assertLimits(input);
    const packageId = this.store.nextId('evidence');
    const record: EvidencePackage = {
      id: packageId,
      tenantId: input.tenantId,
      projectId: input.projectId,
      requestKey: input.requestKey,
      status: 'GENERATING',
      snapshotVersion: 0,
      sourceIds: { sampleIds: [], containerIds: [], batchIds: [], resultIds: [], qualityDecisionIds: [] },
      fileKeys: [],
      version: 1,
      ...(input.includeRejected === undefined ? {} : { includeRejected: input.includeRejected }),
    };
    // Capture BEFORE any await: everything a concurrent revision/review does
    // while the file adapter is in flight lands after the snapshot, never
    // half-in/half-out of the export.
    this.captures.set(packageId, this.buildCapture(input, packageId));
    this.store.evidence.set(packageId, record);
    return this.writeAndPublish(record, this.captures.get(packageId)!);
  }

  private async retryFailed(record: EvidencePackage, input: GenerateEvidenceInput): Promise<EvidencePackage> {
    this.assertSameFilter(record, input);
    let capture = this.captures.get(record.id);
    if (!capture) {
      // Defensive: an old FAILED record without an in-memory pin gets one
      // fresh capture attempt. A pinned failure retries its original content.
      this.assertLimits(input);
      capture = this.buildCapture(input, record.id);
      this.captures.set(record.id, capture);
    }
    delete record.failureReason;
    record.status = 'GENERATING';
    return this.writeAndPublish(record, capture);
  }

  private async writeAndPublish(record: EvidencePackage, capture: PinnedCapture): Promise<EvidencePackage> {
    try {
      // Idempotent, content-addressed write. A timeout here publishes nothing:
      // the index, audit and outbox are all committed afterwards.
      await this.files.put(capture.fileKey, capture.body, capture.contentHash);
    } catch (error) {
      this.markFailed(record, error instanceof EvidenceFileError ? error.code : 'evidence.file_failed',
        `evidence file could not be written: ${error instanceof Error ? error.message : String(error)}`);
      throw error instanceof DomainError ? error : new EvidenceFileError('evidence.file_failed', 'evidence file write failed');
    }

    // Verify the bytes actually stored match the manifest we are about to commit.
    const stored = await this.files.get(capture.fileKey);
    if (stableHash(stored.body) !== capture.contentHash) {
      this.markFailed(record, 'evidence.file_corrupt', 'stored evidence file failed hash verification');
      throw new EvidenceFileError('evidence.file_corrupt', 'stored evidence file failed hash verification', capture.fileKey);
    }

    this.commitFileIndex(capture);
    record.status = 'READY';
    record.snapshotVersion = capture.snapshotVersion;
    record.sourceIds = capture.sourceIds;
    record.manifestHash = capture.manifestHash;
    record.fileKeys = [capture.fileKey];
    delete record.failureReason;
    this.store.evidence.set(record.id, record);
    this.store.addAudit({ tenantId: record.tenantId, projectId: record.projectId, action: 'evidence.generated', entity: 'evidence', entityId: record.id, metadata: { snapshotVersion: capture.snapshotVersion, requestKey: record.requestKey, sourceCount: capture.sourceIds.resultIds.length, manifestHash: capture.manifestHash } });
    this.store.addOutbox({ tenantId: record.tenantId, projectId: record.projectId, topic: 'evidence.ready', aggregateId: record.id, payload: JSON.stringify({ evidenceId: record.id, manifestHash: capture.manifestHash, fileKey: capture.fileKey, snapshotVersion: capture.snapshotVersion }) });
    return record;
  }

  /**
   * Verify a published package against its backing file. READY packages are
   * transparently repaired (the content is pinned and content-addressed);
   * FROZEN packages fail loudly — a frozen export must never change silently.
   */
  private async verifyPublished(record: EvidencePackage): Promise<EvidencePackage> {
    const capture = this.captures.get(record.id);
    const fileKey = record.fileKeys[0];
    assertCondition(capture && fileKey, 'evidence.incomplete', 'evidence package is missing its file reference');
    let stored;
    try {
      stored = await this.files.get(fileKey);
    } catch (error) {
      if (record.status === 'FROZEN') {
        throw new EvidenceFileError('evidence.file_missing', `frozen evidence file is missing: ${fileKey}`, fileKey);
      }
      if (!(error instanceof EvidenceFileError)) throw error;
      return this.repairReady(record, capture, fileKey);
    }
    const expectedHash = fileKey.split('/').pop()?.replace('.json', '') ?? '';
    if (stableHash(stored.body) !== expectedHash || stored.contentHash !== capture.contentHash || record.manifestHash !== capture.manifestHash) {
      if (record.status === 'FROZEN') throw new EvidenceFileError('evidence.file_corrupt', `frozen evidence file failed verification: ${fileKey}`, fileKey);
      return this.repairReady(record, capture, fileKey);
    }
    this.commitFileIndex(capture);
    return record;
  }

  private async repairReady(record: EvidencePackage, capture: PinnedCapture, fileKey: string): Promise<EvidencePackage> {
    await this.files.put(fileKey, capture.body, capture.contentHash);
    this.commitFileIndex(capture);
    this.store.addAudit({ tenantId: record.tenantId, projectId: record.projectId, action: 'evidence.repaired', entity: 'evidence', entityId: record.id, metadata: { fileKey, manifestHash: capture.manifestHash } });
    return record;
  }

  async freeze(evidenceId: string): Promise<EvidencePackage> {
    const record = assertFound(this.store.evidence.get(evidenceId), 'evidence.not_found', 'evidence package not found');
    if (record.status === 'FROZEN') return this.verifyPublished(record);
    if (record.status !== 'READY') throw new DomainError('evidence.not_ready', 'only ready evidence packages can be frozen');
    // Freezing is only allowed when the backing file actually verifies.
    await this.verifyPublished(record);
    record.status = 'FROZEN';
    record.frozenAt = this.store.now();
    record.version += 1;
    this.store.addAudit({ tenantId: record.tenantId, projectId: record.projectId, action: 'evidence.frozen', entity: 'evidence', entityId: record.id, metadata: { manifestHash: record.manifestHash ?? null } });
    return record;
  }

  /** Pinned content hash for a package (test/diagnostics; never regenerated). */
  pinnedContentHash(evidenceId: string): string | undefined {
    return this.captures.get(evidenceId)?.contentHash;
  }

  /** Read back and verify a published package payload (used by consumers/tests). */
  async readPackage(evidenceId: string): Promise<{ record: EvidencePackage; payload: unknown }> {    const record = assertFound(this.store.evidence.get(evidenceId), 'evidence.not_found', 'evidence package not found');
    assertCondition(record.status === 'READY' || record.status === 'FROZEN', 'evidence.not_ready', 'evidence package is not published');
    const verified = await this.verifyPublished(record);
    const fileKey = verified.fileKeys[0]!;
    const stored = await this.files.get(fileKey);
    return { record: verified, payload: JSON.parse(stored.body) as unknown };
  }

  private markFailed(record: EvidencePackage, code: string, reason: string): void {
    record.status = 'FAILED';
    record.failureReason = reason;
    record.fileKeys = [];
    this.store.evidence.set(record.id, record);
    this.store.addAudit({ tenantId: record.tenantId, projectId: record.projectId, action: 'evidence.failed', entity: 'evidence', entityId: record.id, metadata: { reasonCode: code } });
  }

  private commitFileIndex(capture: PinnedCapture): void {
    const existing = this.store.files.get(capture.fileKey);
    if (!existing || existing.status !== 'COMMITTED') {
      this.store.files.set(capture.fileKey, { key: capture.fileKey, contentHash: capture.contentHash, createdAt: this.store.now(), status: 'COMMITTED' });
    }
  }

  private findRequest(input: GenerateEvidenceInput): EvidencePackage | undefined {
    return [...this.store.evidence.values()].find((item) => item.tenantId === input.tenantId && item.projectId === input.projectId && item.requestKey === input.requestKey);
  }

  private assertSameFilter(record: EvidencePackage, input: GenerateEvidenceInput): void {
    assertCondition((record.includeRejected ?? false) === (input.includeRejected ?? false), 'evidence.request_conflict', 'request key was already used with a different includeRejected filter');
  }

  private assertRequestShape(input: GenerateEvidenceInput): void {
    assertCondition(input.tenantId.trim().length > 0, 'evidence.invalid_tenant', 'tenant is required');
    assertCondition(input.projectId.trim().length > 0, 'evidence.invalid_project', 'project is required');
    assertCondition(input.requestKey.trim().length >= 4, 'evidence.invalid_request', 'request key is too short');
    assertCondition(input.includeRejected === undefined || typeof input.includeRejected === 'boolean', 'evidence.invalid_filter', 'invalid rejected filter');
  }

  private assertLimits(input: GenerateEvidenceInput): void {
    const pending = [...this.store.evidence.values()].filter((item) => item.tenantId === input.tenantId && item.projectId === input.projectId && item.status === 'GENERATING');
    assertCondition(pending.length < 4, 'evidence.too_many_pending', 'too many evidence requests are pending');
    const fileCount = [...this.store.files.values()].filter((file) => file.status === 'COMMITTED').length;
    assertCondition(fileCount < 100000, 'evidence.storage_limit', 'evidence storage limit reached');
    const auditCount = this.store.audit.filter((item) => item.projectId === input.projectId).length;
    assertCondition(auditCount < 1000000, 'evidence.audit_limit', 'audit history is too large to export');
  }

  /**
   * Take one consistent read of every entity the export depends on. All
   * filtering and referential checks happen here, before the file write, so
   * the resulting payload is a point-in-time snapshot and cannot mix data
   * mutated later in the request.
   */
  private buildCapture(input: GenerateEvidenceInput, packageId: string): PinnedCapture {
    const includeRejected = input.includeRejected ?? false;
    const inScope = (entity: { tenantId: string; projectId: string }): boolean => entity.tenantId === input.tenantId && entity.projectId === input.projectId;

    const samples = [...this.store.samples.values()].filter(inScope).sort(byId);
    const sampleIds = new Set(samples.map((item) => item.id));
    const containers = [...this.store.containers.values()].filter(inScope).sort(byId);
    for (const container of containers) {
      assertCondition(sampleIds.has(container.sampleId), 'evidence.integrity_violation', `container ${container.id} references a sample outside the export scope`);
    }
    const containerIds = new Set(containers.map((item) => item.id));

    const transfers = [...this.store.transfers.values()].filter(inScope).sort(bySequence);
    for (const transfer of transfers) {
      assertCondition(containerIds.has(transfer.containerId), 'evidence.integrity_violation', `transfer ${transfer.id} references a container outside the export scope`);
    }
    const receiving = [...this.store.receiving.values()].filter(inScope).sort(byId);
    for (const record of receiving) {
      assertCondition(containerIds.has(record.containerId), 'evidence.integrity_violation', `receiving record ${record.id} references a container outside the export scope`);
    }
    const aliquots = [...this.store.aliquots.values()].filter(inScope).sort(byId);
    for (const aliquot of aliquots) {
      assertCondition(containerIds.has(aliquot.parentContainerId), 'evidence.integrity_violation', `aliquot ${aliquot.id} references a container outside the export scope`);
    }
    const aliquotIds = new Set(aliquots.map((item) => item.id));

    const batches = [...this.store.batches.values()].filter((batch) => inScope(batch) && (includeRejected || batch.status === 'APPROVED')).sort(byId);
    const batchIds = new Set(batches.map((item) => item.id));
    for (const batch of batches) {
      for (const memberId of batch.memberAliquotIds) {
        assertCondition(aliquotIds.has(memberId), 'evidence.integrity_violation', `batch ${batch.id} references aliquot ${memberId} outside the export scope`);
      }
    }

    const runs = [...this.store.runs.values()].filter((run) => inScope(run) && batchIds.has(run.batchId)).sort(byId);
    const runIds = new Set(runs.map((item) => item.id));

    const currentResults = [...this.store.results.values()].filter((result) => inScope(result) && batchIds.has(result.batchId) && result.status === 'CURRENT').sort(byId);
    for (const result of currentResults) {
      assertCondition(runIds.has(result.runId), 'evidence.integrity_violation', `result ${result.id} references a run outside the export scope`);
    }
    // Default export admits only results carrying an APPROVED quality
    // conclusion. A revision ingested after batch approval is filtered out
    // until its own review lands. includeRejected bypasses the gate.
    const approvedResultIds = new Set(
      includeRejected
        ? currentResults.map((result) => result.id)
        : [...this.store.quality.values()]
            .filter((decision) => inScope(decision) && decision.decision === 'APPROVED' && batchIds.has(decision.batchId))
            .map((decision) => decision.resultId),
    );
    const results = currentResults.filter((result) => approvedResultIds.has(result.id));
    const resultIds = new Set(results.map((item) => item.id));

    const quality = [...this.store.quality.values()]
      .filter((decision) => inScope(decision) && batchIds.has(decision.batchId) && resultIds.has(decision.resultId))
      .sort(byId);
    for (const decision of quality) {
      const result = results.find((item) => item.id === decision.resultId);
      assertCondition(result && result.batchId === decision.batchId, 'evidence.integrity_violation', `quality decision ${decision.id} references a result outside the export scope`);
    }

    assertCondition(samples.length > 0, 'evidence.no_samples', 'project has no samples to export');

    const sourceIds: EvidencePackage['sourceIds'] = {
      sampleIds: samples.map((item) => item.id),
      containerIds: containers.map((item) => item.id),
      batchIds: batches.map((item) => item.id),
      resultIds: results.map((item) => item.id),
      qualityDecisionIds: quality.map((item) => item.id),
    };
    const transferIds = transfers.map((item) => item.id);
    const entities: CapturedEntities = { samples, containers, transfers, receiving, aliquots, batches, runs, results, quality };
    const manifest = stableStringify({ sourceIds, transferIds, includeRejected });
    const manifestHash = stableHash(manifest);
    const body = stableStringify({
      schemaVersion: 1,
      tenantId: input.tenantId,
      projectId: input.projectId,
      requestKey: input.requestKey,
      manifest: { sourceIds, transferIds, includeRejected },
      entities,
    });
    const contentHash = stableHash(body);
    const snapshotVersion = parseInt(contentHash, 16);
    const fileKey = `evidence/${input.projectId}/${packageId}/${contentHash}.json`;
    return {
      input,
      entities: structuredClone(entities),
      sourceIds: structuredClone(sourceIds),
      transferIds,
      manifest,
      manifestHash,
      body,
      contentHash,
      snapshotVersion,
      fileKey,
    };
  }
}

function byId<T extends { id: string }>(a: T, b: T): number {
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

function bySequence<T extends { transferSequence: number }>(a: T, b: T): number {
  return a.transferSequence - b.transferSequence;
}
