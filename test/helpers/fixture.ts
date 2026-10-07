import { SampleChainApplication } from '../../src/app.js';
import { InMemoryEvidenceFileAdapter } from '../../src/services/evidence-file-adapter.js';
import type { AnalysisBatch, InstrumentRun, ResultRevision, SampleContainer, SamplingRecord } from '../../src/domain/types.js';

export interface ProjectFixture {
  app: SampleChainApplication;
  files: InMemoryEvidenceFileAdapter;
  tenantId: string;
  projectId: string;
  sample: SamplingRecord;
  container: SampleContainer;
  aliquotIds: string[];
  batch: AnalysisBatch;
  run: InstrumentRun;
  result: ResultRevision;
}

let counter = 0;

/**
 * Build one fully approved project: sealed sample -> transfer -> receipt ->
 * aliquot -> batch with a blank control -> run -> analyte + blank results,
 * each quality-approved, batch APPROVED. Pass an existing app to build an
 * additional project inside the same store (used for isolation tests).
 */
export async function buildApprovedProject(opts: { app?: SampleChainApplication; tenantId?: string; projectId?: string } = {}): Promise<ProjectFixture> {
  counter += 1;
  const app = opts.app ?? new SampleChainApplication();
  const files = app.evidence.files as InMemoryEvidenceFileAdapter;
  const tenantId = opts.tenantId ?? `tenant_${counter}`;
  const projectId = opts.projectId ?? `project_${counter}`;
  const suffix = counter.toString();

  const sealed = app.sampling.createSealedSample({
    tenantId,
    projectId,
    collectorId: 'collector_01',
    protocolVersion: 'water-v3',
    medium: 'WATER',
    collectedAt: '2026-10-06T08:00:00.000Z',
    location: { latitude: 31.23, longitude: 121.47, siteCode: `SITE-${suffix}` },
    preservation: { temperatureCelsius: 4, method: 'cooled', maxTransitHours: 48 },
    clientDeviceId: `device_${suffix}`,
    clientSequence: 1,
    containerBarcode: `CNT-${suffix}`,
    sealCode: `SEAL-${suffix}`,
    totalVolumeMl: 100,
  });

  const transfer = app.custody.handOver({
    tenantId, projectId, containerId: sealed.container.id,
    fromHolderId: 'collector_01', toHolderId: 'courier_01',
    fromLocation: `SITE-${suffix}`, toLocation: `LAB-${suffix}`,
    handedOverAt: '2026-10-06T08:30:00.000Z', temperatureCelsius: 5,
    sealStatus: 'INTACT', clientOperationId: `handover-${suffix}`,
  });
  app.custody.confirmReceipt(transfer.id, { receiverId: 'courier_01', receivedAt: '2026-10-06T10:00:00.000Z', temperatureCelsius: 5, sealStatus: 'INTACT' });
  app.receiving.receive({
    tenantId, projectId, containerId: sealed.container.id,
    receivedBy: 'lab_receiver_01', receivedAt: '2026-10-06T10:15:00.000Z',
    temperatureCelsius: 5, sealStatus: 'INTACT', decision: 'ACCEPTED',
    clientOperationId: `receive-${suffix}`,
  });

  const aliquots = app.aliquots.createMany({
    tenantId, projectId, parentContainerId: sealed.container.id,
    protocolVersion: 'water-v3', createdBy: 'analyst_01',
    operationId: `aliquot-${suffix}`,
    items: [
      { barcode: `ALQ-${suffix}-1`, volumeMl: 20, unit: 'ML' },
      { barcode: `ALQ-${suffix}-2`, volumeMl: 20, unit: 'ML' },
    ],
  });

  const batch = app.batches.create({
    tenantId, projectId, protocolVersion: 'water-v3', instrumentType: 'ICP-MS',
    requiredControls: [{ kind: 'BLANK', minimumCount: 1 }], createdBy: 'analyst_01',
  });
  for (const aliquot of aliquots) app.batches.addAliquot(batch.id, aliquot.id);
  app.batches.markReady(batch.id);
  app.batches.start(batch.id, '2026-10-06T11:00:00.000Z');

  const run = app.results.registerRun({
    tenantId, projectId, batchId: batch.id, externalRunId: `RUN-${suffix}`,
    rawFileHash: `raw-${suffix}`, instrumentSoftwareVersion: 'instrument-7.2',
    startedAt: '2026-10-06T11:10:00.000Z',
  });
  app.results.completeRun(run.id, '2026-10-06T12:00:00.000Z', 'COMPLETED');

  const analyte = app.results.ingestRevision({
    tenantId, projectId, batchId: batch.id, runId: run.id, aliquotId: aliquots[0]!.id,
    analyte: 'lead', value: 0.12, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: `result-${suffix}-1`,
  });
  const blank = app.results.ingestRevision({
    tenantId, projectId, batchId: batch.id, runId: run.id, controlKind: 'BLANK',
    analyte: 'lead', value: 0, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: `blank-${suffix}`,
  });
  for (const result of [analyte, blank]) {
    app.quality.decide({
      tenantId, projectId, batchId: batch.id, resultId: result.id,
      decision: 'APPROVED', reason: 'within protocol limits',
      ruleVersion: 'quality-v2', reviewerId: 'reviewer_01',
    });
  }
  app.batches.attachControlResult(batch.id, blank.id);
  app.quality.approveBatch(batch.id, 'reviewer_01');

  return {
    app, files, tenantId, projectId,
    sample: sealed.sample, container: sealed.container,
    aliquotIds: aliquots.map((item) => item.id),
    batch, run, result: analyte,
  };
}
