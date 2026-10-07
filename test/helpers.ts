import { SampleChainApplication } from '../src/app.js';
import type { AnalysisBatch, InstrumentRun, ResultRevision, SampleContainer, SamplingRecord } from '../src/domain/types.js';

export interface Fixture {
  app: SampleChainApplication;
  tenantId: string;
  projectId: string;
  sample: SamplingRecord;
  container: SampleContainer;
  batch: AnalysisBatch;
  run: InstrumentRun;
  result: ResultRevision;
  blank: ResultRevision;
}

/** Build a fully approved chain ready for evidence export. */
export function buildApprovedChain(overrides: { app?: SampleChainApplication; tenantId?: string; projectId?: string; index?: number } = {}): Fixture {
  const app = overrides.app ?? new SampleChainApplication();
  const tenantId = overrides.tenantId ?? 'tenant_a';
  const projectId = overrides.projectId ?? 'project_a';
  const index = overrides.index ?? 1;
  const suffix = `${index}`.padStart(3, '0');

  const sealed = app.sampling.createSealedSample({
    tenantId, projectId,
    collectorId: 'collector_01',
    protocolVersion: 'water-v3',
    medium: 'WATER',
    collectedAt: `2026-10-01T08:0${index}:00.000Z`,
    location: { latitude: 31.23, longitude: 121.47, siteCode: `SITE-${suffix}` },
    preservation: { temperatureCelsius: 4, method: 'cooled', maxTransitHours: 48 },
    clientDeviceId: `device-${suffix}`,
    clientSequence: 1,
    containerBarcode: `CNT-${suffix}`,
    sealCode: `SEAL-${suffix}`,
    totalVolumeMl: 100,
  });
  const transfer = app.custody.handOver({
    tenantId, projectId,
    containerId: sealed.container.id,
    fromHolderId: 'collector_01',
    toHolderId: 'courier_01',
    fromLocation: `SITE-${suffix}`,
    toLocation: 'LAB-A',
    handedOverAt: '2026-10-01T08:30:00.000Z',
    temperatureCelsius: 5,
    sealStatus: 'INTACT',
    clientOperationId: `handover-${suffix}`,
  });
  app.custody.confirmReceipt(transfer.id, { receiverId: 'courier_01', receivedAt: '2026-10-01T10:00:00.000Z', temperatureCelsius: 5, sealStatus: 'INTACT' });
  app.receiving.receive({
    tenantId, projectId,
    containerId: sealed.container.id,
    receivedBy: 'receiver_01',
    receivedAt: '2026-10-01T10:15:00.000Z',
    temperatureCelsius: 5,
    sealStatus: 'INTACT',
    decision: 'ACCEPTED',
    clientOperationId: `receive-${suffix}`,
  });
  const aliquots = app.aliquots.createMany({
    tenantId, projectId,
    parentContainerId: sealed.container.id,
    protocolVersion: 'water-v3',
    createdBy: 'analyst_01',
    operationId: `aliquot-${suffix}`,
    items: [
      { barcode: `ALQ-${suffix}-1`, volumeMl: 20, unit: 'ML' },
      { barcode: `ALQ-${suffix}-2`, volumeMl: 20, unit: 'ML' },
    ],
  });
  const batch = app.batches.create({
    tenantId, projectId,
    protocolVersion: 'water-v3',
    instrumentType: 'ICP-MS',
    requiredControls: [{ kind: 'BLANK', minimumCount: 1 }],
    createdBy: 'analyst_01',
  });
  for (const aliquot of aliquots) app.batches.addAliquot(batch.id, aliquot.id);
  app.batches.markReady(batch.id);
  app.batches.start(batch.id, '2026-10-01T11:00:00.000Z');

  const run = app.results.registerRun({
    tenantId, projectId, batchId: batch.id,
    externalRunId: `RUN-${suffix}`,
    rawFileHash: `raw-${suffix}`,
    instrumentSoftwareVersion: 'instrument-7.2',
    startedAt: '2026-10-01T11:10:00.000Z',
  });
  app.results.completeRun(run.id, '2026-10-01T12:00:00.000Z', 'COMPLETED');
  const result = app.results.ingestRevision({
    tenantId, projectId, batchId: batch.id, runId: run.id,
    ...(aliquots[0] ? { aliquotId: aliquots[0].id } : {}),
    analyte: 'lead', value: 0.12, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2',
    contentHash: `hash-${suffix}`,
  });

  const blankRun = app.results.registerRun({
    tenantId, projectId, batchId: batch.id,
    externalRunId: `RUN-${suffix}-BLANK`,
    rawFileHash: `raw-${suffix}-blank`,
    instrumentSoftwareVersion: 'instrument-7.2',
    startedAt: '2026-10-01T11:15:00.000Z',
  });
  app.results.completeRun(blankRun.id, '2026-10-01T12:05:00.000Z', 'COMPLETED');
  const blank = app.results.ingestRevision({
    tenantId, projectId, batchId: batch.id, runId: blankRun.id,
    controlKind: 'BLANK',
    analyte: 'lead', value: 0, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2',
    contentHash: `hash-${suffix}-blank`,
  });

  app.quality.decide({ tenantId, projectId, batchId: batch.id, resultId: result.id, decision: 'APPROVED', reason: 'within limits', ruleVersion: 'quality-v2', reviewerId: 'reviewer_01' });
  app.quality.decide({ tenantId, projectId, batchId: batch.id, resultId: blank.id, decision: 'APPROVED', reason: 'blank clean', ruleVersion: 'quality-v2', reviewerId: 'reviewer_01' });
  app.quality.approveBatch(batch.id, 'reviewer_01');

  return { app, tenantId, projectId, sample: sealed.sample, container: sealed.container, batch, run, result, blank };
}
