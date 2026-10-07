import { SampleChainApplication } from '../app.js';
const app = new SampleChainApplication();
const tenantId = 'tenant_demo';
const projectId = 'project_delta';
const sealed = app.sampling.createSealedSample({
    tenantId,
    projectId,
    collectorId: 'collector_01',
    protocolVersion: 'water-v3',
    medium: 'WATER',
    collectedAt: '2026-10-06T08:00:00.000Z',
    location: { latitude: 31.23, longitude: 121.47, siteCode: 'SITE-SH-01' },
    preservation: { temperatureCelsius: 4, method: 'cooled', maxTransitHours: 48 },
    clientDeviceId: 'device_01',
    clientSequence: 1,
    containerBarcode: 'CNT-DEMO-001',
    sealCode: 'SEAL-001',
    totalVolumeMl: 100,
});
const transfer = app.custody.handOver({
    tenantId,
    projectId,
    containerId: sealed.container.id,
    fromHolderId: 'collector_01',
    toHolderId: 'courier_01',
    fromLocation: 'SITE-SH-01',
    toLocation: 'LAB-SH-A',
    handedOverAt: '2026-10-06T08:30:00.000Z',
    temperatureCelsius: 5,
    sealStatus: 'INTACT',
    clientOperationId: 'handover-001',
});
app.custody.confirmReceipt(transfer.id, { receiverId: 'courier_01', receivedAt: '2026-10-06T10:00:00.000Z', temperatureCelsius: 5, sealStatus: 'INTACT' });
app.receiving.receive({ tenantId, projectId, containerId: sealed.container.id, receivedBy: 'lab_receiver_01', receivedAt: '2026-10-06T10:15:00.000Z', temperatureCelsius: 5, sealStatus: 'INTACT', decision: 'ACCEPTED', clientOperationId: 'receive-001' });
const aliquots = app.aliquots.createMany({ tenantId, projectId, parentContainerId: sealed.container.id, protocolVersion: 'water-v3', createdBy: 'analyst_01', operationId: 'aliquot-001', items: [{ barcode: 'ALQ-001', volumeMl: 20, unit: 'ML' }, { barcode: 'ALQ-002', volumeMl: 20, unit: 'ML' }] });
const batch = app.batches.create({ tenantId, projectId, protocolVersion: 'water-v3', instrumentType: 'ICP-MS', requiredControls: [{ kind: 'BLANK', minimumCount: 1 }], createdBy: 'analyst_01' });
for (const aliquot of aliquots)
    app.batches.addAliquot(batch.id, aliquot.id);
app.batches.markReady(batch.id);
app.batches.start(batch.id, '2026-10-06T11:00:00.000Z');
const run = app.results.registerRun({ tenantId, projectId, batchId: batch.id, externalRunId: 'RUN-001', rawFileHash: 'raw-hash-001', instrumentSoftwareVersion: 'instrument-7.2', startedAt: '2026-10-06T11:10:00.000Z' });
app.results.completeRun(run.id, '2026-10-06T12:00:00.000Z', 'COMPLETED');
const result = app.results.ingestRevision({ tenantId, projectId, batchId: batch.id, runId: run.id, ...(aliquots[0] ? { aliquotId: aliquots[0].id } : {}), analyte: 'lead', value: 0.12, unit: 'mg/L', detectionLimit: 0.01, qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'result-hash-001' });
const blankRun = app.results.registerRun({ tenantId, projectId, batchId: batch.id, externalRunId: 'RUN-001-BLANK', rawFileHash: 'raw-hash-blank-001', instrumentSoftwareVersion: 'instrument-7.2', startedAt: '2026-10-06T11:15:00.000Z' });
app.results.completeRun(blankRun.id, '2026-10-06T12:05:00.000Z', 'COMPLETED');
const blank = app.results.ingestRevision({ tenantId, projectId, batchId: batch.id, runId: blankRun.id, controlKind: 'BLANK', analyte: 'lead', value: 0, unit: 'mg/L', detectionLimit: 0.01, qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'result-hash-blank-001' });
app.quality.decide({ tenantId, projectId, batchId: batch.id, resultId: result.id, decision: 'APPROVED', reason: 'within protocol limits', ruleVersion: 'quality-v2', reviewerId: 'reviewer_01' });
app.quality.decide({ tenantId, projectId, batchId: batch.id, resultId: blank.id, decision: 'APPROVED', reason: 'blank below detection limit', ruleVersion: 'quality-v2', reviewerId: 'reviewer_01' });
app.quality.approveBatch(batch.id, 'reviewer_01');
const evidence = await app.evidence.generate({ tenantId, projectId, requestKey: 'REQ-SMOKE-0001' });
const duplicate = await app.evidence.generate({ tenantId, projectId, requestKey: 'REQ-SMOKE-0001' });
const frozen = await app.evidence.freeze(evidence.id);
const verification = await app.evidence.verify(evidence.id);
console.log(JSON.stringify({
    projectId,
    sampleId: sealed.sample.id,
    containerId: sealed.container.id,
    aliquotIds: aliquots.map((item) => item.id),
    batchId: batch.id,
    resultId: result.id,
    evidence: {
        id: evidence.id,
        status: frozen.status,
        snapshotVersion: evidence.snapshotVersion,
        manifestHash: evidence.manifestHash,
        fileKey: evidence.fileKeys[0],
        sameLogicalPackage: duplicate.id === evidence.id && duplicate.manifestHash === evidence.manifestHash,
        healthy: verification.healthy,
        version: frozen.version,
    },
    auditCount: app.store.audit.length,
    outboxCount: app.store.outbox.length,
}, null, 2));
