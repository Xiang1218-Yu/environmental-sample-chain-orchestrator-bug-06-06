import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { DomainError } from '../src/domain/errors.js';
import type { EvidenceManifest } from '../src/services/evidence-service.js';
import { buildApprovedChain, type Fixture } from './helpers.js';

const REQUEST = 'REQ-0001';

async function expectDomainError<T>(promise: Promise<T>, code: string): Promise<DomainError> {
  try {
    await promise;
  } catch (error) {
    assert.ok(error instanceof DomainError, `expected DomainError, got ${String(error)}`);
    assert.equal((error as DomainError).code, code);
    return error as DomainError;
  }
  throw new Error(`expected code ${code} but the call succeeded`);
}

function parseArtifact(fixture: Fixture, evidenceId: string): EvidenceManifest {
  return JSON.parse(fixture.app.evidence.getArtifact(evidenceId)) as EvidenceManifest;
}

function onlyPackage(fixture: Fixture) {
  const packages = [...fixture.app.store.evidence.values()];
  assert.equal(packages.length, 1);
  return packages[0] as NonNullable<(typeof packages)[number]>;
}

describe('evidence generation - happy path and idempotency', () => {
  it('produces a ready package with a committed file, manifest hash and snapshot version', async () => {
    const fixture = buildApprovedChain();
    const pkg = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });

    assert.equal(pkg.status, 'READY');
    assert.equal(pkg.version, 1);
    assert.ok(pkg.manifestHash);
    assert.equal(pkg.fileKeys[0], `evidence/${fixture.projectId}/${REQUEST}/${pkg.manifestHash}.json`);
    const file = fixture.app.store.files.get(pkg.fileKeys[0] as string);
    assert.equal(file?.status, 'COMMITTED');
    assert.equal(file?.contentHash, pkg.manifestHash);
    assert.ok((file?.sizeBytes ?? 0) > 0);

    assert.deepEqual(pkg.sourceIds.sampleIds, [fixture.sample.id]);
    assert.deepEqual(pkg.sourceIds.containerIds, [fixture.container.id]);
    assert.deepEqual(pkg.sourceIds.batchIds, [fixture.batch.id]);
    assert.deepEqual(pkg.sourceIds.resultIds.sort(), [fixture.blank.id, fixture.result.id].sort());
    assert.equal(pkg.sourceIds.transferIds.length, 1);
    assert.equal(pkg.sourceIds.receivingIds.length, 1);
    assert.equal(pkg.sourceIds.aliquotIds.length, 2);
    assert.equal(pkg.sourceIds.runIds.length, 2);
    assert.equal(pkg.sourceIds.qualityDecisionIds.length, 2);
    assert.ok(pkg.snapshotVersion > 0);

    const verification = await fixture.app.evidence.verify(pkg.id);
    assert.equal(verification.healthy, true);
  });

  it('returns the same logical package for a repeated request and emits the ready event once', async () => {
    const fixture = buildApprovedChain();
    const first = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    const second = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });

    assert.equal(second.id, first.id);
    assert.equal(second.manifestHash, first.manifestHash);
    assert.equal(second.snapshotVersion, first.snapshotVersion);
    assert.equal(second.version, 1);
    assert.equal(fixture.app.evidenceFiles.objectCount(), 1);
    assert.equal(fixture.app.store.outbox.filter((message) => message.topic === 'evidence.ready').length, 1);
    assert.equal(fixture.app.store.audit.filter((record) => record.action === 'evidence.generated').length, 1);
  });

  it('freeze verifies files, bumps version once and is idempotent', async () => {
    const fixture = buildApprovedChain();
    const pkg = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    const frozen = await fixture.app.evidence.freeze(pkg.id);
    const frozenAgain = await fixture.app.evidence.freeze(pkg.id);

    assert.equal(frozen.status, 'FROZEN');
    assert.equal(frozen.version, 2);
    assert.ok(frozen.frozenAt);
    assert.equal(frozenAgain.version, 2);
    assert.equal(frozenAgain.frozenAt, frozen.frozenAt);
  });
});

describe('evidence generation - concurrency', () => {
  it('dedupes concurrent requests with the same request key onto one artifact', async () => {
    const fixture = buildApprovedChain();
    const input = { tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST };

    const [first, second, third] = await Promise.all([
      fixture.app.evidence.generate(input),
      fixture.app.evidence.generate(input),
      fixture.app.evidence.generate(input),
    ]);

    assert.equal(second.id, first.id);
    assert.equal(third.id, first.id);
    assert.equal(fixture.app.evidenceFiles.objectCount(), 1);
    assert.equal(fixture.app.store.evidence.size, 1);
    assert.equal(fixture.app.store.outbox.filter((message) => message.topic === 'evidence.ready').length, 1);
  });

  it('captures one consistent snapshot when a result revision lands while the upload is in flight', async () => {
    const fixture = buildApprovedChain();
    let interleaved = false;
    fixture.app.evidenceFiles.onBeforePut(() => {
      if (interleaved) return;
      interleaved = true;
      fixture.app.results.ingestRevision({
        tenantId: fixture.tenantId,
        projectId: fixture.projectId,
        batchId: fixture.batch.id,
        runId: fixture.run.id,
        ...(fixture.result.aliquotId ? { aliquotId: fixture.result.aliquotId } : {}),
        analyte: 'lead', value: 0.15, unit: 'mg/L', detectionLimit: 0.01,
        qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2',
        contentHash: 'hash-changed-during-upload',
      });
    });

    const pkg = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    const artifact = parseArtifact(fixture, pkg.id);

    const exportedLead = artifact.sources.results.find((result) => result.id === fixture.result.id);
    assert.equal(exportedLead?.revisionNumber, 1);
    assert.equal(exportedLead?.contentHash, 'hash-001');
    assert.equal(exportedLead?.status, 'CURRENT');
    assert.equal(artifact.sources.results.length, 2);

    const liveCurrent = [...fixture.app.store.results.values()]
      .filter((result) => result.batchId === fixture.batch.id && result.analyte === 'lead' && result.status === 'CURRENT' && !result.controlKind);
    assert.equal(liveCurrent[0]?.revisionNumber, 2);

    // Repeated request still serves the frozen revision 1 snapshot.
    const repeated = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    assert.equal(repeated.manifestHash, pkg.manifestHash);
  });
});

describe('evidence generation - file failures and safe retries', () => {
  it('leaves only a FAILED marker when the adapter times out before storing, then retries to a healthy package', async () => {
    const fixture = buildApprovedChain();
    fixture.app.evidenceFiles.failPutPrefix(`evidence/${fixture.projectId}/${REQUEST}/`);

    await expectDomainError(
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST }),
      'evidence.file_timeout',
    );

    const marker = onlyPackage(fixture);
    assert.equal(marker.status, 'FAILED');
    assert.ok(marker.failureReason?.includes('timed out'));
    assert.equal(marker.attempts, 1);
    assert.deepEqual(marker.fileKeys, []);
    assert.equal([...fixture.app.store.files.values()].filter((file) => file.status === 'COMMITTED').length, 0);
    assert.equal(fixture.app.evidenceFiles.objectCount(), 0);

    fixture.app.evidenceFiles.clearFailures();
    const retried = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    assert.equal(retried.status, 'READY');
    assert.equal(retried.id, marker.id);
    assert.equal(retried.attempts, 2);
    assert.equal(fixture.app.evidenceFiles.objectCount(), 1);
    assert.equal((await fixture.app.evidence.verify(retried.id)).healthy, true);
  });

  it('an ambiguous timeout after the object was stored is resolved idempotently by the client retry', async () => {
    const fixture = buildApprovedChain();
    fixture.app.evidenceFiles.timeoutAfterStoreByPrefix(`evidence/${fixture.projectId}/${REQUEST}/`);

    await expectDomainError(
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST }),
      'evidence.file_timeout',
    );
    // The object landed, but no index was committed and no READY package is visible.
    assert.equal(fixture.app.evidenceFiles.objectCount(), 1);
    assert.equal(onlyPackage(fixture).status, 'FAILED');

    const retried = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    assert.equal(retried.status, 'READY');
    assert.equal(fixture.app.evidenceFiles.objectCount(), 1);
    assert.equal(retried.fileKeys[0], fixture.app.evidenceFiles.objectKeys()[0]);
    assert.equal((await fixture.app.evidence.verify(retried.id)).healthy, true);
  });
  it('concurrent callers share one failure and both may retry the same logical request', async () => {
    const fixture = buildApprovedChain();
    fixture.app.evidenceFiles.failPutPrefix(`evidence/${fixture.projectId}/${REQUEST}/`);
    const input = { tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST };

    const outcomes = await Promise.allSettled([fixture.app.evidence.generate(input), fixture.app.evidence.generate(input)]);
    for (const outcome of outcomes) {
      assert.equal(outcome.status, 'rejected');
      if (outcome.status === 'rejected') assert.equal((outcome.reason as DomainError).code, 'evidence.file_timeout');
    }
    assert.equal(onlyPackage(fixture).status, 'FAILED');
    assert.equal(onlyPackage(fixture).attempts, 1);

    fixture.app.evidenceFiles.clearFailures();
    const retried = await fixture.app.evidence.generate(input);
    assert.equal(retried.status, 'READY');
    assert.equal(retried.attempts, 2);
  });
});

describe('evidence generation - frozen packages do not change', () => {
  it('does not silently change after a source result is revised or a new sample arrives', async () => {
    const fixture = buildApprovedChain();
    const pkg = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    await fixture.app.evidence.freeze(pkg.id);

    fixture.app.results.ingestRevision({
      tenantId: fixture.tenantId,
      projectId: fixture.projectId,
      batchId: fixture.batch.id,
      runId: fixture.run.id,
      ...(fixture.result.aliquotId ? { aliquotId: fixture.result.aliquotId } : {}),
      analyte: 'lead', value: 0.18, unit: 'mg/L', detectionLimit: 0.01,
      qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2',
      contentHash: 'hash-revised-after-freeze',
    });
    fixture.app.sampling.createSealedSample({
      tenantId: fixture.tenantId, projectId: fixture.projectId,
      collectorId: 'collector_02',
      protocolVersion: 'water-v3',
      medium: 'WATER',
      collectedAt: '2026-10-02T08:00:00.000Z',
      location: { latitude: 31.0, longitude: 121.0, siteCode: 'SITE-NEW' },
      preservation: { temperatureCelsius: 4, method: 'cooled', maxTransitHours: 48 },
      clientDeviceId: 'device-new',
      clientSequence: 1,
      containerBarcode: 'CNT-NEW',
      sealCode: 'SEAL-NEW',
      totalVolumeMl: 50,
    });

    const same = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    assert.equal(same.id, pkg.id);
    assert.equal(same.status, 'FROZEN');
    assert.equal(same.manifestHash, pkg.manifestHash);
    assert.equal(same.snapshotVersion, pkg.snapshotVersion);
    assert.equal(same.version, 2);

    const artifact = parseArtifact(fixture, same.id);
    assert.equal(artifact.sources.samples.length, 1);
    assert.equal(artifact.sources.results.length, 2);
    assert.equal(artifact.sources.results.find((result) => result.id === fixture.result.id)?.revisionNumber, 1);
  });

  it('survives deletion of live source objects and reports an unhealthy package when the artifact object is deleted', async () => {
    const fixture = buildApprovedChain();
    const pkg = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    await fixture.app.evidence.freeze(pkg.id);

    // Delete every live source entity; the artifact content remains intact.
    fixture.app.store.results.clear();
    fixture.app.store.samples.clear();
    fixture.app.store.containers.clear();
    fixture.app.store.batches.clear();
    fixture.app.store.quality.clear();
    fixture.app.store.runs.clear();
    fixture.app.store.aliquots.clear();
    fixture.app.store.transfers.clear();
    fixture.app.store.receiving.clear();

    const artifact = parseArtifact(fixture, pkg.id);
    assert.equal(artifact.sources.samples.length, 1);
    assert.ok(artifact.sources.results.find((result) => result.id === fixture.result.id));
    assert.equal((await fixture.app.evidence.verify(pkg.id)).healthy, true);

    // Deleting the stored object is detected rather than silently ignored.
    await fixture.app.evidenceFiles.remove(pkg.fileKeys[0] as string);
    const verification = await fixture.app.evidence.verify(pkg.id);
    assert.equal(verification.healthy, false);
    assert.equal(verification.files[0]?.presentInAdapter, false);
  });
});

describe('evidence generation - cross project references', () => {
  it('never exports objects that belong to another tenant or project', async () => {
    const fixtureA = buildApprovedChain({ tenantId: 'tenant_a', projectId: 'project_a', index: 1 });
    buildApprovedChain({ tenantId: 'tenant_b', projectId: 'project_b', index: 2 });

    const pkg = await fixtureA.app.evidence.generate({ tenantId: 'tenant_a', projectId: 'project_a', requestKey: REQUEST });
    const artifact = parseArtifact(fixtureA, pkg.id);

    assert.equal(artifact.sources.samples.length, 1);
    assert.equal(artifact.sources.samples[0]?.tenantId, 'tenant_a');
    assert.ok(artifact.sources.results.every((result) => result.tenantId === 'tenant_a' && result.projectId === 'project_a'));
    assert.ok(artifact.sources.quality.every((decision) => decision.tenantId === 'tenant_a'));
    assert.ok(artifact.sources.transfers.every((transfer) => transfer.projectId === 'project_a'));
  });

  it('rejects export when a batch references an aliquot owned by another project', async () => {
    const fixture = buildApprovedChain({ index: 1 });
    const foreign = buildApprovedChain({ app: fixture.app, tenantId: 'tenant_a', projectId: 'project_foreign', index: 9 });

    // Tamper the approved batch so one member points at the foreign project's aliquot.
    const batch = fixture.app.store.batches.get(fixture.batch.id);
    assert.ok(batch);
    const foreignAliquot = [...fixture.app.store.aliquots.values()].find((aliquot) => aliquot.projectId === 'project_foreign');
    assert.ok(foreignAliquot);
    batch.memberAliquotIds.push(foreignAliquot.id);

    await expectDomainError(
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST }),
      'scope.forbidden',
    );
    void foreign;
  });
});

describe('evidence generation - unapproved results are filtered', () => {
  it('excludes non-approved batches and results without an APPROVED quality conclusion', async () => {
    const fixture = buildApprovedChain();

    // A second batch is still running (not approved) and must be absent from the default export.
    const draftAliquots = fixture.app.aliquots.createMany({
      tenantId: fixture.tenantId, projectId: fixture.projectId,
      parentContainerId: fixture.container.id,
      protocolVersion: 'water-v3', createdBy: 'analyst_02', operationId: 'aliquot-draft',
      items: [{ barcode: 'ALQ-DRAFT-1', volumeMl: 10, unit: 'ML' }],
    });
    const draftBatch = fixture.app.batches.create({
      tenantId: fixture.tenantId, projectId: fixture.projectId,
      protocolVersion: 'water-v3', instrumentType: 'GC-MS',
      requiredControls: [], createdBy: 'analyst_02',
    });
    for (const aliquot of draftAliquots) fixture.app.batches.addAliquot(draftBatch.id, aliquot.id);
    fixture.app.batches.markReady(draftBatch.id);
    fixture.app.batches.start(draftBatch.id, '2026-10-01T13:00:00.000Z');
    const draftRun = fixture.app.results.registerRun({
      tenantId: fixture.tenantId, projectId: fixture.projectId, batchId: draftBatch.id,
      externalRunId: 'RUN-DRAFT', rawFileHash: 'raw-draft',
      instrumentSoftwareVersion: 'instrument-8.0', startedAt: '2026-10-01T13:05:00.000Z',
    });
    fixture.app.results.completeRun(draftRun.id, '2026-10-01T13:30:00.000Z', 'COMPLETED');
    fixture.app.results.ingestRevision({
      tenantId: fixture.tenantId, projectId: fixture.projectId, batchId: draftBatch.id, runId: draftRun.id,
      analyte: 'cadmium', value: 0.04, unit: 'mg/L', detectionLimit: 0.01,
      qualityFlags: [], instrumentSoftwareVersion: 'instrument-8.0', contentHash: 'hash-draft',
    });

    // A new revision on the approved batch arrives after approval and has no APPROVED
    // quality conclusion yet (review completed meanwhile) - it must be filtered out.
    const unreviewed = fixture.app.results.ingestRevision({
      tenantId: fixture.tenantId, projectId: fixture.projectId, batchId: fixture.batch.id, runId: fixture.run.id,
      ...(fixture.result.aliquotId ? { aliquotId: fixture.result.aliquotId } : {}),
      analyte: 'mercury', value: 0.002, unit: 'mg/L', detectionLimit: 0.001,
      qualityFlags: ['REVIEW'], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'hash-unreviewed',
    });

    const pkg = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    const artifact = parseArtifact(fixture, pkg.id);

    assert.equal(artifact.sources.batches.length, 1);
    assert.deepEqual(artifact.sources.batches.map((batch) => batch.id), [fixture.batch.id]);
    assert.ok(!artifact.sources.results.some((result) => result.id === unreviewed.id));
    assert.ok(!pkg.sourceIds.resultIds.includes(unreviewed.id));
    assert.ok(artifact.sources.results.every((result) => result.id === fixture.result.id || result.id === fixture.blank.id));

    // includeRejected exports every current result regardless of quality conclusion.
    const wide = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'REQ-WIDE', includeRejected: true });
    const wideArtifact = parseArtifact(fixture, wide.id);
    assert.ok(wideArtifact.sources.results.some((result) => result.id === unreviewed.id));
  });

  it('default export keeps only APPROVED results; includeRejected exposes the retest trail', async () => {
    const fixture = buildApprovedChain();
    const secondAliquot = [...fixture.app.store.aliquots.values()]
      .filter((aliquot) => aliquot.projectId === fixture.projectId)
      .find((aliquot) => aliquot.id !== fixture.result.aliquotId);
    const retest = fixture.app.results.ingestRevision({
      tenantId: fixture.tenantId, projectId: fixture.projectId, batchId: fixture.batch.id, runId: fixture.run.id,
      ...(secondAliquot?.id ? { aliquotId: secondAliquot.id } : {}),
      analyte: 'arsenic', value: 0.5, unit: 'mg/L', detectionLimit: 0.01,
      qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'hash-arsenic',
    });
    fixture.app.quality.decide({
      tenantId: fixture.tenantId, projectId: fixture.projectId, batchId: fixture.batch.id,
      resultId: retest.id, decision: 'RETEST_REQUIRED', reason: 'over limit',
      ruleVersion: 'quality-v2', reviewerId: 'reviewer_02',
    });
    // The retest decision moves the batch back to REJECTED per the quality domain rules.
    assert.equal(fixture.app.store.batches.get(fixture.batch.id)?.status, 'REJECTED');

    // No approved batch anymore: default export contains zero batches.
    const pkg = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    const artifact = parseArtifact(fixture, pkg.id);
    assert.deepEqual(artifact.sources.batches, []);
    assert.deepEqual(artifact.sources.results, []);
    assert.deepEqual(artifact.sources.quality, []);
    assert.equal(artifact.sources.samples.length, 1);

    const wide = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'REQ-WIDE', includeRejected: true });
    const wideArtifact = parseArtifact(fixture, wide.id);
    assert.equal(wideArtifact.sources.batches.length, 1);
    assert.ok(wideArtifact.sources.results.some((result) => result.id === retest.id));
    assert.ok(wideArtifact.sources.quality.some((decision) => decision.resultId === retest.id && decision.decision === 'RETEST_REQUIRED'));
    assert.ok(wideArtifact.sources.quality.some((decision) => decision.resultId === fixture.result.id && decision.decision === 'APPROVED'));
  });
});

describe('evidence generation - edge cases', () => {
  it('a prepare-phase failure leaves no evidence record behind', async () => {
    const fixture = buildApprovedChain();
    await expectDomainError(
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: 'project_empty', requestKey: REQUEST }),
      'evidence.no_samples',
    );
    assert.equal(fixture.app.store.evidence.size, 0);
    assert.equal(fixture.app.evidenceFiles.objectCount(), 0);
    // A later valid request with another key still succeeds.
    const pkg = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    assert.equal(pkg.status, 'READY');
  });

  it('cannot freeze a FAILED marker, but the retried package can be frozen afterwards', async () => {
    const fixture = buildApprovedChain();
    fixture.app.evidenceFiles.failPutPrefix(`evidence/${fixture.projectId}/${REQUEST}/`);
    await assert.rejects(fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST }));
    const marker = onlyPackage(fixture);
    await expectDomainError(fixture.app.evidence.freeze(marker.id), 'evidence.not_ready');

    fixture.app.evidenceFiles.clearFailures();
    const retried = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    const frozen = await fixture.app.evidence.freeze(retried.id);
    assert.equal(frozen.status, 'FROZEN');
    assert.equal(frozen.attempts, 2);
  });

  it('freeze refuses when the file index is missing the artifact', async () => {
    const fixture = buildApprovedChain();
    const pkg = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: REQUEST });
    fixture.app.store.files.delete(pkg.fileKeys[0] as string);
    await expectDomainError(fixture.app.evidence.freeze(pkg.id), 'evidence.file_missing');
  });

  it('different request keys produce independent packages that can be generated concurrently', async () => {
    const fixture = buildApprovedChain();
    const [pkgA, pkgB] = await Promise.all([
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'REQ-A-001' }),
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'REQ-B-002' }),
    ]);
    assert.notEqual(pkgA.id, pkgB.id);
    assert.notEqual(pkgA.manifestHash, pkgB.manifestHash);
    assert.equal(fixture.app.evidenceFiles.objectCount(), 2);
  });
});

describe('evidence generation - request validation', () => {
  it('rejects malformed requests and requests without samples', async () => {
    const fixture = buildApprovedChain();
    await expectDomainError(
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'abc' }),
      'evidence.invalid_request',
    );
    await expectDomainError(
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: 'project_empty', requestKey: REQUEST }),
      'evidence.no_samples',
    );
  });
});
