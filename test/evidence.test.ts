import assert from 'node:assert/strict';
import { test, before, describe } from 'node:test';
import { DomainError } from '../src/domain/errors.js';
import { buildApprovedProject, type ProjectFixture } from './helpers/fixture.js';

let fixture: ProjectFixture;

before(async () => {
  fixture = await buildApprovedProject();
});

function expectError(code: string, fn: () => Promise<unknown> | unknown): Promise<void> | void {
  return assert.rejects(() => Promise.resolve(fn()), (error: unknown) => {
    assert.ok(error instanceof DomainError, `expected DomainError, got ${String(error)}`);
    assert.equal((error as DomainError).code, code);
    return true;
  });
}

// ---------------------------------------------------------------------------
// 1. Concurrent generation: same request key -> one package, one file, one
//    audit row; different keys -> independent consistent snapshots.
// ---------------------------------------------------------------------------
describe('concurrent generation', () => {
  test('same request key racing requests share one logical version', async () => {
    fixture.files.setPutDelay(20);
    const input = { tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-concurrent' };
    const [first, second, third] = await Promise.all([
      fixture.app.evidence.generate(input),
      fixture.app.evidence.generate(input),
      fixture.app.evidence.generate(input),
    ]);
    fixture.files.setPutDelay(0);

    assert.equal(first.id, second.id);
    assert.equal(first.id, third.id);
    assert.equal(first.status, 'READY');
    assert.equal(first.snapshotVersion, second.snapshotVersion);
    assert.deepEqual(first.fileKeys, second.fileKeys);

    const packages = [...fixture.app.store.evidence.values()].filter((pkg) => pkg.requestKey === 'req-concurrent');
    assert.equal(packages.length, 1, 'only one package record for the request key');
    const generatedAudits = fixture.app.store.audit.filter((row) => row.entityId === first.id && row.action === 'evidence.generated');
    assert.equal(generatedAudits.length, 1, 'generation committed exactly once');
    const fileRecord = fixture.app.store.files.get(first.fileKeys[0]!);
    assert.equal(fileRecord?.status, 'COMMITTED');
  });

  test('two request keys generated concurrently get independent packages', async () => {
    fixture.files.setPutDelay(20);
    const [a, b] = await Promise.all([
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-con-a' }),
      fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-con-b' }),
    ]);
    fixture.files.setPutDelay(0);
    assert.notEqual(a.id, b.id);
    assert.notEqual(a.fileKeys[0], b.fileKeys[0]);
    assert.equal(a.status, 'READY');
    assert.equal(b.status, 'READY');
  });
});

// ---------------------------------------------------------------------------
// 2. File adapter failure: first attempt times out -> FAILED record, no file
//    index; client retry with the SAME request key returns a complete package
//    with the SAME logical version that was captured on attempt one.
// ---------------------------------------------------------------------------
test('file failure is safely retryable and keeps the first snapshot version', async () => {
  const input = { tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-file-fail' };
  fixture.files.failNextPutAttempt(1);
  const committedFilesBefore = fixture.app.store.files.size;

  await expectError('evidence.file_timeout', () => fixture.app.evidence.generate(input));

  const failed = [...fixture.app.store.evidence.values()].find((pkg) => pkg.requestKey === 'req-file-fail')!;
  assert.equal(failed.status, 'FAILED');
  assert.equal(failed.fileKeys.length, 0, 'failed attempt must not publish a file reference');
  assert.equal(fixture.app.store.files.size, committedFilesBefore, 'failure must not commit a file index entry');
  assert.equal(fixture.app.store.outbox.filter((msg) => msg.aggregateId === failed.id).length, 0, 'no outbox message on failure');
  const pinnedHash = fixture.app.evidence.pinnedContentHash(failed.id);
  assert.ok(pinnedHash, 'failed attempt still pins its captured content for retry');

  // Meanwhile the underlying result is revised (the "review finishes during
  // generation" case). The retry must NOT pick it up: same logical version.
  const revised = fixture.app.results.ingestRevision({
    tenantId: fixture.tenantId, projectId: fixture.projectId, batchId: fixture.batch.id, runId: fixture.run.id,
    aliquotId: fixture.aliquotIds[0]!, analyte: 'lead', value: 0.15, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'result-revised-after-fail',
  });
  assert.notEqual(revised.id, fixture.result.id);

  const retried = await fixture.app.evidence.generate(input);
  assert.equal(retried.id, failed.id, 'retry reuses the same package id');
  assert.equal(retried.status, 'READY');
  assert.ok(retried.fileKeys[0]!.includes(pinnedHash!), 'retry writes the same content-addressed file (same logical version)');
  assert.equal(fixture.app.evidence.pinnedContentHash(retried.id), pinnedHash);
  assert.ok(retried.fileKeys[0]);
  const { payload } = await fixture.app.evidence.readPackage(retried.id);
  const resultIds = (payload as any).manifest.sourceIds.resultIds as string[];
  assert.ok(resultIds.includes(fixture.result.id), 'retry pins the pre-failure revision');
  assert.ok(!resultIds.includes(revised.id), 'retry must not absorb the revision made after the failed attempt');
});

// ---------------------------------------------------------------------------
// 3. Duplicate request: same key returns the identical logical version; same
//    key with a conflicting filter is rejected.
// ---------------------------------------------------------------------------
test('repeated request returns the same logical version and never regenerates', async () => {
  const input = { tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-dup' };
  const first = await fixture.app.evidence.generate(input);
  const auditsBefore = fixture.app.store.audit.length;
  const again = await fixture.app.evidence.generate(input);
  const onceMore = await fixture.app.evidence.generate({ ...input });

  assert.equal(again.id, first.id);
  assert.equal(onceMore.manifestHash, first.manifestHash);
  assert.equal(onceMore.snapshotVersion, first.snapshotVersion);
  assert.deepEqual(onceMore.fileKeys, first.fileKeys);
  assert.equal(fixture.app.store.audit.length, auditsBefore, 'idempotent replay writes no new audit');
});

test('same request key with a conflicting includeRejected filter is rejected', async () => {
  const key = 'req-filter-conflict';
  await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: key });
  await expectError('evidence.request_conflict', () =>
    fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: key, includeRejected: true }));
});

// ---------------------------------------------------------------------------
// 4. Result changes: a revision that lands while the file adapter is in flight
//    must not mix into the export. Post-generation changes require a new
//    request key and produce a new logical version.
// ---------------------------------------------------------------------------
test('snapshot is consistent even when a revision is ingested mid-generation', async () => {
  // Dedicated project so an unreviewed revision landing during the write is
  // the only state change the capture could wrongly absorb.
  const project = await buildApprovedProject();
  const files = project.files;
  let resolvePut!: () => void;
  const putStarted = new Promise<void>((resolve) => { resolvePut = resolve; });
  let releasePut!: () => void;
  const putMayFinish = new Promise<void>((resolve) => { releasePut = resolve; });
  // Gate the adapter write: capture is pinned first, then we hold the write
  // open while a revision lands, proving the snapshot cannot mix it in.
  const originalPut = files.put.bind(files);
  files.put = (async (key: string, body: string, hash: string) => {
    resolvePut();
    await putMayFinish;
    return originalPut(key, body, hash);
  }) as typeof files.put;

  const generating = project.app.evidence.generate({
    tenantId: project.tenantId, projectId: project.projectId, requestKey: 'req-mid-flight',
  });
  await putStarted;
  // Capture is pinned by now; the write is blocked on putMayFinish.
  const midFlight = project.app.results.ingestRevision({
    tenantId: project.tenantId, projectId: project.projectId, batchId: project.batch.id, runId: project.run.id,
    aliquotId: project.aliquotIds[0]!, analyte: 'lead', value: 0.99, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'result-mid-flight',
  });
  releasePut();
  const pkg = await generating;
  files.put = originalPut;

  const { payload } = await project.app.evidence.readPackage(pkg.id);
  const resultIds = (payload as any).manifest.sourceIds.resultIds as string[];
  assert.ok(!resultIds.includes(midFlight.id), 'mid-flight revision must never appear in a consistent snapshot');
  assert.ok(resultIds.includes(project.result.id), 'pre-flight approved revision is the one captured');
});

test('changes after generation do not alter the existing package; new key versions it', async () => {
  const key = 'req-before-change';
  const before = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: key });
  const { payload: beforePayload } = await fixture.app.evidence.readPackage(before.id);

  // New approved revision after the package was generated.
  const revision2 = fixture.app.results.ingestRevision({
    tenantId: fixture.tenantId, projectId: fixture.projectId, batchId: fixture.batch.id, runId: fixture.run.id,
    aliquotId: fixture.aliquotIds[0]!, analyte: 'lead', value: 0.2, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'result-v2',
  });
  fixture.app.quality.decide({
    tenantId: fixture.tenantId, projectId: fixture.projectId, batchId: fixture.batch.id, resultId: revision2.id,
    decision: 'APPROVED', reason: 'corrected value', ruleVersion: 'quality-v2', reviewerId: 'reviewer_02',
  });

  const replay = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: key });
  assert.equal(replay.manifestHash, before.manifestHash, 'old request key stays on its frozen content');
  const { payload: replayPayload } = await fixture.app.evidence.readPackage(replay.id);
  assert.deepEqual(replayPayload, beforePayload);

  const next = await fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-after-change' });
  assert.notEqual(next.manifestHash, before.manifestHash, 'a new request key produces a new logical version');
  const ids = (await fixture.app.evidence.readPackage(next.id)).payload as any;
  assert.ok(ids.manifest.sourceIds.resultIds.includes(revision2.id));
  assert.ok(!ids.manifest.sourceIds.resultIds.includes(fixture.result.id), 'superseded revision excluded');
});

// ---------------------------------------------------------------------------
// 5. Object deletion: READY packages self-heal from the pinned content;
//    FROZEN packages fail loudly instead of silently changing.
// ---------------------------------------------------------------------------
test('deleted backing object: READY repairs, FROZEN raises', async () => {
  const readyPkg = await fixture.app.evidence.generate({
    tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-delete-ready',
  });
  fixture.files.simulateObjectDeletion(readyPkg.fileKeys[0]!);

  const repaired = await fixture.app.evidence.generate({
    tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-delete-ready',
  });
  assert.equal(repaired.status, 'READY');
  assert.equal(repaired.fileKeys[0], readyPkg.fileKeys[0], 'same content-addressed key restored');
  const { payload } = await fixture.app.evidence.readPackage(repaired.id);
  assert.ok((payload as any).manifest.sourceIds.sampleIds.includes(fixture.sample.id));
  assert.ok(fixture.app.store.audit.some((row) => row.entityId === readyPkg.id && row.action === 'evidence.repaired'));

  const frozenPkg = await fixture.app.evidence.generate({
    tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-delete-frozen',
  });
  await fixture.app.evidence.freeze(frozenPkg.id);
  fixture.files.simulateObjectDeletion(frozenPkg.fileKeys[0]!);

  await expectError('evidence.file_missing', () =>
    fixture.app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-delete-frozen' }));
  await expectError('evidence.file_missing', () => fixture.app.evidence.readPackage(frozenPkg.id));
});

test('frozen package never changes silently and freeze verifies the file', async () => {
  const project = await buildApprovedProject();
  const pkg = await project.app.evidence.generate({
    tenantId: project.tenantId, projectId: project.projectId, requestKey: 'req-freeze',
  });
  const frozen = await project.app.evidence.freeze(pkg.id);
  assert.equal(frozen.status, 'FROZEN');
  assert.equal(frozen.manifestHash, pkg.manifestHash);
  const before = await project.app.evidence.readPackage(pkg.id);

  // Source data moves on after the freeze: a fresh approved revision lands.
  const later = project.app.results.ingestRevision({
    tenantId: project.tenantId, projectId: project.projectId, batchId: project.batch.id, runId: project.run.id,
    aliquotId: project.aliquotIds[0]!, analyte: 'lead', value: 0.7, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'result-post-freeze',
  });
  project.app.quality.decide({
    tenantId: project.tenantId, projectId: project.projectId, batchId: project.batch.id, resultId: later.id,
    decision: 'APPROVED', reason: 'post freeze', ruleVersion: 'quality-v2', reviewerId: 'reviewer_09',
  });

  const again = await project.app.evidence.freeze(pkg.id);
  assert.equal(again.status, 'FROZEN');
  assert.equal(again.manifestHash, pkg.manifestHash, 'freeze is stable');
  const after = await project.app.evidence.readPackage(pkg.id);
  assert.deepEqual(after.payload, before.payload, 'frozen payload does not absorb later changes');
  assert.ok(!(after.payload as any).manifest.sourceIds.resultIds.includes(later.id));
});

// ---------------------------------------------------------------------------
// 6. Cross-project and cross-tenant references are never exported.
// ---------------------------------------------------------------------------
test('export is scoped to tenant + project and never leaks references', async () => {
  // A second project in the SAME store proves scoping does not rely on
  // separate instances, and ids cannot collide.
  const other = await buildApprovedProject({ app: fixture.app, tenantId: fixture.tenantId, projectId: 'project_other' });
  const otherPkg = await other.app.evidence.generate({
    tenantId: other.tenantId, projectId: other.projectId, requestKey: 'req-other',
  });
  const otherPayload = (await other.app.evidence.readPackage(otherPkg.id)).payload as any;
  assert.deepEqual(otherPayload.manifest.sourceIds.sampleIds, [other.sample.id]);
  assert.ok(!otherPayload.manifest.sourceIds.sampleIds.includes(fixture.sample.id));
  assert.ok(!otherPayload.manifest.sourceIds.resultIds.includes(fixture.result.id));
  assert.ok(!otherPayload.manifest.sourceIds.containerIds.includes(fixture.container.id));
  assert.equal(otherPayload.projectId, 'project_other');
  assert.equal(otherPayload.tenantId, fixture.tenantId);

  // Same request key in a different project is an independent package.
  const mine = await fixture.app.evidence.generate({
    tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-other',
  });
  assert.notEqual(mine.id, otherPkg.id);
  assert.equal(mine.projectId, fixture.projectId);

  // Cross-tenant, same store: isolated as well.
  const foreign = await buildApprovedProject({ app: fixture.app, tenantId: 'tenant_foreign', projectId: fixture.projectId });
  const foreignPkg = await foreign.app.evidence.generate({
    tenantId: foreign.tenantId, projectId: foreign.projectId, requestKey: 'req-cross-tenant',
  });
  const foreignPayload = (await foreign.app.evidence.readPackage(foreignPkg.id)).payload as any;
  assert.deepEqual(foreignPayload.manifest.sourceIds.sampleIds, [foreign.sample.id]);
  assert.ok(!foreignPayload.manifest.sourceIds.resultIds.includes(fixture.result.id));
  assert.equal(foreignPayload.tenantId, 'tenant_foreign');
});

test('integrity violation: a result referencing a run outside the scope is refused', async () => {
  // Hand-craft a dangling result whose run id does not exist in this store:
  // the capture referential check must refuse the export.
  const { app } = fixture;
  const corruptId = app.store.nextId('result');
  app.store.results.set(corruptId, {
    id: corruptId,
    tenantId: fixture.tenantId,
    projectId: fixture.projectId,
    batchId: fixture.batch.id,
    runId: 'run_from_another_project', // foreign / missing run
    aliquotId: fixture.aliquotIds[0]!,
    analyte: 'mercury', value: 0.01, unit: 'mg/L', detectionLimit: 0.001,
    qualityFlags: [], instrumentSoftwareVersion: 'x', contentHash: 'corrupt',
    revisionNumber: 1, status: 'CURRENT', createdAt: app.store.now(), version: 1,
  });
  const qualityId = app.store.nextId('quality');
  app.store.quality.set(qualityId, {
    id: qualityId,
    tenantId: fixture.tenantId, projectId: fixture.projectId, batchId: fixture.batch.id,
    resultId: corruptId, decision: 'APPROVED', reason: 'x', ruleVersion: 'v',
    reviewerId: 'r', decidedAt: app.store.now(), version: 1,
  });
  await expectError('evidence.integrity_violation', () =>
    app.evidence.generate({ tenantId: fixture.tenantId, projectId: fixture.projectId, requestKey: 'req-integrity' }));
});

// ---------------------------------------------------------------------------
// 7. Unapproved result filtering: default export omits results without an
//    APPROVED quality decision (including revisions arriving after approval);
//    includeRejected exports everything in scope.
// ---------------------------------------------------------------------------
test('results without APPROVED quality decision are filtered by default', async () => {
  const project = await buildApprovedProject();
  // Revision lands after batch approval, no quality decision yet.
  const unreviewed = project.app.results.ingestRevision({
    tenantId: project.tenantId, projectId: project.projectId, batchId: project.batch.id, runId: project.run.id,
    aliquotId: project.aliquotIds[0]!, analyte: 'lead', value: 0.4, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'result-unreviewed',
  });

  const strict = await project.app.evidence.generate({
    tenantId: project.tenantId, projectId: project.projectId, requestKey: 'req-strict',
  });
  const strictPayload = (await project.app.evidence.readPackage(strict.id)).payload as any;
  assert.ok(!strictPayload.manifest.sourceIds.resultIds.includes(unreviewed.id), 'unreviewed current revision excluded');
  assert.ok(!strictPayload.manifest.sourceIds.resultIds.includes(project.result.id), 'its superseded predecessor also excluded');

  // Once the revision is approved it is included.
  project.app.quality.decide({
    tenantId: project.tenantId, projectId: project.projectId, batchId: project.batch.id, resultId: unreviewed.id,
    decision: 'APPROVED', reason: 'ok', ruleVersion: 'quality-v2', reviewerId: 'reviewer_01',
  });
  const strict2 = await project.app.evidence.generate({
    tenantId: project.tenantId, projectId: project.projectId, requestKey: 'req-strict-approved',
  });
  const strict2Payload = (await project.app.evidence.readPackage(strict2.id)).payload as any;
  assert.ok(strict2Payload.manifest.sourceIds.resultIds.includes(unreviewed.id));

  // includeRejected bypasses the quality gate and includes non-approved batches.
  const rejected = await buildApprovedProject();
  // A separate analyte gets a RETEST_REQUIRED conclusion, flipping the batch.
  const cadmium = rejected.app.results.ingestRevision({
    tenantId: rejected.tenantId, projectId: rejected.projectId, batchId: rejected.batch.id, runId: rejected.run.id,
    aliquotId: rejected.aliquotIds[1]!, analyte: 'cadmium', value: 0.5, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'result-cadmium',
  });
  rejected.app.quality.decide({
    tenantId: rejected.tenantId, projectId: rejected.projectId, batchId: rejected.batch.id, resultId: cadmium.id,
    decision: 'RETEST_REQUIRED', reason: 'out of range', ruleVersion: 'quality-v3', reviewerId: 'reviewer_03',
  });
  // An unreviewed revision also lands after the batch was rejected.
  const unreviewed2 = rejected.app.results.ingestRevision({
    tenantId: rejected.tenantId, projectId: rejected.projectId, batchId: rejected.batch.id, runId: rejected.run.id,
    aliquotId: rejected.aliquotIds[0]!, analyte: 'lead', value: 0.5, unit: 'mg/L', detectionLimit: 0.01,
    qualityFlags: [], instrumentSoftwareVersion: 'instrument-7.2', contentHash: 'result-rejected-project',
  });
  const wide = await rejected.app.evidence.generate({
    tenantId: rejected.tenantId, projectId: rejected.projectId, requestKey: 'req-wide', includeRejected: true,
  });
  const widePayload = (await rejected.app.evidence.readPackage(wide.id)).payload as any;
  assert.ok(widePayload.manifest.sourceIds.batchIds.includes(rejected.batch.id), 'rejected batch included with flag');
  assert.ok(widePayload.manifest.sourceIds.resultIds.includes(unreviewed2.id), 'unreviewed result included with flag');
  const wideQuality = widePayload.entities.quality as Array<{ decision: string }>;
  assert.ok(wideQuality.some((decision) => decision.decision === 'RETEST_REQUIRED'));

  // Default export of the rejected project omits the whole batch (and its results).
  const narrow = await rejected.app.evidence.generate({
    tenantId: rejected.tenantId, projectId: rejected.projectId, requestKey: 'req-narrow',
  });
  const narrowPayload = (await rejected.app.evidence.readPackage(narrow.id)).payload as any;
  assert.deepEqual(narrowPayload.manifest.sourceIds.batchIds, []);
  assert.deepEqual(narrowPayload.manifest.sourceIds.resultIds, []);
});
