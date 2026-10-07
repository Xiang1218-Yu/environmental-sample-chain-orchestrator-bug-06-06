import { assertCondition, assertFound, DomainError } from '../domain/errors.js';
/**
 * Evidence packages are produced in three serialised phases:
 *   1. prepare  - read one immutable store snapshot, scope-check and filter it
 *   2. upload   - write the self-contained artifact to the file adapter
 *   3. commit   - re-check the request under the store lock and publish the index
 * The index is only ever published together with a COMMITTED file record, so a
 * failure in any phase leaves at most a FAILED marker and a retry is safe.
 */
export class EvidenceService {
    store;
    files;
    inFlight = new Map();
    constructor(store, files) {
        this.store = store;
        this.files = files;
    }
    async generate(input) {
        const request = this.normalize(input);
        const dedupeKey = `${request.tenantId}|${request.projectId}|${request.requestKey}`;
        const running = this.inFlight.get(dedupeKey);
        if (running)
            return running;
        const work = this.runRequest(request);
        this.inFlight.set(dedupeKey, work);
        try {
            return await work;
        }
        finally {
            this.inFlight.delete(dedupeKey);
        }
    }
    get(evidenceId) {
        return assertFound(this.store.evidence.get(evidenceId), 'evidence.not_found', 'evidence package not found');
    }
    /** Frozen artifact payload; the package is self-contained even if live objects are deleted. */
    getArtifact(evidenceId) {
        const packageRecord = this.get(evidenceId);
        const fileKey = packageRecord.fileKeys[0];
        if (!fileKey)
            throw new DomainError('evidence.file_missing', 'evidence package has no file');
        const file = this.store.files.get(fileKey);
        assertCondition(file && file.status === 'COMMITTED' && typeof file.content === 'string', 'evidence.file_missing', 'evidence artifact is not committed');
        return file.content;
    }
    async verify(evidenceId) {
        const packageRecord = this.get(evidenceId);
        const files = [];
        for (const key of packageRecord.fileKeys) {
            const file = this.store.files.get(key);
            const presentInAdapter = await this.files.exists(key);
            files.push({
                key,
                presentInAdapter,
                presentInIndex: Boolean(file && file.status === 'COMMITTED'),
                hashMatches: Boolean(file && file.contentHash === packageRecord.manifestHash),
            });
        }
        const healthy = packageRecord.status !== 'GENERATING'
            && packageRecord.status !== 'FAILED'
            && files.length > 0
            && files.every((file) => file.presentInAdapter && file.presentInIndex && file.hashMatches);
        return { status: packageRecord.status, manifestHash: packageRecord.manifestHash ?? '', files, healthy };
    }
    async freeze(evidenceId) {
        return this.store.transaction(async () => {
            const packageRecord = assertFound(this.store.evidence.get(evidenceId), 'evidence.not_found', 'evidence package not found');
            if (packageRecord.status === 'FROZEN')
                return packageRecord;
            if (packageRecord.status !== 'READY')
                throw new DomainError('evidence.not_ready', 'only ready evidence packages can be frozen');
            this.assertIndexComplete(packageRecord);
            packageRecord.status = 'FROZEN';
            packageRecord.frozenAt = this.store.now();
            packageRecord.version += 1;
            this.store.addAudit({ tenantId: packageRecord.tenantId, projectId: packageRecord.projectId, action: 'evidence.frozen', entity: 'evidence', entityId: packageRecord.id, metadata: { manifestHash: packageRecord.manifestHash ?? '' } });
            return packageRecord;
        });
    }
    async runRequest(input) {
        // A finished package for this request is the logical answer forever.
        const terminal = this.findExisting(input);
        if (terminal && (terminal.status === 'READY' || terminal.status === 'FROZEN'))
            return terminal;
        // Phase 1: build the export from one consistent snapshot and reserve its revision.
        const prepared = await this.store.transaction(() => this.prepare(input));
        const { packageRecord, artifact } = prepared;
        // Phase 2: adapter upload happens outside the store lock. An ambiguous
        // timeout may still have stored the object; the adapter makes this idempotent.
        try {
            await this.files.put({ key: artifact.key, content: artifact.content, contentHash: artifact.contentHash });
        }
        catch (error) {
            await this.markFailure(packageRecord, error);
            throw error instanceof DomainError ? error : new DomainError('evidence.file_failed', error instanceof Error ? error.message : 'file adapter failed');
        }
        // Phase 3: commit index + file record atomically, re-checking the request.
        try {
            return await this.store.transaction(() => this.commit(input, packageRecord, artifact));
        }
        catch (error) {
            await this.markFailure(packageRecord, error);
            throw error;
        }
    }
    prepare(input) {
        const existing = this.findExisting(input);
        if (existing && (existing.status === 'READY' || existing.status === 'FROZEN')) {
            throw new DomainError('evidence.already_ready', 'evidence package for this request is already published');
        }
        this.assertLimits(input);
        const plan = this.buildPlan(input);
        const packageRecord = existing ?? {
            id: this.store.nextId('evidence'),
            tenantId: input.tenantId,
            projectId: input.projectId,
            requestKey: input.requestKey,
            status: 'GENERATING',
            snapshotVersion: 0,
            attempts: 0,
            sourceIds: emptySourceIds(),
            fileKeys: [],
            version: 1,
        };
        if (existing) {
            existing.status = 'GENERATING';
            delete existing.failureReason;
            existing.version = Math.max(existing.version, 1);
        }
        packageRecord.attempts += 1;
        packageRecord.snapshotVersion = contentSnapshotVersion(input.tenantId, input.projectId, input.includeRejected, plan.manifest.sources);
        packageRecord.includeRejected = input.includeRejected;
        packageRecord.generatedAt = packageRecord.generatedAt ?? this.store.now();
        packageRecord.sourceIds = plan.sourceIds;
        this.store.evidence.set(packageRecord.id, packageRecord);
        const manifest = {
            ...plan.manifest,
            packageId: packageRecord.id,
            snapshotVersion: packageRecord.snapshotVersion,
            generatedAt: packageRecord.generatedAt,
        };
        const content = canonicalJson(manifest);
        const manifestHash = stableHash(content);
        return {
            packageRecord,
            artifact: { key: this.fileKey(input, manifestHash), content, contentHash: manifestHash, sizeBytes: content.length },
        };
    }
    commit(input, claimed, artifact) {
        // Another attempt for the same request may have committed while we uploaded.
        const winner = this.findExisting(input);
        if (winner && winner.id !== claimed.id && (winner.status === 'READY' || winner.status === 'FROZEN')) {
            return winner;
        }
        const packageRecord = winner ?? claimed;
        if (packageRecord.status === 'READY' && packageRecord.manifestHash === artifact.contentHash) {
            return packageRecord;
        }
        const file = {
            key: artifact.key,
            contentHash: artifact.contentHash,
            sizeBytes: artifact.sizeBytes,
            content: artifact.content,
            createdAt: this.store.now(),
            status: 'COMMITTED',
        };
        this.store.files.set(file.key, file);
        packageRecord.status = 'READY';
        packageRecord.manifestHash = artifact.contentHash;
        packageRecord.fileKeys = [artifact.key];
        delete packageRecord.failureReason;
        packageRecord.snapshotVersion = claimed.snapshotVersion;
        packageRecord.sourceIds = claimed.sourceIds;
        packageRecord.generatedAt = claimed.generatedAt ?? this.store.now();
        packageRecord.includeRejected = claimed.includeRejected ?? false;
        packageRecord.attempts = claimed.attempts;
        this.store.evidence.set(packageRecord.id, packageRecord);
        this.store.addAudit({ tenantId: input.tenantId, projectId: input.projectId, action: 'evidence.generated', entity: 'evidence', entityId: packageRecord.id, metadata: { snapshotVersion: packageRecord.snapshotVersion, manifestHash: artifact.contentHash, resultCount: packageRecord.sourceIds.resultIds.length, attempts: packageRecord.attempts } });
        this.store.addOutbox({ tenantId: input.tenantId, projectId: input.projectId, topic: 'evidence.ready', aggregateId: packageRecord.id, payload: JSON.stringify({ evidenceId: packageRecord.id, requestKey: input.requestKey, snapshotVersion: packageRecord.snapshotVersion, manifestHash: artifact.contentHash, fileKey: artifact.key }) });
        return packageRecord;
    }
    async markFailure(packageRecord, error) {
        const reason = error instanceof Error ? error.message : String(error);
        try {
            await this.store.transaction(() => {
                const current = this.store.evidence.get(packageRecord.id);
                if (!current || current.status === 'READY' || current.status === 'FROZEN')
                    return;
                current.status = 'FAILED';
                current.failureReason = reason;
                this.store.addAudit({ tenantId: current.tenantId, projectId: current.projectId, action: 'evidence.generation_failed', entity: 'evidence', entityId: current.id, metadata: { reason, attempts: current.attempts } });
            });
        }
        catch {
            // Recording the failure must not mask the original adapter error.
        }
    }
    assertIndexComplete(packageRecord) {
        for (const key of packageRecord.fileKeys) {
            const file = this.store.files.get(key);
            assertCondition(file && file.status === 'COMMITTED' && file.contentHash === packageRecord.manifestHash, 'evidence.file_missing', `evidence artifact ${key} is missing or corrupt`);
        }
        assertCondition(packageRecord.fileKeys.length > 0 && packageRecord.manifestHash !== undefined, 'evidence.file_missing', 'evidence package has no committed artifact');
    }
    buildPlan(input) {
        const samples = [...this.store.samples.values()].filter((sample) => sample.tenantId === input.tenantId && sample.projectId === input.projectId);
        const containers = [...this.store.containers.values()].filter((container) => container.tenantId === input.tenantId && container.projectId === input.projectId);
        const batches = [...this.store.batches.values()].filter((batch) => batch.tenantId === input.tenantId && batch.projectId === input.projectId)
            .filter((batch) => input.includeRejected || batch.status === 'APPROVED');
        const batchIds = new Set(batches.map((batch) => batch.id));
        const results = [...this.store.results.values()]
            .filter((result) => result.tenantId === input.tenantId && result.projectId === input.projectId && batchIds.has(result.batchId) && result.status === 'CURRENT')
            .filter((result) => input.includeRejected || this.hasApprovedConclusion(result.id, input));
        const resultIds = new Set(results.map((result) => result.id));
        const quality = [...this.store.quality.values()].filter((decision) => decision.tenantId === input.tenantId && decision.projectId === input.projectId && resultIds.has(decision.resultId));
        const runs = [...this.store.runs.values()].filter((run) => run.tenantId === input.tenantId && run.projectId === input.projectId && batchIds.has(run.batchId));
        const aliquotIds = new Set();
        for (const batch of batches)
            for (const aliquotId of batch.memberAliquotIds)
                aliquotIds.add(aliquotId);
        for (const result of results)
            if (result.aliquotId)
                aliquotIds.add(result.aliquotId);
        const aliquots = [...this.store.aliquots.values()].filter((aliquot) => aliquot.tenantId === input.tenantId && aliquot.projectId === input.projectId && aliquotIds.has(aliquot.id));
        const containerIds = new Set(containers.map((container) => container.id));
        const transfers = [...this.store.transfers.values()].filter((transfer) => transfer.tenantId === input.tenantId && transfer.projectId === input.projectId && containerIds.has(transfer.containerId));
        const receiving = [...this.store.receiving.values()].filter((record) => record.tenantId === input.tenantId && record.projectId === input.projectId && containerIds.has(record.containerId));
        assertCondition(samples.length > 0, 'evidence.no_samples', 'project has no samples to export');
        // Defence in depth: every object referenced by an exported entity must be
        // in the same tenant/project, so a cross-project reference can never leak.
        this.assertReferencesScoped({ input, samples, containers, batches, results, quality, runs, aliquots, transfers, receiving });
        const sourceIds = {
            sampleIds: samples.map((sample) => sample.id).sort(),
            containerIds: containers.map((container) => container.id).sort(),
            transferIds: transfers.map((transfer) => transfer.id).sort(),
            receivingIds: receiving.map((record) => record.id).sort(),
            aliquotIds: aliquots.map((aliquot) => aliquot.id).sort(),
            batchIds: batches.map((batch) => batch.id).sort(),
            runIds: runs.map((run) => run.id).sort(),
            resultIds: results.map((result) => result.id).sort(),
            qualityDecisionIds: quality.map((decision) => decision.id).sort(),
        };
        const manifest = {
            tenantId: input.tenantId,
            projectId: input.projectId,
            requestKey: input.requestKey,
            includeRejected: input.includeRejected,
            sourceIds,
            sources: { samples, containers, transfers, receiving, aliquots, batches, runs, results, quality },
        };
        return { manifest, sourceIds };
    }
    hasApprovedConclusion(resultId, input) {
        return [...this.store.quality.values()].some((decision) => decision.tenantId === input.tenantId && decision.projectId === input.projectId && decision.resultId === resultId && decision.decision === 'APPROVED');
    }
    assertReferencesScoped(scope) {
        const { input } = scope;
        const inScope = (entity) => entity === undefined || (entity.tenantId === input.tenantId && entity.projectId === input.projectId);
        const fail = (reference) => {
            throw new DomainError('scope.forbidden', `referenced object ${reference} is outside the requested project`);
        };
        for (const sample of scope.samples) {
            if (!sample.containerId)
                continue;
            const container = scope.containers.find((item) => item.id === sample.containerId);
            if (!container) {
                const liveContainer = this.store.containers.get(sample.containerId);
                if (liveContainer && !inScope(liveContainer))
                    fail(sample.containerId);
            }
        }
        for (const container of scope.containers) {
            const sample = scope.samples.find((item) => item.id === container.sampleId);
            if (!sample) {
                const liveSample = this.store.samples.get(container.sampleId);
                if (liveSample && !inScope(liveSample))
                    fail(container.sampleId);
            }
        }
        for (const batch of scope.batches) {
            for (const aliquotId of batch.memberAliquotIds) {
                if (!scope.aliquots.some((item) => item.id === aliquotId)) {
                    const live = this.store.aliquots.get(aliquotId);
                    if (live && !inScope(live))
                        fail(aliquotId);
                }
            }
        }
        for (const aliquot of scope.aliquots) {
            if (!scope.containers.some((container) => container.id === aliquot.parentContainerId)) {
                const liveContainer = this.store.containers.get(aliquot.parentContainerId);
                if (liveContainer && !inScope(liveContainer))
                    fail(aliquot.parentContainerId);
            }
        }
        for (const run of scope.runs) {
            if (!scope.batches.some((batch) => batch.id === run.batchId))
                fail(run.batchId);
        }
        for (const result of scope.results) {
            if (!scope.batches.some((batch) => batch.id === result.batchId))
                fail(result.batchId);
            if (!scope.runs.some((run) => run.id === result.runId))
                fail(result.runId);
            if (result.aliquotId && !scope.aliquots.some((aliquot) => aliquot.id === result.aliquotId)) {
                const live = this.store.aliquots.get(result.aliquotId);
                if (live && !inScope(live))
                    fail(result.aliquotId);
            }
        }
        for (const decision of scope.quality) {
            if (!scope.results.some((result) => result.id === decision.resultId))
                fail(decision.resultId);
            if (!scope.batches.some((batch) => batch.id === decision.batchId))
                fail(decision.batchId);
        }
        for (const transfer of scope.transfers) {
            if (!scope.containers.some((container) => container.id === transfer.containerId))
                fail(transfer.containerId);
        }
        for (const record of scope.receiving) {
            if (!scope.containers.some((container) => container.id === record.containerId))
                fail(record.containerId);
        }
    }
    normalize(input) {
        assertCondition(typeof input.tenantId === 'string' && input.tenantId.trim().length > 0, 'evidence.invalid_tenant', 'tenant is required');
        assertCondition(typeof input.projectId === 'string' && input.projectId.trim().length > 0, 'evidence.invalid_project', 'project is required');
        assertCondition(typeof input.requestKey === 'string' && /^[A-Za-z0-9_.:-]{4,128}$/.test(input.requestKey), 'evidence.invalid_request', 'request key must be 4-128 chars from [A-Za-z0-9_.:-]');
        assertCondition(input.includeRejected === undefined || typeof input.includeRejected === 'boolean', 'evidence.invalid_filter', 'invalid rejected filter');
        return { tenantId: input.tenantId.trim(), projectId: input.projectId.trim(), requestKey: input.requestKey, includeRejected: input.includeRejected ?? false };
    }
    assertLimits(input) {
        const pending = [...this.store.evidence.values()].filter((item) => item.tenantId === input.tenantId && item.projectId === input.projectId && item.status !== 'READY' && item.status !== 'FROZEN');
        assertCondition(pending.length < 4, 'evidence.too_many_pending', 'too many evidence requests are pending');
        const fileCount = [...this.store.files.values()].filter((file) => file.status === 'COMMITTED').length;
        assertCondition(fileCount < 100000, 'evidence.storage_limit', 'evidence storage limit reached');
        const auditCount = this.store.audit.filter((item) => item.projectId === input.projectId).length;
        assertCondition(auditCount < 1000000, 'evidence.audit_limit', 'audit history is too large to export');
    }
    findExisting(input) {
        return [...this.store.evidence.values()].find((item) => item.tenantId === input.tenantId && item.projectId === input.projectId && item.requestKey === input.requestKey);
    }
    fileKey(input, manifestHash) {
        return `evidence/${input.projectId}/${input.requestKey}/${manifestHash}.json`;
    }
}
function emptySourceIds() {
    return { sampleIds: [], containerIds: [], transferIds: [], receivingIds: [], aliquotIds: [], batchIds: [], runIds: [], resultIds: [], qualityDecisionIds: [] };
}
function canonicalJson(value) {
    return JSON.stringify(sortDeep(value));
}
function sortDeep(value) {
    if (Array.isArray(value))
        return value.map(sortDeep);
    if (value && typeof value === 'object') {
        const entries = Object.entries(value).map(([key, entry]) => [key, sortDeep(entry)]);
        entries.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
        return Object.fromEntries(entries);
    }
    return value;
}
/**
 * Logical version of the exported data: two attempts see the same source
 * objects (and scope) exactly when the snapshot version is equal. It depends
 * only on data content, never on attempts, timestamps or internal bookkeeping.
 */
function contentSnapshotVersion(tenantId, projectId, includeRejected, sources) {
    const hex = stableHash(canonicalJson({ tenantId, projectId, includeRejected, sources }));
    return Number.parseInt(hex, 16);
}
function stableHash(value) {
    let hash = 2166136261;
    for (let index = 0; index < value.length; index += 1) {
        hash ^= value.charCodeAt(index);
        hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(16).padStart(8, '0');
}
