class Mutex {
    tail = Promise.resolve();
    async runExclusive(work) {
        const previous = this.tail;
        let release;
        this.tail = new Promise((resolve) => { release = resolve; });
        await previous;
        try {
            return await work();
        }
        finally {
            release();
        }
    }
}
/**
 * Change journal for transactional rollback. It preserves object identity:
 * entities are mutated in place by the services, so on failure we undo the
 * map structural changes and restore the original properties of every entity
 * that existed when the transaction began. Replacing the maps with cloned
 * snapshots would leave already-returned callers holding stale copies, which
 * breaks evidence retries that reuse the same package object.
 */
class ChangeJournal {
    changes = [];
    entityBaselines = new Map();
    auditStartLength = 0;
    outboxStartLength = 0;
    begin(collections, audit, outbox) {
        this.changes.length = 0;
        this.entityBaselines.clear();
        this.auditStartLength = audit.length;
        this.outboxStartLength = outbox.length;
        for (const collection of collections) {
            for (const entity of collection.values()) {
                if (entity && typeof entity === 'object' && !this.entityBaselines.has(entity)) {
                    this.entityBaselines.set(entity, { ...entity });
                }
            }
        }
    }
    recordSet(collection, key) {
        const existed = collection.has(key);
        this.changes.push({ collection, kind: 'set', key, existed, ...(existed ? { previous: collection.get(key) } : {}) });
    }
    recordDelete(collection, key) {
        this.changes.push({ collection, kind: 'delete', key, previous: collection.get(key) });
    }
    rollback(audit, outbox, restoreSequence) {
        for (let index = this.changes.length - 1; index >= 0; index -= 1) {
            const change = this.changes[index];
            if (!change)
                continue;
            if (change.kind === 'set') {
                if (change.existed)
                    change.collection.set(change.key, change.previous);
                else
                    change.collection.delete(change.key);
            }
            else {
                change.collection.set(change.key, change.previous);
            }
        }
        audit.length = this.auditStartLength;
        outbox.length = this.outboxStartLength;
        for (const [entity, baseline] of this.entityBaselines) {
            Object.assign(entity, baseline);
        }
        restoreSequence();
    }
}
function journaledMap(isJournaling, journal) {
    const target = new Map();
    return new Proxy(target, {
        get: (map, property, receiver) => {
            if (property === 'set') {
                return (key, value) => {
                    if (isJournaling())
                        journal.recordSet(map, key);
                    return map.set(key, value);
                };
            }
            if (property === 'delete') {
                return (key) => {
                    if (isJournaling() && map.has(key))
                        journal.recordDelete(map, key);
                    return map.delete(key);
                };
            }
            const value = Reflect.get(map, property, map);
            if (typeof value === 'function')
                return value.bind(map);
            return value;
        },
    });
}
export class Store {
    journal = new ChangeJournal();
    journaling = false;
    transactionDepth = 0;
    samples = journaledMap(() => this.journaling, this.journal);
    containers = journaledMap(() => this.journaling, this.journal);
    transfers = journaledMap(() => this.journaling, this.journal);
    receiving = journaledMap(() => this.journaling, this.journal);
    aliquots = journaledMap(() => this.journaling, this.journal);
    batches = journaledMap(() => this.journaling, this.journal);
    runs = journaledMap(() => this.journaling, this.journal);
    results = journaledMap(() => this.journaling, this.journal);
    quality = journaledMap(() => this.journaling, this.journal);
    evidence = journaledMap(() => this.journaling, this.journal);
    syncRecords = journaledMap(() => this.journaling, this.journal);
    files = journaledMap(() => this.journaling, this.journal);
    audit = [];
    outbox = [];
    lock = new Mutex();
    sequence = 0;
    now() { return new Date().toISOString(); }
    nextId(prefix) {
        this.sequence += 1;
        return `${prefix}_${this.sequence.toString().padStart(8, '0')}`;
    }
    addAudit(input) {
        const record = { ...input, id: this.nextId('audit'), createdAt: this.now() };
        this.audit.push(record);
        return record;
    }
    addOutbox(input) {
        const message = { ...input, id: this.nextId('outbox'), status: 'PENDING', createdAt: this.now() };
        this.outbox.push(message);
        return message;
    }
    snapshot() {
        return {
            sequence: this.sequence,
            samples: structuredClone([...this.samples.values()]),
            containers: structuredClone([...this.containers.values()]),
            transfers: structuredClone([...this.transfers.values()]),
            receiving: structuredClone([...this.receiving.values()]),
            aliquots: structuredClone([...this.aliquots.values()]),
            batches: structuredClone([...this.batches.values()]),
            runs: structuredClone([...this.runs.values()]),
            results: structuredClone([...this.results.values()]),
            quality: structuredClone([...this.quality.values()]),
            evidence: structuredClone([...this.evidence.values()]),
            syncRecords: structuredClone([...this.syncRecords.values()]),
            audit: structuredClone(this.audit),
            outbox: structuredClone(this.outbox),
            files: structuredClone([...this.files.values()]),
        };
    }
    async transaction(work) {
        return this.lock.runExclusive(async () => {
            if (this.transactionDepth > 0)
                return work();
            this.transactionDepth += 1;
            const sequenceAtStart = this.sequence;
            this.journal.begin(this.journaledCollections(), this.audit, this.outbox);
            this.journaling = true;
            try {
                return await work();
            }
            catch (error) {
                this.journaling = false;
                this.journal.rollback(this.audit, this.outbox, () => { this.sequence = sequenceAtStart; });
                throw error;
            }
            finally {
                this.journaling = false;
                this.transactionDepth -= 1;
            }
        });
    }
    journaledCollections() {
        return [
            this.samples, this.containers, this.transfers, this.receiving, this.aliquots, this.batches,
            this.runs, this.results, this.quality, this.evidence, this.syncRecords, this.files,
        ];
    }
}
