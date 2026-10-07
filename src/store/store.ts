import type {
  Aliquot, AnalysisBatch, AuditRecord, CustodyTransfer, EvidencePackage, FileRecord, InstrumentRun, OutboxMessage,
  QualityDecision, ReceivingRecord, ResultRevision, SampleContainer, SamplingRecord, SyncRecord,
} from '../domain/types.js';

class Mutex {
  private tail = Promise.resolve();

  async runExclusive<T>(work: () => Promise<T> | T): Promise<T> {
    const previous = this.tail;
    let release!: () => void;
    this.tail = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    try {
      return await work();
    } finally {
      release();
    }
  }
}

type Snapshot = {
  sequence: number;
  samples: SamplingRecord[];
  containers: SampleContainer[];
  transfers: CustodyTransfer[];
  receiving: ReceivingRecord[];
  aliquots: Aliquot[];
  batches: AnalysisBatch[];
  runs: InstrumentRun[];
  results: ResultRevision[];
  quality: QualityDecision[];
  evidence: EvidencePackage[];
  syncRecords: SyncRecord[];
  audit: AuditRecord[];
  outbox: OutboxMessage[];
  files: FileRecord[];
};

type Change =
  | { collection: Map<string, unknown>; kind: 'set'; key: string; existed: boolean; previous?: unknown }
  | { collection: Map<string, unknown>; kind: 'delete'; key: string; previous: unknown };

/**
 * Change journal for transactional rollback. It preserves object identity:
 * entities are mutated in place by the services, so on failure we undo the
 * map structural changes and restore the original properties of every entity
 * that existed when the transaction began. Replacing the maps with cloned
 * snapshots would leave already-returned callers holding stale copies, which
 * breaks evidence retries that reuse the same package object.
 */
class ChangeJournal {
  private readonly changes: Change[] = [];
  private readonly entityBaselines = new Map<object, Record<string, unknown>>();
  private auditStartLength = 0;
  private outboxStartLength = 0;

  begin(collections: Array<Map<string, unknown>>, audit: AuditRecord[], outbox: OutboxMessage[]): void {
    this.changes.length = 0;
    this.entityBaselines.clear();
    this.auditStartLength = audit.length;
    this.outboxStartLength = outbox.length;
    for (const collection of collections) {
      for (const entity of collection.values()) {
        if (entity && typeof entity === 'object' && !this.entityBaselines.has(entity)) {
          this.entityBaselines.set(entity, { ...(entity as Record<string, unknown>) });
        }
      }
    }
  }

  recordSet(collection: Map<string, unknown>, key: string): void {
    const existed = collection.has(key);
    this.changes.push({ collection, kind: 'set', key, existed, ...(existed ? { previous: collection.get(key) } : {}) });
  }

  recordDelete(collection: Map<string, unknown>, key: string): void {
    this.changes.push({ collection, kind: 'delete', key, previous: collection.get(key) });
  }

  rollback(audit: AuditRecord[], outbox: OutboxMessage[], restoreSequence: () => void): void {
    for (let index = this.changes.length - 1; index >= 0; index -= 1) {
      const change = this.changes[index];
      if (!change) continue;
      if (change.kind === 'set') {
        if (change.existed) change.collection.set(change.key, change.previous);
        else change.collection.delete(change.key);
      } else {
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

function journaledMap<K, V>(isJournaling: () => boolean, journal: { recordSet(collection: Map<string, unknown>, key: string): void; recordDelete(collection: Map<string, unknown>, key: string): void }): Map<K, V> {
  const target = new Map<K, V>();
  return new Proxy(target, {
    get: (map, property, receiver) => {
      if (property === 'set') {
        return (key: K, value: V) => {
          if (isJournaling()) journal.recordSet(map as unknown as Map<string, unknown>, key as string);
          return map.set(key, value);
        };
      }
      if (property === 'delete') {
        return (key: K) => {
          if (isJournaling() && map.has(key)) journal.recordDelete(map as unknown as Map<string, unknown>, key as string);
          return map.delete(key);
        };
      }
      const value = Reflect.get(map, property, map);
      if (typeof value === 'function') return (value as (...args: unknown[]) => unknown).bind(map);
      return value;
    },
  });
}

export class Store {
  private journal = new ChangeJournal();
  private journaling = false;
  private transactionDepth = 0;
  readonly samples: Map<string, SamplingRecord> = journaledMap(() => this.journaling, this.journal);
  readonly containers: Map<string, SampleContainer> = journaledMap(() => this.journaling, this.journal);
  readonly transfers: Map<string, CustodyTransfer> = journaledMap(() => this.journaling, this.journal);
  readonly receiving: Map<string, ReceivingRecord> = journaledMap(() => this.journaling, this.journal);
  readonly aliquots: Map<string, Aliquot> = journaledMap(() => this.journaling, this.journal);
  readonly batches: Map<string, AnalysisBatch> = journaledMap(() => this.journaling, this.journal);
  readonly runs: Map<string, InstrumentRun> = journaledMap(() => this.journaling, this.journal);
  readonly results: Map<string, ResultRevision> = journaledMap(() => this.journaling, this.journal);
  readonly quality: Map<string, QualityDecision> = journaledMap(() => this.journaling, this.journal);
  readonly evidence: Map<string, EvidencePackage> = journaledMap(() => this.journaling, this.journal);
  readonly syncRecords: Map<string, SyncRecord> = journaledMap(() => this.journaling, this.journal);
  readonly files: Map<string, FileRecord> = journaledMap(() => this.journaling, this.journal);
  readonly audit: AuditRecord[] = [];
  readonly outbox: OutboxMessage[] = [];
  readonly lock = new Mutex();
  private sequence = 0;

  now(): string { return new Date().toISOString(); }

  nextId(prefix: string): string {
    this.sequence += 1;
    return `${prefix}_${this.sequence.toString().padStart(8, '0')}`;
  }

  addAudit(input: Omit<AuditRecord, 'id' | 'createdAt'>): AuditRecord {
    const record = { ...input, id: this.nextId('audit'), createdAt: this.now() };
    this.audit.push(record);
    return record;
  }

  addOutbox(input: Omit<OutboxMessage, 'id' | 'createdAt' | 'status'>): OutboxMessage {
    const message = { ...input, id: this.nextId('outbox'), status: 'PENDING' as const, createdAt: this.now() };
    this.outbox.push(message);
    return message;
  }

  snapshot(): Snapshot {
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

  async transaction<T>(work: () => Promise<T> | T): Promise<T> {
    return this.lock.runExclusive(async () => {
      if (this.transactionDepth > 0) return work();
      this.transactionDepth += 1;
      const sequenceAtStart = this.sequence;
      this.journal.begin(this.journaledCollections(), this.audit, this.outbox);
      this.journaling = true;
      try {
        return await work();
      } catch (error) {
        this.journaling = false;
        this.journal.rollback(this.audit, this.outbox, () => { this.sequence = sequenceAtStart; });
        throw error;
      } finally {
        this.journaling = false;
        this.transactionDepth -= 1;
      }
    });
  }

  private journaledCollections(): Array<Map<string, unknown>> {
    return [
      this.samples, this.containers, this.transfers, this.receiving, this.aliquots, this.batches,
      this.runs, this.results, this.quality, this.evidence, this.syncRecords, this.files,
    ] as Array<Map<string, unknown>>;
  }
}
