import { DomainError } from '../domain/errors.js';

/**
 * Raised by adapters when the remote file store times out or rejects a write.
 * The evidence service turns this into a FAILED package without committing
 * the package index, so the client can safely retry with the same request key.
 */
export class EvidenceFileError extends DomainError {
  constructor(code: string, message: string, public readonly key?: string) {
    super(code, message);
    this.name = 'EvidenceFileError';
  }
}

export interface EvidenceFileContent {
  key: string;
  body: string;
  contentHash: string;
}

export interface EvidenceFileAdapter {
  /** Write the package payload to storage. Must be idempotent for the same key. */
  put(key: string, body: string, contentHash: string): Promise<void>;
  /** Read back a previously written payload, or throw evidence.file_missing. */
  get(key: string): Promise<EvidenceFileContent>;
  /** True when a payload for the key is available in the backing store. */
  exists(key: string): Promise<boolean>;
}

/**
 * Default in-memory adapter. Failures are injected by key so tests can
 * simulate the file adapter timing out on a first attempt and succeeding
 * on the client retry.
 */
export class InMemoryEvidenceFileAdapter implements EvidenceFileAdapter {
  private readonly objects = new Map<string, EvidenceFileContent>();
  private readonly failingKeys = new Set<string>();
  private putDelayMs = 0;
  private failOnceCount = 0;

  /** The next N put attempts fail regardless of key (simulated timeout). */
  failNextPutAttempt(times = 1): void {
    this.failOnceCount += times;
  }

  /** Make puts for this key fail (e.g. simulate a timeout) until cleared. */
  failNextPut(key: string): void {
    this.failingKeys.add(key);
  }

  clearFailure(key: string): void {
    this.failingKeys.delete(key);
  }

  /** Inject artificial latency so concurrent generation is observable. */
  setPutDelay(delayMs: number): void {
    this.putDelayMs = delayMs;
  }

  /** Test hook: remove the backing object while leaving the package index. */
  simulateObjectDeletion(key: string): void {
    this.objects.delete(key);
  }

  async put(key: string, body: string, contentHash: string): Promise<void> {
    if (this.putDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.putDelayMs));
    }
    if (this.failOnceCount > 0 || this.failingKeys.has(key)) {
      this.failOnceCount = Math.max(0, this.failOnceCount - 1);
      throw new EvidenceFileError('evidence.file_timeout', `file adapter timed out while writing ${key}`, key);
    }
    // Content-addressed: a put of the same key must never overwrite with
    // different bytes, which would silently mutate a published package.
    const existing = this.objects.get(key);
    if (existing && existing.contentHash !== contentHash) {
      throw new EvidenceFileError('evidence.file_hash_conflict', `file ${key} already exists with different content`, key);
    }
    this.objects.set(key, { key, body, contentHash });
  }

  async get(key: string): Promise<EvidenceFileContent> {
    const object = this.objects.get(key);
    if (!object) throw new EvidenceFileError('evidence.file_missing', `evidence file is missing from storage: ${key}`, key);
    return object;
  }

  async exists(key: string): Promise<boolean> {
    return this.objects.has(key);
  }
}
