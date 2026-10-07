import { DomainError } from '../domain/errors.js';

export interface EvidenceArtifact {
  key: string;
  content: string;
  contentHash: string;
}

/**
 * Object storage boundary for evidence artifacts. Implementations must be
 * idempotent for the same (key, contentHash) pair so a retried request after
 * an ambiguous timeout never produces a duplicate or a corrupted object.
 */
export interface EvidenceFileAdapter {
  put(artifact: EvidenceArtifact): Promise<{ key: string; contentHash: string }>;
  exists(key: string): Promise<boolean>;
  remove(key: string): Promise<void>;
}

/**
 * Deterministic in-process adapter used by the application and tests. Failures
 * can be injected to model adapter timeouts between the client call and the
 * index commit; already stored objects survive a simulated failure and make a
 * client retry idempotent.
 */
export class InMemoryEvidenceFileAdapter implements EvidenceFileAdapter {
  private readonly objects = new Map<string, EvidenceArtifact>();
  private readonly failKeys = new Set<string>();
  private readonly failPrefixes: string[] = [];
  private storeThenFailPrefix: string | undefined;
  private beforePutHook?: (artifact: EvidenceArtifact) => void;

  async put(artifact: EvidenceArtifact): Promise<{ key: string; contentHash: string }> {
    const existing = this.objects.get(artifact.key);
    if (existing) {
      if (existing.contentHash !== artifact.contentHash) {
        throw new DomainError('evidence.file_conflict', `file key ${artifact.key} already exists with different content`);
      }
      return { key: artifact.key, contentHash: artifact.contentHash };
    }
    this.beforePutHook?.(artifact);
    const storeThenFail = this.storeThenFailPrefix !== undefined && artifact.key.startsWith(this.storeThenFailPrefix);
    if (!storeThenFail && (this.failKeys.has(artifact.key) || this.failPrefixes.some((prefix) => artifact.key.startsWith(prefix)))) {
      throw new DomainError('evidence.file_timeout', `file adapter timed out while storing ${artifact.key}`);
    }
    this.objects.set(artifact.key, { ...artifact });
    if (storeThenFail) {
      this.storeThenFailPrefix = undefined;
      throw new DomainError('evidence.file_timeout', `file adapter timed out after storing ${artifact.key}`);
    }
    return { key: artifact.key, contentHash: artifact.contentHash };
  }

  async exists(key: string): Promise<boolean> {
    return this.objects.has(key);
  }

  async remove(key: string): Promise<void> {
    this.objects.delete(key);
  }

  clearFailures(): void {
    this.failKeys.clear();
    this.failPrefixes.splice(0, this.failPrefixes.length);
  }

  /** Every upload for an exact key fails before the object is stored. */
  failPut(key: string): void {
    this.failKeys.add(key);
  }

  /** Every upload whose key starts with the prefix fails before the object is stored. */
  failPutPrefix(prefix: string): void {
    this.failPrefixes.push(prefix);
  }

  /** First upload stores the object but reports a timeout; the retry is idempotent. */
  timeoutAfterStoreByPrefix(prefix: string): void {
    this.storeThenFailPrefix = prefix;
  }

  objectCount(): number {
    return this.objects.size;
  }

  objectKeys(): string[] {
    return [...this.objects.keys()];
  }

  /** Test hook for interleaving domain writes while an upload is in flight. */
  onBeforePut(hook: (artifact: EvidenceArtifact) => void): void {
    this.beforePutHook = hook;
  }
}
