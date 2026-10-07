/**
 * FNV-1a 32-bit stable hash used for manifest/content identity.
 * Deterministic across processes; not cryptographic.
 */
export function stableHash(value: string): string {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
}

/**
 * Deterministic JSON serialization: object keys are sorted and arrays keep
 * their (already curated) order. Two captures of identical domain content
 * always produce the same string; any field change changes it.
 */
export function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  const record = value as Record<string, unknown>;
  const entries = Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`);
  return `{${entries.join(',')}}`;
}
