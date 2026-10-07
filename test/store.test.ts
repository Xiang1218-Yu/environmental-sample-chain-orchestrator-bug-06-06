import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { DomainError } from '../src/domain/errors.js';
import { Store } from '../src/store/store.js';

describe('store transaction rollback', () => {
  it('restores entity state in place without breaking references held by callers', async () => {
    const store = new Store();
    store.samples.set('sample_1', { id: 'sample_1', tenantId: 't', projectId: 'p', status: 'SEALED', version: 1 } as never);
    const heldReference = store.samples.get('sample_1');

    await assert.rejects(
      store.transaction(() => {
        const sample = store.samples.get('sample_1');
        assert.ok(sample);
        sample.status = 'CONSUMED';
        sample.version = 2;
        store.samples.set('sample_2', { id: 'sample_2' } as never);
        store.addAudit({ tenantId: 't', projectId: 'p', action: 'will.rollback', entity: 'sample', entityId: 'sample_1', metadata: {} });
        throw new DomainError('forced.rollback', 'rollback please');
      }),
      (error: unknown) => error instanceof DomainError && error.code === 'forced.rollback',
    );

    assert.equal(store.samples.get('sample_1'), heldReference);
    assert.equal(heldReference?.status, 'SEALED');
    assert.equal(heldReference?.version, 1);
    assert.equal(store.samples.has('sample_2'), false);
    assert.equal(store.audit.length, 0);
  });

  it('keeps committed changes after a successful transaction', async () => {
    const store = new Store();
    await store.transaction(() => {
      store.results.set('r1', { id: 'r1', status: 'CURRENT' } as never);
      store.addAudit({ tenantId: 't', projectId: 'p', action: 'committed', entity: 'result', entityId: 'r1', metadata: {} });
    });
    assert.equal(store.results.size, 1);
    assert.equal(store.audit.length, 1);
  });

  it('restores a deleted entity with its original content on rollback', async () => {
    const store = new Store();
    store.batches.set('b1', { id: 'b1', status: 'APPROVED' } as never);
    await assert.rejects(store.transaction(() => {
      store.batches.delete('b1');
      throw new Error('boom');
    }));
    assert.equal(store.batches.get('b1')?.status, 'APPROVED');
  });
});
