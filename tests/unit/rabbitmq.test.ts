import { createTopology, decodeMessage, encodeMessage } from '../../src/shared/rabbitmq';

describe('createTopology', () => {
  const topology = createTopology([10_000, 60_000, 1500]);

  it('names queues after priority and delay', () => {
    expect(topology.mainQueue('transactional')).toBe('emails.transactional');
    expect(topology.retryQueue('promotional', 10_000)).toBe('emails.promotional.retry.10s');
    expect(topology.retryQueue('transactional', 60_000)).toBe('emails.transactional.retry.1m');
    expect(topology.retryQueue('transactional', 1500)).toBe('emails.transactional.retry.1500ms');
    expect(topology.deadLetterQueue).toBe('emails.dead-letter');
  });

  it('uses the prefix for every name, so tests can run in isolation', () => {
    const isolated = createTopology([1000], 'test-123');
    expect(isolated.exchange).toBe('test-123');
    expect(isolated.mainQueue('promotional')).toBe('test-123.promotional');
  });
});

describe('encodeMessage / decodeMessage', () => {
  it('round-trips a message', () => {
    expect(decodeMessage(encodeMessage({ emailId: 'abc' }))).toEqual({ emailId: 'abc' });
  });

  it('returns null for anything malformed', () => {
    expect(decodeMessage(Buffer.from('not json'))).toBeNull();
    expect(decodeMessage(Buffer.from('{"other": 1}'))).toBeNull();
    expect(decodeMessage(Buffer.from('null'))).toBeNull();
  });
});
