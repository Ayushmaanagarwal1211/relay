import { isPermanentSmtpError } from '../../src/worker/email-provider';

describe('isPermanentSmtpError', () => {
  it('treats SMTP 5xx replies as permanent', () => {
    expect(isPermanentSmtpError({ responseCode: 550 })).toBe(true); // mailbox doesn't exist
    expect(isPermanentSmtpError({ responseCode: 554 })).toBe(true); // rejected
  });

  it('treats SMTP 4xx replies as temporary', () => {
    expect(isPermanentSmtpError({ responseCode: 421 })).toBe(false); // service unavailable, try later
    expect(isPermanentSmtpError({ responseCode: 451 })).toBe(false); // local error, try later
  });

  it('treats network errors (no SMTP reply) as temporary', () => {
    const refused = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNECTION' });
    expect(isPermanentSmtpError(refused)).toBe(false);
    expect(isPermanentSmtpError(undefined)).toBe(false);
  });
});
