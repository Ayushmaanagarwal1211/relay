import { EmailProvider, OutgoingEmail, PermanentSendError } from '../../src/worker/email-provider';

type Behaviour = 'ok' | 'fail' | 'permanent';

// An email provider for tests: records what it sent, and fails on demand.
// `script` says what to do on each call; after the script runs out, it repeats the last entry.
export class FakeProvider implements EmailProvider {
  readonly name = 'fake';
  sent: OutgoingEmail[] = [];
  calls = 0;

  constructor(private script: Behaviour[] = ['ok']) {}

  async send(email: OutgoingEmail): Promise<void> {
    const behaviour = this.script[Math.min(this.calls, this.script.length - 1)];
    this.calls++;
    if (behaviour === 'fail') throw new Error('provider temporarily unavailable');
    if (behaviour === 'permanent') throw new PermanentSendError('mailbox does not exist');
    this.sent.push(email);
  }
}
