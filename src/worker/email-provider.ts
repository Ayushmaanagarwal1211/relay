import nodemailer, { Transporter } from 'nodemailer';

export interface OutgoingEmail {
  id: string;
  to: string;
  subject: string;
  body: string;
}

// Anything that can deliver an email: SMTP today; a mock for load tests; SES or WhatsApp later.
// The worker only knows this interface, so swapping providers doesn't touch the worker.
export interface EmailProvider {
  readonly name: string;
  send(email: OutgoingEmail): Promise<void>;
}

// Thrown when retrying can't help (e.g. the address doesn't exist).
// Any other error is treated as temporary and retried.
export class PermanentSendError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'PermanentSendError';
  }
}

/**
 * SMTP reply codes: 4xx = temporary ("try again later": mailbox busy, rate limited),
 * 5xx = permanent ("will never work": no such user, rejected).
 * Network errors (no responseCode at all) are temporary: the server may come back.
 */
export function isPermanentSmtpError(err: unknown): boolean {
  const code = (err as { responseCode?: unknown })?.responseCode;
  return typeof code === 'number' && code >= 500 && code < 600;
}

export class SmtpProvider implements EmailProvider {
  readonly name = 'smtp';
  private transporter: Transporter;

  constructor(
    options: { host: string; port: number },
    private from: string,
  ) {
    this.transporter = nodemailer.createTransport({
      host: options.host,
      port: options.port,
      secure: false,
      // Fail fast instead of nodemailer's default 2-minute connect timeout.
      // Must stay well under SENDING_LEASE_MS.
      connectionTimeout: 10_000,
      greetingTimeout: 10_000,
      socketTimeout: 30_000,
    });
  }

  async send(email: OutgoingEmail): Promise<void> {
    try {
      await this.transporter.sendMail({
        from: this.from,
        to: email.to,
        subject: email.subject,
        text: email.body,
        // Same Message-ID on every attempt, so mail servers can spot a duplicate.
        messageId: `<${email.id}@relay.local>`,
      });
    } catch (err) {
      if (isPermanentSmtpError(err)) {
        throw new PermanentSendError(`SMTP rejected the email: ${(err as Error).message}`, { cause: err });
      }
      throw err;
    }
  }
}
