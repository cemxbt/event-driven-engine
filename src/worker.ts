import pg from 'pg';
import { Dispatcher } from './dispatcher.js';
import { OUTBOX_CHANNEL } from './outbox.js';

export interface WorkerOptions {
  /** Upper bound on how long a due event can wait if a notification is missed. */
  pollIntervalMs?: number;
  batchSize?: number;
  logger?: Pick<Console, 'error'>;
}

/**
 * Runs a dispatcher continuously. It wakes immediately on `NOTIFY` from `publish()` and
 * also polls, because notifications are not delivered while disconnected and retries
 * become due without any new notification.
 */
export class OutboxWorker {
  private running = false;
  private loop?: Promise<void>;
  private wake?: () => void;
  private listener?: pg.PoolClient;

  constructor(
    private readonly pool: pg.Pool,
    private readonly dispatcher: Dispatcher,
    private readonly options: WorkerOptions = {},
  ) {}

  async start() {
    if (this.running) return;
    this.running = true;

    this.listener = await this.pool.connect();
    this.listener.on('notification', () => this.wake?.());
    await this.listener.query(`LISTEN ${OUTBOX_CHANNEL}`);

    this.loop = this.run();
  }

  async stop() {
    this.running = false;
    this.wake?.();
    await this.loop;
    if (this.listener) {
      await this.listener.query(`UNLISTEN ${OUTBOX_CHANNEL}`).catch(() => undefined);
      this.listener.release();
      this.listener = undefined;
    }
  }

  private async run() {
    const { pollIntervalMs = 1_000, batchSize = 100, logger = console } = this.options;
    while (this.running) {
      try {
        // Keep draining while full batches come back; otherwise wait.
        if ((await this.dispatcher.drain(batchSize)) === batchSize) continue;
      } catch (error) {
        logger.error('[outbox] dispatch failed', error);
      }
      if (!this.running) break;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, pollIntervalMs);
        this.wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
      this.wake = undefined;
    }
  }
}
