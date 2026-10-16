import type { Logger } from '@nestjs/common';

/** Loop com setTimeout encadeado: nunca sobrepõe ciclos; stop() espera o ciclo em andamento. */
export class PollLoop {
  private timer?: ReturnType<typeof setTimeout>;
  private current?: Promise<void>;
  private running = false;

  constructor(
    private readonly tick: () => Promise<unknown>,
    private readonly intervalMs: number,
    private readonly logger: Logger,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.schedule(0);
  }

  async stop(): Promise<void> {
    this.running = false;
    clearTimeout(this.timer);
    await this.current;
  }

  private schedule(ms: number) {
    this.timer = setTimeout(() => {
      this.current = this.tick()
        .then(
          () => undefined,
          (e) => this.logger.error(e instanceof Error ? (e.stack ?? e.message) : String(e)),
        )
        .finally(() => {
          if (this.running) this.schedule(this.intervalMs);
        });
    }, ms);
  }
}
