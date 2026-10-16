import { z } from 'zod';

const str = z.string().min(1).max(255);
const money = z.object({ amount: z.string().min(1).max(64), currency: z.string().min(1).max(16) }).strict();

export const openWalletSchema = z.object({ playerId: z.string().uuid(), initialBalance: money }).strict();

export const submitSchema = z
  .object({
    providerId: str,
    externalTransactionId: str,
    playerId: z.string().uuid(),
    walletId: z.string().uuid(),
    roundId: str,
    gameId: str,
    kind: str,
    money,
    referenceExternalTransactionId: str.optional(),
  })
  .strict();

export const idempotencyKeySchema = str;

export const ledgerQuerySchema = z.object({
  cursor: z.string().min(1).max(512).optional(),
  limit: z.coerce.number().int().default(50),
});

export const uuidSchema = z.string().uuid();

/** Valida e devolve o dado tipado; ZodError vira 400 no filtro global. */
export const parse = <T extends z.ZodTypeAny>(schema: T, data: unknown): z.infer<T> => schema.parse(data);
