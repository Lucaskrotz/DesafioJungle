/** UUIDv7: ordenável no tempo, bom para índices B-tree. */
export const newId = (): string => Bun.randomUUIDv7();
