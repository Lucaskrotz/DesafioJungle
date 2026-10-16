// Tokens de DI explícitos: o Bun não emite design:paramtypes, então todo construtor usa @Inject(TOKEN).
export const ORM = Symbol('ORM');
export const UNIT_OF_WORK = Symbol('UNIT_OF_WORK');
