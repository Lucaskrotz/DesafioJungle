import { Injectable, type CanActivate } from '@nestjs/common';

export const PROVIDER_IDENTITY = Symbol('PROVIDER_IDENTITY');

/** Identidade do provedor autenticado (extraída do token pelo IdP). */
export interface ProviderIdentityPort {
  /** Retorna o providerId do chamador, ou undefined se não autenticado. */
  resolve(request: unknown): string | undefined;
}

export class NoopProviderIdentity implements ProviderIdentityPort {
  resolve(): string | undefined {
    return undefined;
  }
}

/**
 * Ponto de extensão de autenticação: com um IdP externo via OIDC (ex.: Keycloak),
 * este guard valida o JWT e checa que o providerId do token == providerId do payload.
 */
@Injectable()
export class NoopAuthGuard implements CanActivate {
  canActivate(): boolean {
    return true;
  }
}
