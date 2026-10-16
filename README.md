# Distributed Wagering Processor

Serviço financeiro que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) vindas de múltiplos provedores, por HTTP e por SQS FIFO.
Cada transação altera saldo, ledger imutável, inbox e outbox na mesma transação SQL, com lock pessimista por wallet.
Idempotência é persistente (`Idempotency-Key` + `payloadHash` no PostgreSQL); unicidade, imutabilidade e não-negatividade estão no schema.
Eventos de integração saem por transactional outbox para `wager-events.fifo`; referências fora de ordem ficam `PENDING_REFERENCE` e são reprocessadas por um worker.
Roda com N instâncias simultâneas; os testes de concorrência sobem processos reais (`bun src/main.ts`) contra PostgreSQL e LocalStack.

Decisões, trade-offs e limitações: [ARCHITECTURE.md](ARCHITECTURE.md).

## Stack

Bun 1.4 · TypeScript strict · NestJS 10 (Express) · MikroORM 6 (`EntitySchema`) · PostgreSQL 16 · SQS FIFO via LocalStack 3.8 · Docker Compose · zod · decimal.js · pino · prom-client.

## Pré-requisitos

| Ferramenta | Observação |
|---|---|
| Docker + Docker Compose v2 | Postgres e LocalStack sobem por `docker compose`. No Windows, Docker Desktop com integração WSL ligada para a distro usada. |
| Bun 1.x | Testado com 1.4. Os testes de concorrência executam `bun src/main.ts` via `Bun.spawn`, então o binário `bun` precisa estar no `PATH` do shell que roda os testes. No WSL, instalar o Bun dentro da distro (`curl -fsSL https://bun.sh/install \| bash`) e abrir um shell de login (`bash -l`) para carregar `~/.bun/bin` no `PATH`. |
| curl | Para os exemplos abaixo. `jq` é opcional. |

Portas usadas: `3000` (API), `5432` (Postgres), `4566` (LocalStack).

## Setup

```bash
bun install
cp .env.example .env        # opcional: os defaults de src/config.ts são os mesmos valores
bun run infra:up            # Postgres + LocalStack; espera os healthchecks (as filas são criadas no init do LocalStack)
bun run migration:up        # aplica a migration inicial
bun run dev                 # API em http://localhost:3000, com consumidor SQS, publisher da outbox e worker de PENDING_REFERENCE
```

O Bun carrega `.env` automaticamente; não há `dotenv` no código. `infra:up` só retorna quando o LocalStack está `healthy`, e o healthcheck só passa depois que `infra/localstack-init.sh` criou as três filas:

| Fila | Uso |
|---|---|
| `wager-transactions.fifo` | Entrada de transações. `VisibilityTimeout=30`, redrive para a DLQ com `maxReceiveCount=5`. |
| `wager-transactions-dlq.fifo` | DLQ: redrive do SQS e envio explícito de mensagens com erro permanente. |
| `wager-events.fifo` | Eventos de integração publicados pela outbox. Nenhum consumidor deste repositório lê essa fila. |

## Variáveis de ambiente

Lidas em `src/config.ts` (sob demanda, para que testes e processos filhos possam sobrescrever) e `src/infrastructure/observability/logger.ts`.

| Variável | Default | Descrição |
|---|---|---|
| `PORT` | `3000` | Porta HTTP. |
| `DATABASE_URL` | `postgres://wager:wager@localhost:5432/wager` | Conexão Postgres. |
| `DB_POOL_MAX` | `20` | Tamanho máximo do pool por instância. O compose sobe o Postgres com `max_connections=200`. |
| `SQS_ENDPOINT` | `http://localhost:4566` | Endpoint do LocalStack. |
| `AWS_REGION` | `us-east-1` | Região do cliente SQS. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `test` / `test` | Credenciais fictícias aceitas pelo LocalStack. |
| `SQS_TRANSACTIONS_QUEUE` | `wager-transactions.fifo` | Fila consumida. |
| `SQS_TRANSACTIONS_DLQ` | `wager-transactions-dlq.fifo` | DLQ (envio explícito e gauge `wager_dlq_depth`). |
| `SQS_EVENTS_QUEUE` | `wager-events.fifo` | Destino da outbox. |
| `CONSUMER_CONCURRENCY` | `10` | Grupos FIFO (`MessageGroupId`) processados em paralelo por instância. |
| `CONSUMER_SHUTDOWN_TIMEOUT_MS` | `20000` | Tempo máximo para concluir mensagens em andamento no `SIGTERM`. |
| `RUN_WORKERS` | `1` | `1` liga consumidor SQS, publisher da outbox e worker de `PENDING_REFERENCE`; qualquer outro valor sobe só a API. |
| `OUTBOX_POLL_MS` | `500` | Intervalo do publisher da outbox. |
| `PENDING_REFERENCE_POLL_MS` | `1000` | Intervalo do worker de `PENDING_REFERENCE`. |
| `LOG_LEVEL` | `info` | Nível do pino (`trace`, `debug`, `info`, `warn`, `error`). |
| `FAULT_EXIT_BEFORE_ACK` | (vazio) | Somente testes: com `1`, o consumidor chama `process.exit(137)` entre o commit e o `DeleteMessage`. |

`.env.example` lista apenas parte delas (`DB_POOL_MAX`, `SQS_TRANSACTIONS_DLQ` e `CONSUMER_*` usam o default).

## Scripts

| Script | Comando | O que faz |
|---|---|---|
| `dev` | `bun --watch src/main.ts` | API + workers, reinicia ao salvar. |
| `start` | `bun src/main.ts` | API + workers, sem watch. Trata `SIGTERM`/`SIGINT`: fecha o app (consumidor conclui ou devolve mensagens) e sai com código 0. |
| `typecheck` | `tsc --noEmit` | Checagem de tipos em modo estrito (`src` e `test`). |
| `test` | `bun test --timeout 60000 test` | Todas as suítes. |
| `test:unit` | `bun test test/unit` | Domínio, classificação de erros, logger, reconciliação com snapshot simulado. Não precisa de containers. |
| `test:integration` | `bun test --timeout 60000 --max-concurrency 1 test/integration` | Postgres e LocalStack reais. |
| `test:concurrency` | `bun test --timeout 120000 --max-concurrency 1 test/concurrency` | Paralelismo real e processos `bun src/main.ts` reais. |
| `migration:up` | `bun src/infrastructure/mikro-orm/migrate.ts up` | Aplica migrations pendentes. |
| `migration:down` | `bun src/infrastructure/mikro-orm/migrate.ts down` | Desfaz a última migration (hoje há uma só: remove tabelas e funções). |
| `infra:up` | `docker compose up -d --wait postgres localstack` | Sobe Postgres e LocalStack e espera os healthchecks. |
| `infra:down` | `docker compose down -v` | Remove containers e volumes. O Postgres não usa volume nomeado: os dados se perdem. |

### O que cada suíte exige e quanto demora

Medido em WSL2 (Ubuntu 24.04) + Docker Desktop, Bun 1.4, containers já de pé e banco migrado:

| Suíte | Exige | Arquivos / testes | Tempo |
|---|---|---|---|
| `typecheck` | nada | - | ~6,5 s |
| `test:unit` | nada | 6 / 82 | ~0,1 s |
| `test:integration` | Postgres migrado + LocalStack com as filas do init | 7 / 77 | ~35 s |
| `test:concurrency` | Postgres migrado + LocalStack + `bun` no `PATH` (sobe até 4 processos por cenário, em portas livres) | 2 / 44 | ~44 s |
| `test` | tudo acima | 15 / 203 | ~76 s |

Observações:

- As suítes de integração e concorrência usam o mesmo banco do `dev` (`DATABASE_URL`). Cada caso cria wallets/UUIDs novos; nada é apagado. `test/integration/outbox.test.ts` marca como publicadas todas as linhas pendentes da outbox antes de cada caso (`beforeEach`), para enxergar só as próprias mensagens.
- O teste de migrations cria e remove um banco temporário `wager_migration_test`.
- Pare o `bun run dev` antes de rodar os testes: os workers dele publicariam a mesma outbox e disputariam a fila `wager-events.fifo` com `outbox.test.ts`.
- Os testes SQS criam filas FIFO temporárias (com DLQ e redrive próprios) e as apagam no fim.

## Réplicas via Docker Compose

O serviço `app` está no profile `app` (`Dockerfile`: `oven/bun:1.4`, `bun install --frozen-lockfile`, `CMD ["bun", "src/main.ts"]`). Ele não roda migrations: aplique-as antes, pelo host.

```bash
bun run infra:up
bun run migration:up
docker compose --profile app up -d --build --scale app=3
docker compose --profile app ps          # app-1 -> :3000, app-2 -> :3001, app-3 -> :3002 (faixa 3000-3010)
curl -s localhost:3001/health/ready
docker compose --profile app logs -f app

# encerrar só as réplicas (Postgres e LocalStack continuam de pé)
docker compose --profile app stop app && docker compose --profile app rm -f app
```

Verificado: build em ~25 s, três réplicas `healthy` no `/health/ready`, e o cenário da seção 8 com cada aposta em uma réplica (wallet criada em `:3001`, BET de 80.00 em `:3000` e em `:3002` simultâneas) resultou em uma `PROCESSED`, uma `REJECTED INSUFFICIENT_FUNDS`, saldo `20.00`, `version` 2 e reconciliação consistente. Dentro da rede do compose as réplicas usam `postgres:5432` e `localstack:4566`. Pare o `bun run dev` antes (porta 3000). Não há `.dockerignore`: `COPY . .` leva `node_modules` e um eventual `.env` do host para a imagem.

## Exemplos de API

Respostas reais, capturadas com a API rodando (`bun src/main.ts`, workers ligados). IDs são UUIDv7 gerados pelo servidor; os seus serão diferentes. Toda resposta traz `X-Correlation-Id` (o enviado pelo cliente, se válido, ou um novo).

```bash
B=http://localhost:3000
PLAYER=$(cat /proc/sys/kernel/random/uuid)
```

### Criar wallet

```bash
curl -s -X POST $B/wallets -H 'content-type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}"
```
```
HTTP 201
{"id":"01a14704-ea66-74dc-9c3e-7779ffcd7cd1","playerId":"7307c890-2d54-42dd-9747-70ac9d2cb267","balance":{"amount":"100.00","currency":"BRL"},"version":1}
```

Saldo inicial maior que zero gera a transação interna `OPENING` e um lançamento `CREDIT` na mesma transação SQL. Repetir para o mesmo `playerId` + moeda:

```
HTTP 409
{"error":"WALLET_ALREADY_EXISTS","message":"Wallet already exists for this player and currency"}
```

### Consultar wallet

```bash
W=01a14704-ea66-74dc-9c3e-7779ffcd7cd1
curl -s $B/wallets/$W
```
```
HTTP 200
{"id":"01a14704-ea66-74dc-9c3e-7779ffcd7cd1","playerId":"7307c890-2d54-42dd-9747-70ac9d2cb267","balance":{"amount":"100.00","currency":"BRL"},"version":1}
```

### Submeter transações

Função auxiliar usada nos exemplos (kind, externalTransactionId, valor, referência opcional). A chave segue o default recomendado `{providerId}:{externalTransactionId}`.

```bash
tx() {
  local ref=""; [ -n "$4" ] && ref=",\"referenceExternalTransactionId\":\"$4\""
  curl -s -w '\nHTTP %{http_code}\n' -X POST $B/wagering/transactions \
    -H 'content-type: application/json' -H "Idempotency-Key: provider-a:$2" \
    -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$2\",\"playerId\":\"$PLAYER\",\"walletId\":\"$W\",\"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"$1\",\"money\":{\"amount\":\"$3\",\"currency\":\"BRL\"}$ref}"
}
```

| Chamada | Status | Corpo |
|---|---|---|
| `tx BET bet-1 25.00` | 200 | `{"transactionId":"01a14704-ea9a-751f-81e3-56fd48d7348c","status":"PROCESSED","balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}` |
| `tx BET bet-1 25.00` (replay) | 200 | `{"transactionId":"01a14704-ea9a-751f-81e3-56fd48d7348c","status":"PROCESSED","balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":true}` |
| `tx BET bet-1 30.00` (mesma key, payload diferente) | 409 | `{"error":"IDEMPOTENCY_CONFLICT","message":"Idempotency key or external transaction id already used with a different payload"}` |
| `tx WIN win-1 40.00 bet-1` | 200 | `{"transactionId":"01a14704-eacf-...","status":"PROCESSED","balance":{"amount":"115.00","currency":"BRL"},"idempotentReplay":false}` |
| `tx LOSS loss-1 25.00 bet-1` | 200 | `{"transactionId":"01a14704-eae5-...","status":"PROCESSED","balance":{"amount":"115.00","currency":"BRL"},"idempotentReplay":false}` (saldo inalterado, sem ledger) |
| `tx BET bet2-1 20.00` | 200 | `... "balance":{"amount":"95.00","currency":"BRL"} ...` |
| `tx REFUND refund-1 20.00 bet2-1` | 200 | `... "status":"PROCESSED","balance":{"amount":"115.00","currency":"BRL"} ...` |
| `tx BET bet3-1 10.00` | 200 | `... "balance":{"amount":"105.00","currency":"BRL"} ...` |
| `tx ROLLBACK rb-1 10.00 bet3-1` | 200 | `... "status":"PROCESSED","balance":{"amount":"115.00","currency":"BRL"} ...` (ROLLBACK de BET credita) |
| `tx ROLLBACK rb2-1 10.00 bet3-1` (segunda reversão) | 422 | `{"transactionId":"01a14704-eb44-...","status":"REJECTED","failureCode":"REFERENCE_ALREADY_REVERSED","balance":{"amount":"115.00","currency":"BRL"},"idempotentReplay":false}` |
| `tx BET big-1 1000.00` (saldo insuficiente) | 422 | `{"transactionId":"01a14704-eb56-7236-9ae3-78e1f899b68b","status":"REJECTED","failureCode":"INSUFFICIENT_FUNDS","balance":{"amount":"115.00","currency":"BRL"},"idempotentReplay":false}` |
| `tx BET big-1 1000.00` (replay da rejeição) | 422 | mesmo corpo, `"idempotentReplay":true` |
| `tx REFUND early-1 5.00 nao-chegou-1` (referência ausente) | 202 | `{"transactionId":"01a14704-eb70-7620-982d-ed80996e9d48","status":"PENDING_REFERENCE","idempotentReplay":false}` |
| `tx BET bad-1 10.001` | 400 | `{"error":"INVALID_PAYLOAD","message":"Invalid amount: expected non-negative decimal string with up to 2 decimals"}` |
| sem header `Idempotency-Key` | 400 | `{"error":"INVALID_PAYLOAD","message":"Invalid request","details":[{"code":"invalid_type","expected":"string","received":"undefined","path":[],"message":"Required"}]}` |
| `kind: "OPENING"` | 400 | `{"error":"INVALID_PAYLOAD","message":"Invalid kind: OPENING"}` |
| `walletId` inexistente | 422 | `{"status":"REJECTED","failureCode":"WALLET_NOT_FOUND","idempotentReplay":false}` (não persistida, sem `transactionId`) |

O `balance` de uma resposta é o saldo da wallet logo após aquela transação (ou o saldo observado no momento da rejeição); o replay devolve exatamente esse valor, mesmo que o saldo atual seja outro. Uma transação `PENDING_REFERENCE` volta a ser avaliada pelo worker com backoff de 5 s, 10 s, 20 s... (ver ARCHITECTURE.md, seção m).

### Consultar transação

```bash
curl -s $B/wagering/transactions/01a14704-ea9a-751f-81e3-56fd48d7348c
curl -s $B/providers/provider-a/wagering/transactions/bet-1
```
```
HTTP 200   (as duas devolvem o mesmo corpo)
{"transactionId":"01a14704-ea9a-751f-81e3-56fd48d7348c","status":"PROCESSED","balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false,"providerId":"provider-a","externalTransactionId":"bet-1","walletId":"01a14704-ea66-74dc-9c3e-7779ffcd7cd1","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"createdAt":"2026-10-16T23:21:06.978Z"}
```
Transação inexistente: `404 {"error":"NOT_FOUND","message":"Transaction not found"}`.

### Ledger com cursor

```bash
curl -s "$B/wallets/$W/ledger?limit=2"
```
```
HTTP 200
{"items":[
  {"id":"01a14704-ea6b-...","transactionId":"01a14704-ea6b-...","direction":"CREDIT","money":{"amount":"100.00","currency":"BRL"},"balanceBefore":{"amount":"0.00","currency":"BRL"},"balanceAfter":{"amount":"100.00","currency":"BRL"},"createdAt":"2026-10-16T23:21:06.918Z"},
  {"id":"01a14704-eaa2-...","transactionId":"01a14704-ea9a-751f-81e3-56fd48d7348c","direction":"DEBIT","money":{"amount":"25.00","currency":"BRL"},"balanceBefore":{"amount":"100.00","currency":"BRL"},"balanceAfter":{"amount":"75.00","currency":"BRL"},"createdAt":"2026-10-16T23:21:06.978Z"}],
 "nextCursor":"eyJzIjoiMjIwNzAifQ"}
```
```bash
curl -s "$B/wallets/$W/ledger?limit=2&cursor=eyJzIjoiMjIwNzAifQ"
```
```
HTTP 200
{"items":[{... "direction":"CREDIT","money":{"amount":"40.00",...},"balanceBefore":{"amount":"75.00",...},"balanceAfter":{"amount":"115.00",...}},
          {... "direction":"DEBIT","money":{"amount":"20.00",...},"balanceBefore":{"amount":"115.00",...},"balanceAfter":{"amount":"95.00",...}}],
 "nextCursor":"eyJzIjoiMjIwNzIifQ"}
```
`limit` vai de 1 a 200 (default 50); `nextCursor` é `null` na última página. Cursor inválido: `400 {"error":"INVALID_PAYLOAD","message":"Invalid cursor"}`.

### Reconciliação

```bash
curl -s -X POST $B/wallets/$W/reconciliation
```
```
HTTP 200
{"walletId":"01a14704-ea66-74dc-9c3e-7779ffcd7cd1","storedBalance":{"amount":"115.00","currency":"BRL"},"calculatedBalance":{"amount":"115.00","currency":"BRL"},"difference":{"amount":"0.00","currency":"BRL"},"consistent":true,"checkedEntries":7}
```
Com divergência a resposta continua 200, com `consistent: false` (e `chainBroken: true` se a cadeia `balance_before`/`balance_after` tiver buraco); nada é corrigido.

### Health e métricas

```bash
curl -si $B/health/live
```
```
HTTP/1.1 200 OK
X-Correlation-Id: 01a14704-ebf0-75bc-9ed2-8ebf1815f645
Content-Type: application/json; charset=utf-8

{"status":"ok"}
```
```bash
curl -s $B/health/ready
```
```
HTTP 200
{"status":"ok","checks":{"postgres":"up","sqs":"up"}}
```
Com Postgres ou SQS fora: `503 {"status":"unavailable","checks":{"postgres":"up","sqs":"down"}}`.

```bash
curl -s $B/metrics | grep '^wager_'
```
```
wager_transactions_total{status="PROCESSED",kind="BET"} 3
wager_transactions_total{status="REJECTED",kind="BET"} 2
wager_transactions_total{status="PENDING_REFERENCE",kind="REFUND"} 1
wager_transactions_rejected_total{failure_code="INSUFFICIENT_FUNDS"} 1
wager_transactions_rejected_total{failure_code="WALLET_NOT_FOUND"} 1
wager_duplicates_total{source="http"} 2
wager_idempotency_conflicts_total 1
wager_processing_duration_seconds_bucket{le="0.01",source="http"} 9
wager_processing_duration_seconds_count{source="http"} 16
wager_outbox_pending 18
wager_outbox_lag_seconds 0.425267
wager_dlq_depth 1
...
```
Contadores com labels só aparecem depois do primeiro incremento (ex.: `wager_retries_total`, `wager_lock_conflicts_total`). Lista completa em ARCHITECTURE.md, seção n.

## Exemplo via SQS

O corpo segue o envelope da seção 10 do desafio; `data` é o mesmo corpo do HTTP mais `idempotencyKey`. `correlationId` no envelope é opcional.

```bash
Q=http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo
DLQ=http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions-dlq.fifo
BODY="{\"messageId\":\"msg-1\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"2026-10-16T15:00:00.000Z\",\"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"sqs-bet-1\",\"idempotencyKey\":\"provider-a:sqs-bet-1\",\"playerId\":\"$PLAYER\",\"walletId\":\"$W\",\"roundId\":\"round-2\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"5.00\",\"currency\":\"BRL\"}}}"

docker compose exec localstack awslocal sqs send-message --queue-url $Q \
  --message-body "$BODY" --message-group-id "$W" --message-deduplication-id msg-1
```
```json
{ "MD5OfMessageBody": "52dc043132aa947c15880bb5d7f4df25", "MessageId": "6fab5fe8-ef90-4eaf-aeae-a1271f94c2ea", "SequenceNumber": "15394783511953049399" }
```
Use o `walletId` como `MessageGroupId` (ordem por wallet). Resultado, consultado em seguida:

```
curl -s $B/providers/provider-a/wagering/transactions/sqs-bet-1
{"transactionId":"01a14704-efb6-7454-a24c-91a98c776d00","status":"PROCESSED","balance":{"amount":"110.00","currency":"BRL"},"idempotentReplay":false,"providerId":"provider-a","externalTransactionId":"sqs-bet-1",...}
```

Reenviar o mesmo corpo com outro `--message-deduplication-id` (o broker não deduplica; o inbox sim) gera no log `"msg":"duplicate message, acking"` com o mesmo `transactionId`, e o saldo não muda.

Mensagem com erro permanente (JSON inválido) vai para a DLQ explicitamente, com atributos de diagnóstico:

```bash
docker compose exec localstack awslocal sqs send-message --queue-url $Q \
  --message-body '{"messageId":"quebrado"' --message-group-id "$W" --message-deduplication-id broken-1

docker compose exec localstack awslocal sqs get-queue-attributes --queue-url $DLQ --attribute-names ApproximateNumberOfMessages
docker compose exec localstack awslocal sqs receive-message --queue-url $DLQ \
  --max-number-of-messages 10 --message-attribute-names All --visibility-timeout 0
```
```json
{
  "Body": "{\"messageId\":\"quebrado\"",
  "MessageAttributes": {
    "reason":       { "StringValue": "JSON Parse error: Expected '}'", "DataType": "String" },
    "errorName":    { "StringValue": "SyntaxError", "DataType": "String" },
    "receiveCount": { "StringValue": "1", "DataType": "Number" },
    "sqsMessageId": { "StringValue": "5b578b85-b21d-48d9-a9cc-8f5b01ffcfd1", "DataType": "String" }
  }
}
```
Mensagens que esgotam `maxReceiveCount=5` por falha transitória chegam à DLQ pela redrive policy, sem esses atributos. Para esvaziar: `awslocal sqs purge-queue --queue-url $DLQ`. Eventos publicados pela outbox podem ser lidos de `wager-events.fifo` com o mesmo `receive-message`.

## Estrutura

```
src/
  main.ts, bootstrap.ts, app.module.ts, config.ts
  domain/            Money, Wallet, WagerTransaction, WalletLedgerEntry, InboxMessage, OutboxMessage,
                     eventos de integração, FailureCode, payloadHash, erros de domínio (sem Nest/ORM)
  application/       casos de uso (abrir wallet, submeter transação, reconciliar, consultas),
                     núcleo financeiro (wager-processor.ts), portas (ports.ts), erros de aplicação
  infrastructure/
    http/            controllers, schemas zod, filtro de erros, guard no-op, correlationId
    mikro-orm/       EntitySchemas, mappers, repositórios, UnitOfWork, migration, migrate.ts
    sqs/             cliente SQS e consumidor de wager-transactions.fifo
    workers/         publisher da outbox, worker de PENDING_REFERENCE, PollLoop
    observability/   logger pino, métricas prom-client
test/
  unit/              sem I/O
  integration/       Postgres e LocalStack reais
  concurrency/       paralelismo real; phase6 sobe processos bun src/main.ts
  support/           invariante saldo==ledger, cluster de processos, filas temporárias
infra/localstack-init.sh   cria as filas e a redrive policy
```

## Testes

203 testes (82 unidade, 77 integração, 44 concorrência). Nenhum substitui Postgres ou SQS por mock: os testes de integração e concorrência rodam contra os containers. Falhas são injetadas em pontos específicos (um `UnitOfWork` que lança erro transitório, um 40P01 artificial, uma subclasse do publisher que "morre" antes de marcar, o gancho `beforeAck` e `FAULT_EXIT_BEFORE_ACK`), sempre com fila, banco e caso de uso reais.

A invariante final `wallet.balance == saldo reconstruído pelo ledger` está em `test/support/invariants.ts` (`assertWalletConsistent`): saldo não negativo, um lançamento por transação, cadeia `balance_before`/`balance_after` contínua a partir de 0, soma CREDIT - DEBIT igual ao saldo e `version == 1 + lançamentos após a OPENING`. É chamada nos testes de concorrência, de PENDING_REFERENCE, do consumidor SQS e de reconciliação.

### Mapa requisito -> teste

| Requisito (seção 13 do desafio) | Arquivo | Teste |
|---|---|---|
| Cenário obrigatório da seção 8 (100.00, duas BETs de 80.00) | `test/concurrency/phase4.test.ts` | `seção 8 (uc) > duas BETs de 80 em wallet de 100 (rodada %i)` (20 rodadas) e `seção 8 (http) > ...` |
| Seção 8 entre processos, com retry em outra instância | `test/concurrency/phase6.test.ts` | `1. quatro instâncias HTTP: 80+80, mesma key x50, hot wallet e 20 wallets em paralelo` (parte a) |
| Mesma aposta 50x em paralelo | `test/concurrency/phase4.test.ts` | `mesma Idempotency-Key x50 (uc\|http) > um único débito, 49 replays` |
| 50x espalhada por 4 processos | `test/concurrency/phase6.test.ts` | cenário `1.` (parte b) |
| Mesma key, payloads diferentes em paralelo | `test/concurrency/phase4.test.ts` | `mesma key, payloads diferentes (uc\|http) > um payload vence, o outro recebe conflito` |
| Wallets distintas em paralelo | `test/concurrency/phase4.test.ts` | `30 wallets x 3 BETs simultâneas`; cenário `1.` (parte d) |
| Hot wallet | `test/concurrency/phase4.test.ts` | `hot wallet: 100 BETs de 1.00 em 60.00`; cenário `1.` (parte c, 200 BETs em 4 processos) |
| Dupla reversão concorrente | `test/concurrency/phase4.test.ts` | `REFUND x ROLLBACK da mesma BET (rodada %i)` |
| >= 3 processos/instâncias | `test/concurrency/phase6.test.ts` | `1.` (4 HTTP), `2. quatro instâncias consumindo SQS...`, `5.` (3), `6.` (3, com reinícios) |
| Worker morto depois do commit e antes do ack | `test/concurrency/phase6.test.ts` | `3. instância morre depois do commit e antes do ack: outra recebe a redelivery como duplicata` |
| Idem, no consumidor em processo | `test/integration/sqs-consumer.test.ts` | `falha entre commit e ack => redelivery cai no inbox, sem duplo débito` |
| Dois publishers sobre a mesma outbox | `test/concurrency/phase6.test.ts` | `4. publishers em processos distintos sobre a mesma outbox; um morre com SIGKILL no meio` |
| Publishers concorrentes / crash após o send | `test/integration/outbox.test.ts` | `publishers concorrentes: cada mensagem publicada exatamente uma vez, nenhuma perdida`; `crash depois do send e antes do commit: segue pendente e é republicada com o mesmo eventId` |
| REFUND/ROLLBACK antes da referência | `test/concurrency/phase6.test.ts` | `5. referências fora de ordem entre processos: workers de 3 instâncias resolvem sem duplicar` |
| Idem + limite de tentativas | `test/integration/pending-reference.test.ts` | `REFUND, ROLLBACK e WIN chegam antes da BET...`; `referência nunca chega: após MAX tentativas => REJECTED REFERENCE_NOT_FOUND + evento`; `workers concorrentes sobre os mesmos pendentes não duplicam efeito` |
| Reinício com consistência final | `test/concurrency/phase6.test.ts` | `6. reinício com carga contínua (HTTP + SQS): SIGKILL de uma, de todas, retry total => consistência final` |
| SIGTERM | `test/concurrency/phase6.test.ts`; `test/integration/sqs-consumer.test.ts` | `7. SIGTERM com mensagens em voo: sai com 0, nada perdido nem duplicado`; `consumidor SQS: graceful shutdown > ...` |
| Invariante saldo == ledger | `test/support/invariants.ts` | `assertWalletConsistent`, chamado em 5 arquivos de teste |
| Migrations e constraints | `test/integration/schema-constraints.test.ts` | CHECKs, UNIQUEs, FKs compostas, `wt_single_reversal_uq`, triggers, `migrations reversíveis > up -> 6 tabelas -> down -> ... -> up` |
| Atomicidade wallet/ledger/inbox/outbox | `test/integration/outbox.test.ts` | `BET grava eventos na mesma transação; falha depois do insert da outbox não deixa nada` |
| Inbox e redelivery | `test/integration/sqs-consumer.test.ts` | `mesma mensagem 2x ...`; `mesmo externalTransactionId e key, messageId novo ...` |
| Retry e DLQ | `test/integration/sqs-consumer.test.ts` | `erro transitório => visibilidade com backoff, sem delete; após maxReceiveCount vai à DLQ por redrive`; `JSON quebrado, schema inválido, OPENING e messageId reaproveitado com outro payload => DLQ explícita` |
| Money, Wallet, regras por kind, conflito de moeda, payload divergente | `test/unit/money.test.ts`, `wallet.test.ts`, `wager-transaction.test.ts` | ver `describe` de cada arquivo |
| Observabilidade | `test/integration/observability.test.ts`, `test/unit/observability.test.ts` | métricas mínimas, correlationId nos logs, ausência de valores monetários nos logs, reconciliação com divergência |

Os cenários de `phase6` imprimem um resumo ao final, por exemplo `[cenário 6] http=208 sqs=104 instâncias=7` e `[cenário 7] SIGTERM -> exit 0 em 41ms; processadas antes=6, depois=54`.
