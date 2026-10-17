# Teste de carga

`bun run test:load` (`test/load/load-test.ts`). Sobe instâncias reais da aplicação, gera carga HTTP (e SQS) contra elas, coleta métricas do cliente e do `/metrics` e, ao fim de cada cenário, confere as invariantes financeiras no banco. Se alguma invariante falhar, o script sai com código 1.

Resumo do que foi medido (detalhes e ressalvas nas seções abaixo):

- **Correção**: 0 falhas de invariante em todas as execuções (mais de 40 execuções de cenário somando todos os experimentos). Banco e cliente bateram transação a transação; nenhuma key duplicada, nenhum saldo negativo, nenhum replay divergente.
- **Erros**: 0 respostas 5xx. 1 erro de rede em ~150 mil requisições (seção 6.4), sem efeito no banco.
- **Throughput** nesta máquina, com aplicação, banco, LocalStack e gerador de carga dividindo o mesmo host: ~210 req/s com 1 instância e ~375 req/s com 3 (1,76x) no cenário de wallets espalhadas; ~110-135 BETs/s numa hot wallet, sem ganho com mais instâncias.
- **Conflitos de lock**: 0 (`wager_lock_conflicts_total` e `wager_retries_total{component="unit_of_work"}`). As esperas de lock existiram, mas nenhuma chegou ao `lock_timeout` de 3 s. A métrica atual não mede espera.
- **Outbox**: sob carga sustentada o publisher não acompanha a geração de eventos. O lag cresce ~1 s por segundo de carga e a fila drena só depois que a carga para.
- **Dois defeitos reais encontrados e corrigidos** (seção 5): o claim da outbox varria a tabela inteira (211 ms por ciclo com ~314 mil linhas) e o publisher tinha teto fixo de 50 eventos/s por instância.

## 1. Ambiente

Tudo na mesma máquina física. Medido com os comandos abaixo; o script registra o mesmo no JSON de resultados.

| Item | Valor | Comando |
|---|---|---|
| CPU | AMD Ryzen 7 5700, 8 núcleos / 16 threads | `lscpu` |
| CPUs visíveis no WSL2 | 16 | `nproc` |
| RAM do WSL2 | 7871 MB total, ~6 GB disponíveis; swap 2 GB | `free -m` |
| SO | Windows 11 Pro + WSL2, Ubuntu 24.04.4 LTS, kernel 6.18.33.2-microsoft-standard-WSL2 | `uname -r`, `/etc/os-release` |
| Docker | Docker Desktop 29.6.2 (engine e cliente), 16 CPUs e 7,7 GiB para a VM | `docker version`, `docker info` |
| Bun | 1.4.3 | `bun --version` |
| Postgres | 16.15 (alpine), `max_connections=200`, `fsync=on`, `synchronous_commit=on`, `shared_buffers=128MB` | `psql -c "show ..."` |
| LocalStack | 3.8 (somente SQS) | `docker-compose.yml` |
| Latência app -> Postgres | `SELECT 1`: 0,21 ms por ida e volta; `INSERT` em autocommit (com fsync): 0,83 ms | script descartável com 2000 e 500 iterações |

Postgres e LocalStack rodam no Docker Desktop. As instâncias (`bun src/main.ts`) e o gerador de carga rodam como processos do Ubuntu no WSL2. No WSL2 todos compartilham a mesma VM e as mesmas 16 threads.

## 2. Como reproduzir

```bash
bun run infra:up && bun run migration:up
bun run test:load                                    # A, B, B_LOW e C; 1 e 3 instâncias; ~5 min
env REPEAT=2 DRAIN_TIMEOUT_S=120 bun run test:load   # a execução reportada na seção 4 (~14 min)
env SCENARIOS=A INSTANCES=1,3 CONCURRENCY=64 bun run test:load
```

As variáveis estão no README (seção "Teste de carga"). Flags `--nome=valor` equivalem às variáveis (`--duration-s=30` vira `DURATION_S=30`). Cada execução grava `load-results/load-<timestamp>.json` (ignorado no git), com ambiente, configuração, contagens, percentis por tipo de operação, deltas das métricas do servidor, amostras da outbox e o resultado da verificação.

## 3. Metodologia

### Gerador de carga

- Escrito em Bun, sem dependência nova. Usa `fetch` (keep-alive é o padrão do Bun) e roda num único processo.
- **Laço fechado**: `CONCURRENCY` workers virtuais (padrão 32). Cada worker envia uma requisição, espera a resposta e envia a próxima, sem pausa. A vazão é consequência da latência (lei de Little: vazão ≈ concorrência / latência). Não existe taxa de chegada alvo.
- **Distribuição**: round-robin entre as instâncias, por requisição.
- **Aquecimento**: `WARMUP_S` (3 s). A carga já roda, mas as amostras são descartadas. Só entram nas métricas as requisições que *começaram* dentro da janela medida (`DURATION_S`, 15 s). A duração efetiva vai do início da janela até a última resposta e inclui as requisições que terminaram depois do fim.
- **Timeout** por requisição: 10 s. Estouro ou falha de conexão conta como "erro de rede".
- **Latência**: tempo de parede no cliente, do `fetch` até o corpo lido (`performance.now()`). Os percentis são exatos, por posto mais próximo sobre todas as amostras ordenadas (`p = sorted[ceil(q·n) - 1]`), sem histograma.
- **Classificação**: 200, 202, 422, 409, outros 4xx, 5xx, rede. **Taxa de erro** = (5xx + rede) / total. A 422 é rejeição de negócio: aparece à parte e não conta como erro.

### Instâncias

- `Cluster` de `test/support/cluster.ts` (o mesmo helper dos testes multi-processo): `Bun.spawn(['bun', 'src/main.ts'])` em porta livre, pronto quando `/health/ready` responde 200.
- Workers ligados (`RUN_WORKERS=1`): consumidor SQS, publisher da outbox e worker de `PENDING_REFERENCE`.
- Filas FIFO temporárias por cenário (principal com DLQ e fila de eventos), criadas e apagadas por `withQueues` (`test/support/sqs.ts`).
- `DB_POOL_MAX=10` por instância (o padrão da aplicação é 20). As demais variáveis ficam no padrão da aplicação: `LOG_LEVEL=info`, `OUTBOX_POLL_MS=500`, `CONSUMER_CONCURRENCY=10`.
- Os logs (pino, síncrono em stdout) vão por pipe para o processo do gerador, que guarda só a cauda (300 linhas), para diagnóstico.
- Antes de medir, o backlog anterior da outbox (aberturas de wallet, outras suítes) é marcado como publicado. Assim o lag reflete só a carga do cenário.

### Cenários

| Cenário | Carga | Para quê |
|---|---|---|
| **A**: wallets espalhadas | 500 wallets com saldo 1.000.000,00. Mix por requisição: 60% BET (1,00-50,00), 25% WIN referenciando uma BET já confirmada da mesma wallet, 10% LOSS idem, 5% REFUND de uma BET ainda não revertida (valor igual). Se a wallet ainda não tem BET, sai uma BET. Além do mix, 5% das requisições são **reenvios deliberados** (mesma key e mesmo corpo de uma resposta 200/422 recente). Cada operação nova tem `externalTransactionId` UUID e key `load:<id>`. | Caminho normal, referência e idempotência, com pouca contenção. |
| **B**: hot wallet | Toda a carga em uma wallet com saldo 10.000.000,00; BETs de 1,00. | Custo da serialização pelo `SELECT ... FOR UPDATE`. |
| **B_LOW**: hot wallet com saldo baixo | Uma wallet com saldo 100,00; BETs de 1,00. As 100 primeiras passam (ainda no aquecimento); o resto vira 422 `INSUFFICIENT_FUNDS`. | Caminho de rejeição sob contenção; prova de que nenhuma BET passa do saldo. |
| **C**: ingestão SQS | Enfileira 2000 mensagens (BET 1,00, 100 wallets, `MessageGroupId` = wallet) antes de subir as instâncias. Mede o tempo até todas estarem processadas. | Vazão do consumidor. |

Mix realizado (A, 1 instância, execução 1): BET 60,3%, WIN 22,5%, LOSS 9,2%, REFUND 3,2%, REPLAY 4,9%. O REFUND fica abaixo dos 5% porque, sem BET reversível disponível, vira WIN.

A e B rodam com 1 e com 3 instâncias. Na execução reportada, cada combinação rodou 2 vezes.

### Métricas do servidor

O script faz scrape de `/metrics` de todas as instâncias antes e depois da janela e soma os deltas:

- `wager_lock_conflicts_total` (por SQLSTATE) e `wager_retries_total{component="unit_of_work"}`;
- `wager_duplicates_total{source="http"}` (replays vistos pelo servidor);
- `wager_processing_duration_seconds{source="http"}`: média exata (sum/count) e p50/p99 aproximados pelo limite do bucket (buckets grossos: 50, 100, 250, 500 ms...);
- `process_cpu_seconds_total` (núcleos usados por instância) e `process_resident_memory_bytes`;
- `wager_outbox_published_total`.

A outbox é amostrada **a cada 1 s** por `/metrics` (`wager_outbox_lag_seconds`, `wager_outbox_pending`). O script registra o máximo durante a carga, o valor no fim e o tempo até `pending = 0` depois da carga (consulta direta, a cada 250 ms, até `DRAIN_TIMEOUT_S`).

CPU de host e containers:
- **Host**: delta de `/proc/stat`. No WSL2 a VM é compartilhada, então o número inclui os containers.
- **Postgres e LocalStack**: `usage_usec` do cgroup de cada container.
- **Gerador**: `process.cpuUsage()`.

### Verificação de correção (ao fim de cada execução)

A carga só vale se tudo abaixo passar:

1. `assertWalletConsistent` em **todas** as wallets do cenário (500 em A, 1 em B, 100 em C). Saldo ≥ 0, um lançamento por transação, cadeia `balance_before -> balance_after` contínua a partir de 0, soma dos lançamentos = saldo, `version` coerente.
2. Banco × cliente:
   - todo `transactionId` respondido com 200 existe no banco como `PROCESSED`, e todo 422 existe como `REJECTED`;
   - o banco tem no máximo tantas transações a mais quanto as respostas indeterminadas (5xx ou rede).
   - O aquecimento entra nessa conta.
3. Nenhuma `idempotency_key` repetida e nenhuma transação com mais de um lançamento.
4. Nenhum lançamento com `balance_before`/`balance_after` < 0 e nenhuma wallet negativa.
5. Todo reenvio deliberado devolveu o mesmo status e o mesmo `transactionId` com `idempotentReplay: true`.
6. B: saldo = inicial − BETs processadas. B_LOW: exatamente 100 BETs processadas e saldo 0,00.
7. C: inbox com as 2000 mensagens processadas, débitos por wallet iguais aos enviados, fila e DLQ vazias.
8. Nenhuma instância morreu durante a execução.

## 4. Resultados

Execução de referência: `REPEAT=2 DRAIN_TIMEOUT_S=120`, padrões nos demais (`CONCURRENCY=32`, 15 s medidos + 3 s de aquecimento). Feita em 2026-10-17 03:18 UTC, com as duas correções da seção 5 já aplicadas. Arquivo: `load-results/load-2026-10-17T03-18-28-669Z.json`. **Todas as 16 execuções passaram na verificação.** Em todas: 0 respostas 5xx, 0 erros de rede, 0 respostas 409, 0 conflitos de lock e 0 retries.

### A: wallets espalhadas

| Inst. | Exec. | Req. | req/s | p50 | p95 | p99 | max (ms) | Serviço médio (ms) | CPU por inst. (núcleos) | CPU Postgres | CPU host |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 3200 | 211,8 | 144,8 | 211,9 | 241,0 | 291,6 | 149,0 | 1,57 | 0,60 | 18,8% |
| 1 | 2 | 3247 | 214,5 | 142,6 | 216,6 | 244,5 | 282,7 | 146,8 | 1,55 | 0,60 | 18,1% |
| 3 | 1 | 5611 | 372,0 | 68,2 | 171,4 | 204,2 | 324,7 | 80,6 | 1,58 / 1,57 / 1,58 | 1,30 | 43,1% |
| 3 | 2 | 5674 | 376,4 | 61,4 | 182,1 | 212,2 | 280,0 | 79,9 | 1,58 / 1,55 / 1,57 | 1,29 | 43,0% |

- Todas as respostas foram 200 (0 rejeições de negócio).
- Replays conferidos: 171 / 197 / 328 / 301, todos idênticos ao original.
- Latência do replay (3 instâncias, exec. 1): p50 45,5 ms, p99 155,7 ms; de uma BET nova: p50 67,9 ms, p99 201,4 ms.
- Variação entre as duas execuções: 1,3% (1 inst.) e 1,2% (3 inst.) em req/s.
- Memória: ~190-205 MB de RSS por instância.

| Inst. | Exec. | Outbox: lag máx. na carga | Pendentes máx. | Publicados/s na carga | Drenagem após a carga |
|---|---|---|---|---|---|
| 1 | 1 | 15,5 s | 5651 | 60 | 23,3 s |
| 1 | 2 | 15,2 s | 5961 | 59 | 24,8 s |
| 3 | 1 | 14,3 s | 9067 | 145 (soma) | 22,1 s |
| 3 | 2 | 13,4 s | 9093 | 168 (soma) | 22,1 s |

### B: hot wallet, saldo alto

| Inst. | Exec. | Req. | req/s | p50 | p95 | p99 | max (ms) | Serviço médio (ms) | CPU por inst. | CPU Postgres | Lag máx. | Drenagem |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 1907 | 125,4 | 245,6 | 303,9 | 331,1 | 362,8 | 257,7 | 1,12 | 0,46 | 12,2 s | 12,1 s |
| 1 | 2 | 1888 | 124,0 | 249,0 | 302,7 | 336,8 | 347,0 | 261,4 | 1,13 | 0,42 | 11,2 s | 11,6 s |
| 3 | 1 | 1686 | 110,8 | 285,4 | 372,1 | 505,6 | 548,1 | 294,9 | 0,67 / 0,63 / 0,63 | 0,60 | 1,2 s | 0,25 s |
| 3 | 2 | 1702 | 111,2 | 286,5 | 368,4 | 423,2 | 600,5 | 298,5 | 0,64 / 0,63 / 0,62 | 0,59 | 0,6 s | 0 s |

### B_LOW: hot wallet, saldo 100,00

| Inst. | Exec. | Req. (todas 422) | req/s | p50 | p95 | p99 | max (ms) | Serviço médio (ms) | Lag máx. |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 2508 | 165,5 | 188,2 | 232,8 | 268,5 | 300,5 | 197,3 | 7,9 s |
| 1 | 2 | 2518 | 166,1 | 188,8 | 233,3 | 262,0 | 274,2 | 197,7 | 7,7 s |
| 3 | 1 | 2657 | 174,5 | 178,2 | 229,5 | 266,2 | 313,4 | 194,6 | 0,9 s |
| 3 | 2 | 2642 | 174,2 | 177,3 | 227,1 | 247,4 | 268,4 | 195,8 | 0,6 s |

Em todas as execuções: exatamente 100 BETs processadas, saldo final 0,00 e as demais persistidas como `REJECTED INSUFFICIENT_FUNDS` (por exemplo, 2892 transações no banco = 100 + 2792). Taxa de erro 0%; 100% de 422 na janela medida, porque as 100 aprovadas ocorreram no aquecimento.

### C: ingestão SQS (2000 mensagens, 100 grupos)

| Inst. | Exec. | Enfileirar (msg/s) | Consumo (msg/s, pelo `processed_at` do inbox) | Tempo de parede desde o spawn | Por instância | Duplicatas | Drenagem da outbox |
|---|---|---|---|---|---|---|---|
| 1 | 1 | 3936 | 61,6 (32,5 s) | 32,4 s | 2000 | 0 | 0,25 s |
| 1 | 2 | 3707 | 66,6 (30,0 s) | 30,1 s | 2000 | 0 | 0 s |
| 3 | 1 | 3642 | 105,1 (19,0 s) | 19,1 s | 650 / 670 / 680 | 0 | 0 s |
| 3 | 2 | 4542 | 117,2 (17,1 s) | 17,9 s | 660 / 670 / 670 | 0 | 0 s |

A vazão vem da diferença entre o primeiro e o último `processed_at` no inbox, um relógio só (o do banco). As mensagens foram enfileiradas antes de subir as instâncias. Assim a primeira instância a ficar pronta começa sozinha por alguns centésimos de segundo.

### 1 vs 3 instâncias (execução de referência)

| Cenário | 1 instância | 3 instâncias | Razão |
|---|---|---|---|
| A (req/s) | 213 | 374 | **1,76x** |
| B (req/s) | 125 | 111 | **0,89x** (piorou) |
| B_LOW (req/s) | 166 | 174 | 1,05x |
| C (msg/s) | 64 | 111 | 1,73x |

## 5. Defeitos encontrados e corrigidos

### 5.1 Claim da outbox varria a tabela inteira

**Sintoma.** Nas primeiras execuções a outbox não drenava: 8351 e 9503 eventos pendentes 60 s depois do fim da carga em A. Além disso, a CPU do Postgres subia com o tamanho da tabela.

**Causa.** `EXPLAIN ANALYZE` do claim (`... WHERE published_at IS NULL AND next_attempt_at <= now() ORDER BY seq LIMIT 25 FOR UPDATE SKIP LOCKED`), com ~314 mil linhas na outbox: o planner escolhia `outbox_messages_seq_key` (o índice único de `seq`) e filtrava as linhas uma a uma. Resultado: `Rows Removed by Filter: 313858` e **211 ms por claim**. O índice parcial existente, `(next_attempt_at, seq) WHERE published_at IS NULL`, não entrega a ordem de `seq`, então perdia para o índice que entrega. Os pendentes ficam no fim de `seq`. Por isso o custo cresce com o histórico publicado, que nunca é limpo. Em produção isso piora a cada dia, de forma silenciosa.

**Correção.** Migration `Migration20261017000003_outbox_pending_seq_idx`: cria `outbox_pending_seq_idx ON outbox_messages (seq) WHERE published_at IS NULL` e remove o índice anterior (`down` faz o inverso). Mesmo claim, mesma tabela: **0,23 ms**. A contagem do backlog usada pelo gauge também passou a usar esse índice (0,04 ms).

**Teste.** `test/integration/outbox.test.ts > claim da outbox usa o índice parcial dos pendentes` faz `EXPLAIN` do `CLAIM_SQL` (agora exportado) e exige o índice parcial. Com a migration desfeita, o teste falha; com ela, passa. O teste de migrations agora desfaz três migrations.

### 5.2 Publisher com teto fixo de 50 eventos/s por instância

**Causa.** O `PollLoop` sempre esperava `OUTBOX_POLL_MS` (500 ms) entre ciclos, mesmo depois de um lote cheio. Teto: 25 / 0,5 s = 50 eventos/s por instância, qualquer que fosse o backlog. Cada transação de saldo gera 2 eventos.

**Correção.** Se o ciclo devolve `true`, o próximo começa sem esperar (`src/infrastructure/workers/poll.ts`). O publisher devolve `true` quando publicou um lote cheio. Falhas entram em backoff e não contam, então um SQS fora do ar não vira laço quente. Teste: `test/unit/poll.test.ts` (emenda ciclos com `true`; com erro ou retorno diferente de `true`, respeita o intervalo).

### Efeito medido das duas correções

Mesma máquina, `CONCURRENCY=32`, workers ligados:

| | Só antes de 5.2 (índice antigo) | Com 5.2, índice antigo | Com 5.1 e 5.2 |
|---|---|---|---|
| A, 1 inst.: drenagem pós-carga | > 60 s (8351 pendentes) | 38,9 / 37,9 s | 23,3 / 24,8 s |
| A, 3 inst.: drenagem pós-carga | > 60 s (9503 pendentes) | 26,4 / 28,5 s | 22,1 / 22,1 s |
| B, 1 inst.: drenagem pós-carga | > 60 s (3074 pendentes) | 20,9 / 23,0 s | 12,1 / 11,6 s |
| A, 3 inst.: CPU do Postgres | 1,74 | 1,84 / 1,82 | 1,30 / 1,29 |
| B, 3 inst.: CPU do Postgres | 0,82 | 1,45 / 0,76 | 0,60 / 0,59 |
| A: req/s (1 / 3 inst.) | 360 / 586 (\*) | 289, 253 / 409, 416 | 212, 215 / 372, 376 |

(\*) Essa coluna vem de uma sessão anterior, ~3 horas antes. Os req/s **não** são comparáveis entre colunas: há deriva da máquina ao longo da sessão (seção 6.3). Uma repetição do código antigo feita logo antes da coluna do meio deu 246/230 (1 inst.) e 412/404 (3 inst.), praticamente igual à coluna do meio. Conclusão: as correções reduziram a CPU do banco e o tempo de drenagem; não mudaram de forma mensurável a vazão da API, que é limitada por outra coisa (seção 6.1).

## 6. Análise

### 6.1 Onde está o gargalo (cenário A)

- **A instância está saturada de CPU.** Em todas as execuções de A, cada instância usa ~1,55-1,6 núcleo, um número estável que não sobe com mais carga. O Bun executa JavaScript numa thread; o excedente é GC e threads auxiliares. A varredura de concorrência confirma: a vazão para de subir já com concorrência baixa, e o que sobe é a latência.

  | Concorrência | 1 inst.: req/s | p50 (ms) | 3 inst.: req/s | p50 (ms) |
  |---|---|---|---|---|
  | 8 | 200 | 37 | 331 | 22 |
  | 32 (referência) | 212-215 | 143-145 | 372-376 | 61-68 |
  | 64 | 215 | 286 | 361 | 91 |
  | 128 | 223 | 557 | 360 | 384 |

  Com 1 instância, 8 requisições em voo já levam a ~200 req/s; de 8 para 128 a vazão sobe 11% e o p50 multiplica por 15. É o comportamento de um servidor saturado num laço fechado: a fila cresce dentro do processo.
- **Custo de CPU por requisição alto e não perfilado**: ~1,56 núcleo / 213 req/s ≈ 7 ms de CPU por requisição numa instância. No fio para o banco há ~10 comandos curtos (`SET LOCAL`, `FOR UPDATE`, busca de idempotência, inserts de transação/ledger/outbox, `UPDATE` da wallet, commit) a 0,2 ms de ida e volta. O resto é a pilha da aplicação: Nest/express sobre a camada de compatibilidade do Bun, zod, MikroORM (fork do EntityManager e hidratação por requisição), pino síncrono, prom-client e o publisher no mesmo event loop. Não fiz profiling; a divisão entre essas partes é desconhecida.
- **O banco não é o gargalo** nesta faixa: 0,6 núcleo com 1 instância e 1,3 com 3. O host fica em 18-45% de ocupação.
- **O publisher compete com a API.** Mesma sessão, `RUN_WORKERS=0` (só API) contra `RUN_WORKERS=1`: A com 1 instância 265 vs 183 req/s; A com 3 instâncias 444 vs 330; B com 3 instâncias 135 vs 112. Há ruído (seção 6.3), mas a diferença é consistente: o publisher e o SDK da AWS no mesmo processo custam 20-35% da vazão da API.
- **Log síncrono (pino)**: `LOG_LEVEL=warn` contra `info` (2 linhas por requisição), alternados (warn, info, warn, info): A com 1 instância 272 e 229 contra 232 e 214; com 3 instâncias 440 e 397 contra 363 e 379. Em média ~+10%, com sobreposição entre as execuções. Efeito provável, mas pequeno perto da variação.
- **Pool de conexões**: com 1 instância e 32 requisições em voo para 10 conexões, a espera pelo pool entra no "tempo de serviço" medido pelo histograma, que conta do início do caso de uso ao commit. Por isso o tempo de serviço médio (~147 ms) quase iguala a latência do cliente: o tempo é gasto esperando dentro do processo, não na rede. 3 instâncias × 10 = 30 conexões, mais as dos workers, ficam muito abaixo de `max_connections=200`. O pool não limitou nesta faixa. Com 20 instâncias × `DB_POOL_MAX=20` = 400 conexões, o `max_connections` seria ultrapassado.

### 6.2 Hot wallet

- 110-135 BETs/s numa wallet equivale a **7-9 ms por seção crítica serializada**. O piso só do banco é de ~3 ms: ~10 idas e voltas de 0,21 ms mais o commit com fsync, de ~0,8 ms. A diferença é o tempo em que o processo que segura o lock demora para enviar o próximo comando, porque o event loop dele está ocupado com outras requisições (parsing, publisher, logs). O lock pessimista transforma a latência do event loop em throughput perdido.
- **3 instâncias não ajudam e pioram com workers ligados** (125 para 111 req/s). Sem workers, 1 e 3 instâncias dão o mesmo (132 vs 135). Hipótese, não comprovada: com 3 publishers ativos (eles drenam a 250 eventos/s, contra ~100 com 1 instância), o processo que segura o lock perde mais tempo de event loop. Com 3 instâncias também há 30 backends na fila do lock de linha, e não 10 (o resto esperaria no pool, que é mais barato). O resultado esperado e confirmado é que escalar instâncias não aumenta o throughput de uma wallet.
- O p99 com 3 instâncias (420-510 ms) e o máximo de 600 ms ficam bem abaixo do `lock_timeout` de 3 s. Por isso **não houve nenhum 55P03, nenhum retry e nenhum 503**. Com concorrência bem maior numa única wallet, a fila passaria de 3 s e apareceriam retries e 503. Isso não foi exercitado aqui.
- **B_LOW** é mais rápido (165-175 req/s) porque a rejeição não grava ledger nem atualiza a wallet e gera 1 evento em vez de 2. Mesmo sob contenção total, exatamente 100 BETs de 1,00 passaram em 100,00. Prova empírica, nesta escala, de que não há débito duplo.

### 6.3 Variação e deriva

- **Dentro de uma mesma execução**, as duas repetições diferem pouco: A ±1,3%, B ±1%, B_LOW ±0,5%, C até ±11% (com 3 instâncias). O p99 e o máximo variam muito mais que a mediana (B com 3 instâncias: p99 de 506 vs 423 ms).
- **Ao longo da sessão houve deriva grande**: o mesmo cenário A com 1 instância deu 360 req/s na primeira medição e ficou entre 183 e 272 req/s nas últimas duas horas, com código igual ou equivalente.
- Testei se o crescimento do banco explicava a deriva: num banco novo e migrado (`wager_load`), logo antes de rodar o mesmo cenário no banco cheio (~314 mil linhas na outbox), deu 219/234 req/s no banco novo contra 210/224 no cheio. Diferença de ~5%, dentro do ruído.
- A causa mais provável é o próprio host: Windows, WSL2, Docker Desktop, frequência da CPU e page cache com ~130 MB livres. Não está comprovada.
- **Consequência: só compare números da mesma execução.** As razões 1 vs 3 da tabela de referência saíram de execuções vizinhas.

### 6.4 Erros

- Nenhum 5xx em nenhuma execução.
- Um único erro de rede (`TypeError` do `fetch`) em uma execução de A com 3 instâncias e workers desligados, em ~150 mil requisições somadas. A verificação mostrou que ele não commitou: banco = 7336 = confirmadas pelo cliente, com 1 indeterminada.
- A causa não foi determinada. A hipótese é reuso de um socket keep-alive fechado pelo servidor. O gerador agora registra `code` e `message` do erro para a próxima ocorrência. O cliente real de uma API financeira deve tratar isso como indeterminado e reenviar com a mesma key, o que a idempotência cobre.

### 6.5 Conflitos de concorrência

- 0 em `wager_lock_conflicts_total` e 0 em `wager_retries_total{component="unit_of_work"}` em todas as execuções.
- Isso **não** quer dizer que não houve contenção: o cenário B é contenção pura. Essas métricas contam apenas `lock_timeout` (55P03), deadlock (40P01), serialization failure e `23505` concorrente. A espera normal de um `FOR UPDATE` não é medida.
- Em A, com 500 wallets e 32 requisições em voo, a probabilidade de duas requisições na mesma wallet é baixa, e o mix não gera ciclos de lock.
- Nenhuma idempotency key reenviada chegou em paralelo com a original: o replay só sai depois da resposta. O caso concorrente já é coberto pelos testes de concorrência (`mesma key x50`).

### 6.6 Outbox lag

- O publisher roda em cada instância, no mesmo processo da API. Sob carga ele publica ~50-60 eventos/s por instância, enquanto a API gera ~2 eventos por transação (~425 eventos/s com 1 instância em A). **O lag cresce ~1 s por segundo de carga** (lag máximo ≈ duração da carga em A e em B com 1 instância) e só drena depois que a carga para.
- Depois da carga, um publisher sozinho drena ~250 eventos/s (5651 eventos em 23 s). O teto passa a ser o envio sequencial de um `SendMessage` por vez ao LocalStack, com a transação do claim aberta.
- Com 3 instâncias em B (~111 transações/s, ~222 eventos/s) os publishers acompanham: lag máximo de 0,6-1,2 s e drenagem imediata.
- Ou seja: num serviço com tráfego contínuo nessa faixa, os eventos ficam minutos atrasados. Não se perdem, mas atrasam. É o resultado mais relevante para produção depois da correção do índice.

### 6.7 Consumidor SQS

- 62-67 mensagens/s com 1 instância, 105-117 com 3 instâncias.
- O consumidor faz um `ReceiveMessage` (até 10 mensagens) por vez, processa o lote (grupos em paralelo) e só então recebe de novo. A vazão por instância fica limitada a ~10 mensagens por ida e volta ao broker mais o processamento do lote, ~150 ms por ciclo aqui.
- Não medi quantas mensagens o LocalStack devolve por receive em filas FIFO com 100 grupos. Pode ser menos que 10, o que reduziria ainda mais a vazão.
- Zero duplicatas: com visibilidade de 30 s, nenhuma mensagem voltou à fila.

## 7. O que estes números NÃO provam

- **Não são capacidade de produção.** Gerador, 3 instâncias, Postgres e LocalStack dividem a mesma CPU de 8 núcleos, sob Docker Desktop e WSL2. O gerador gastou pouco (0,08-0,19 núcleo), mas os processos competem por cache, memória e escalonador. Em máquinas separadas, a escala de 1 para 3 instâncias provavelmente seria melhor; aqui ela não chega a 3x.
- **Não é tempo de resposta sob carga aberta.** Laço fechado sofre de **omissão coordenada**: quando o servidor fica lento, o gerador desacelera junto e deixa de enviar as requisições que chegariam nesse período, então não as mede. Os percentis são de **tempo de serviço visto pelo cliente**, não de resposta a uma taxa de chegada fixa. Com chegada aberta acima de ~210 req/s (1 instância), a fila cresceria sem limite e o p99 real seria muito maior. Este teste não diz qual latência se teria a, por exemplo, 300 req/s oferecidos.
- **Janelas curtas** (15 s). Não mostram efeitos de longo prazo: autovacuum, inchaço de índices, crescimento das tabelas sem retenção, GC de longo prazo, checkpoints. A deriva da seção 6.3 sugere que execuções longas dariam números diferentes.
- **Banco pequeno e em cache**: o banco cabe nos 128 MB de `shared_buffers` mais o page cache, e o fsync acontece no disco virtual do Docker Desktop (0,8 ms por commit). Disco real, réplicas síncronas ou rede entre zonas mudariam a seção crítica da hot wallet diretamente.
- **LocalStack não é SQS.** Latência de `SendMessage`/`ReceiveMessage`, entrega por grupo FIFO, limites de throughput FIFO (300 TPS por ação sem batching na AWS, com high-throughput mode à parte) e custo não são representados. Os números do cenário C e da drenagem da outbox valem só para o LocalStack.
- **Mix sintético**: um provedor real tem distribuição de wallets com cauda (muitas wallets frias, algumas quentes), rodadas, ROLLBACKs e referências fora de ordem. Aqui não há `PENDING_REFERENCE` (o WIN só referencia BETs já confirmadas) nem ROLLBACK.
- **Sem falhas injetadas durante a carga.** Quedas de instância e de broker são cobertas pelos testes de concorrência, não por este.
- **A verificação de correção é sobre o que foi exercitado**: invariantes do ledger, banco × cliente, unicidade e saldo não negativo, nestes volumes (até ~6 mil transações e 500 wallets por execução). É uma evidência forte, não uma prova geral.

## 8. Recomendações e próximos passos

1. **Tirar o publisher do processo da API** (deployment próprio de workers) e publicar com `SendMessageBatch` (10 por chamada) ou com envios paralelos por grupo. A ordem por wallet se preserva: `MessageGroupId` = walletId e lotes por grupo em sequência. Medir de novo o lag sob carga contínua.
2. **Perfilar a CPU por requisição** (`bun --cpu-prof` ou equivalente) antes de qualquer outro ajuste: é o gargalo dominante e ~7 ms de CPU por requisição é muito para o que o caso de uso faz. Suspeitos: MikroORM (fork, hidratação, change set), Nest/express sobre a compatibilidade do Bun, zod e o SDK da AWS.
3. **Consumidor SQS com receives em pipeline** (vários pollers ou receive do próximo lote enquanto o atual processa), respeitando a ordem por grupo.
4. **Métrica de espera de lock**: histograma do tempo entre pedir e obter o `FOR UPDATE` da wallet, ou amostragem de `pg_stat_activity` com `wait_event_type = 'Lock'`. Hoje a contenção só aparece quando vira timeout.
5. **Pool e conexões**: com mais instâncias, PgBouncer em modo transação (o `SET LOCAL` continua válido) e `DB_POOL_MAX` dimensionado para que `instâncias × pool` fique abaixo de `max_connections`, com folga para workers e manutenção.
6. **Hot wallet**: se o negócio tiver wallets com mais de ~100 operações/s, o limite é estrutural (lock por linha). Opções: reduzir a seção crítica (menos idas e voltas, CTE única, outbox em um insert só) ou rotear por wallet para a mesma instância, para evitar fila entre processos.
7. **Retenção da outbox** publicada (e do inbox). Com o índice novo o claim não depende mais do histórico, mas a tabela continua crescendo.
8. **Repetir em ambiente separado e com carga aberta** (taxa de chegada fixa, medindo a latência a partir do horário agendado), com execuções de vários minutos e várias repetições, reportando intervalos e não pontos.
