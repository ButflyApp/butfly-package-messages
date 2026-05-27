# Padrão de Logs — habitar-package-messages

Este pacote é o adapter RabbitMQ (publisher + consumer) consumido pelos serviços. Os logs são feitos via `pino.Logger` **injetado pelo caller** — ou seja, o transporte (Logtail/pretty) vive em `@habitar/essentials`.

## Eventos logados

### Consumer (`RabbitConsumerAdapter`)

| Evento                          | Nível | Mensagem (exemplo)                                                       |
| ------------------------------- | ----- | ------------------------------------------------------------------------ |
| Bind da fila pronto             | info  | `Consumer is listening on queue "svc.foo" (exchange "x")`                |
| Batch recebido (array)          | info  | `Received batch of 3 messages on queue "svc.foo", splitting into individual messages` |
| Mensagem recebida (objeto)      | info  | `Received message on queue "svc.foo" (uid=abc)` + payload                |
| Mensagem skipada (self-origin)  | info  | `Skipping self-originated message on queue "svc.foo" (uid=abc)`          |
| Mensagem processada com sucesso | info  | `<successMessage> (queue="svc.foo", uid=abc)`                            |
| Erro **intencional** (AppError) | warn  | `Message processing rejected on queue "svc.foo": <reason>`               |
| Erro **inesperado**             | error | `Unexpected error while processing message on queue "svc.foo": <reason>` |

### Publisher (`RabbitPublisherAdapter`)

| Evento              | Nível | Mensagem (exemplo)                                                |
| ------------------- | ----- | ----------------------------------------------------------------- |
| Publicação OK       | info  | `Published message to exchange "x" (uid=abc)` + payload           |
| Falha na publicação | error | `Unexpected error publishing message to exchange "x" (uid=abc)`   |

## Regras

1. **AppError → warn**, qualquer outra exceção → **error**. Centralizado em `ErrorHandler.handleError`.
2. Toda mensagem recebida e toda publicada gera **1 log com o payload** — para garantir rastreabilidade ponta-a-ponta.
3. O `habitarProcessUid` e o `messageOrigin` sempre acompanham os logs.
4. `pickIdentifier(payload)` extrai um id curto (`uid`, `id`, `companyUid`, `userUid`...) para a string da mensagem, igual ao padrão `Realstate 1234 was published on instagram` exigido pelo guia.

## Como integrar

```ts
import { logger } from "@habitar/essentials"
import { RabbitPublisherAdapter, RabbitConsumerAdapter } from "@habitar/messages"

const publisher = new RabbitPublisherAdapter({ logger })
new RabbitConsumerAdapter(
  { useCase, messagingPublisher: publisher, logger, amqpConnection },
  { exchange: "...", queue: "...", successMessage: "+ ..." }
)
```

Como o transporte está no `essentials`, basta exportar `LOGTAIL_SOURCE_TOKEN` no ambiente do serviço para os logs do consumer/publisher fluírem para o Better Stack.
