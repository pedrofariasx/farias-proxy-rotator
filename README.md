# Farias Proxy Rotator

Proxy rotativo leve em Go com coleta de proxies públicos do FreeProxyDB, validação automática e manutenção de um pool de proxies saudáveis.

Este projeto foi criado apenas para fins de estudo, pesquisa e aprendizado sobre redes, proxies, concorrência em Go e manutenção de pools de conexões. O uso indevido para burlar restrições, atacar serviços, coletar dados sem autorização, fraudar sistemas ou violar termos de uso de terceiros não é incentivado nem apoiado.

## Funcionalidades

- Busca proxies no FreeProxyDB com paginação.
- Valida proxies em paralelo antes de usá-los.
- Mantém um pool de proxies saudáveis em memória.
- Remove proxies que começam a falhar.
- Repõe automaticamente o pool quando fica abaixo do mínimo configurado.
- Encaminha requisições para uma `TARGET_URL` definida no `.env`.
- Suporta header `Authorization` e headers customizados.
- Expõe endpoints de saúde e atualização manual.

## Requisitos

- Go `1.22+`
- Opcional: Node/npm apenas se quiser usar os scripts do `package.json`

## Configuração

Crie o arquivo `.env` a partir do exemplo:

```bash
cp .env.example .env
```

Exemplo de configuração:

```env
PORT=3000
TARGET_URL=https://httpbin.org/ip
PROXY_SOURCE_URL=https://freeproxydb.com/api/proxy/search?country=&protocol=http&anonymity=elite,anonymous&speed=0,60&page_index=1&page_size=100
PROXY_SOURCE_PAGES=20
PROXY_SOURCE_PAGE_SIZE=100
PROXY_REQUIRE_HTTPS=false
PROXY_REFRESH_SECONDS=300
PROXY_TIMEOUT_MS=5000
PROXY_ATTEMPT_TIMEOUT_MS=5000
PROXY_VALIDATION_CONCURRENCY=16
HEALTHY_PROXY_TARGET=25
HEALTHY_PROXY_MIN=5
MAX_PROXY_FAILURES=2
MAX_PROXY_ATTEMPTS=5
TARGET_AUTHORIZATION=
TARGET_HEADERS=
```

## Variáveis De Ambiente

| Variável | Descrição |
|---|---|
| `PORT` | Porta local do servidor. |
| `TARGET_URL` | URL final que será chamada através dos proxies. |
| `PROXY_SOURCE_URL` | URL base do FreeProxyDB usada para coletar proxies. |
| `PROXY_SOURCE_PAGES` | Quantidade de páginas do FreeProxyDB para buscar. |
| `PROXY_SOURCE_PAGE_SIZE` | Tamanho da página no FreeProxyDB. Normalmente `100`. |
| `PROXY_REQUIRE_HTTPS` | Se `true`, usa apenas proxies marcados como HTTPS pela fonte. Por padrão fica `false` porque proxies HTTP também podem fazer `CONNECT` para targets HTTPS. |
| `PROXY_REFRESH_SECONDS` | Intervalo de atualização da lista de proxies. |
| `PROXY_TIMEOUT_MS` | Tempo máximo total esperado para uma requisição. |
| `PROXY_ATTEMPT_TIMEOUT_MS` | Timeout por tentativa usando um proxy. |
| `PROXY_VALIDATION_CONCURRENCY` | Quantos proxies validar em paralelo durante manutenção. |
| `HEALTHY_PROXY_TARGET` | Tamanho alvo do pool de proxies saudáveis. |
| `HEALTHY_PROXY_MIN` | Mínimo aceitável antes de iniciar reposição automática. |
| `MAX_PROXY_FAILURES` | Falhas permitidas antes de descartar um proxy saudável. |
| `MAX_PROXY_ATTEMPTS` | Quantos proxies saudáveis tentar por requisição real. |
| `TARGET_AUTHORIZATION` | Valor do header `Authorization` enviado ao target. |
| `TARGET_HEADERS` | Headers extras no formato `header: valor|outro: valor`. |

## Autenticação No Target

Para enviar `Authorization`:

```env
TARGET_AUTHORIZATION=Bearer seu_token_aqui
```

Para enviar headers extras:

```env
TARGET_HEADERS=x-api-key: minha_chave|accept-language: pt-BR
```

## Executando

Com npm:

```bash
npm start
```

Direto com Go:

```bash
go run .
```

Ao iniciar, o sistema já começa a busca e validação de proxies em background:

```text
Proxy rotativo ouvindo em http://localhost:3000
Destino configurado: https://httpbin.org/ip
Iniciando busca e validacao de proxies: bootstrap inicial
```

## Build

Com npm:

```bash
npm run build
```

Direto com Go:

```bash
go build -o farias-proxy-rotator .
```

Executar o binário:

```bash
./farias-proxy-rotator
```

## Uso

Qualquer requisição enviada ao servidor local será encaminhada para `TARGET_URL` usando um proxy saudável.

```bash
curl http://localhost:3000
```

Exemplo com `POST`:

```bash
curl -X POST http://localhost:3000 \
  -H 'content-type: application/json' \
  -d '{"hello":"world"}'
```

## Endpoints

### `GET /health`

Mostra o estado do pool:

```bash
curl http://localhost:3000/health
```

Exemplo de resposta:

```json
{
  "ok": true,
  "stats": {
    "proxies": 900,
    "candidates": 820,
    "healthyProxies": 25,
    "deadProxies": 55,
    "targetHealthyProxies": 25,
    "minHealthyProxies": 5
  }
}
```

### `GET /refresh`

Força nova busca no FreeProxyDB e inicia manutenção do pool:

```bash
curl http://localhost:3000/refresh
```

## Como O Pool Funciona

1. O sistema coleta proxies do FreeProxyDB.
2. Proxies novos entram como candidatos.
3. Candidatos são testados contra a própria `TARGET_URL`.
4. Proxies funcionais entram no pool saudável.
5. O pool é ordenado por score baseado em latência, sucessos e falhas.
6. Proxies que falham demais são descartados.
7. A manutenção em background repõe proxies quando necessário.

## Observações

Proxies públicos costumam ser instáveis. Mesmo com validação e manutenção automática, a disponibilidade depende da qualidade da lista retornada pelo FreeProxyDB.

## Uso Responsável

Use este projeto somente em ambientes, serviços e APIs onde você tem autorização para testar. Respeite limites de taxa, termos de uso, robots.txt quando aplicável e legislações locais.

O autor não se responsabiliza por uso indevido, abusivo ou ilegal deste código.

## Licença

Este projeto está licenciado sob a licença ISC. Consulte o arquivo `LICENSE` para mais detalhes.
