# quant_streamingchart

Fetches a **1-day intraday chart** for a stock ticker (e.g. `MSFT`) from Yahoo Finance, stores it,
and **replays it slice-by-slice to a Kafka topic** with a configurable delay between slices so that
downstream order-execution consumers have time to react.

See [SPEC.md](SPEC.md) for the full design, laid out in implementation slices.

## Stack
Python 3.12 · Bottle + Waitress · SQLAlchemy + Alembic · Pydantic Settings · confluent-kafka ·
Docker + supervisord.

Postgres and Kafka are **external dependencies** and are assumed to already exist.

## Runbook

### Install
```bash
pip install .[dev]
```

Configuration is provided via environment variables (see `docker-compose.yml`); local runs fall back to the safe defaults in `config.py`.

### Migrate
```bash
alembic upgrade head
```

### Run (locally)
```bash
python -m streamchart.api_main               # API on :8000
python -m streamchart.workers.replay_worker  # replay worker
```

### Test / lint / type / security
```bash
ruff check .
mypy src
bandit -r src
pytest -q
```

### Docker
```bash
make docker-build
docker compose up
curl localhost:8000/health
```

## API

| Method / path | Purpose |
|---|---|
| `GET /health` | process alive |
| `GET /ready` | DB reachable (+ Kafka broker if configured) |
| `POST /api/v1/fetch` | `{ticker, interval?, range?}` → fetch + store bars |
| `GET /api/v1/bars?ticker=&interval=` | ordered bars (resampled if interval differs) |
| `POST /api/v1/replays` | `{ticker, interval?, replay_interval_seconds?, topic?}` → create session |
| `GET /api/v1/replays` | list sessions |
| `GET /api/v1/replays/{id}` | status + progress |
| `POST /api/v1/replays/{id}/cancel` | request cancellation |

### Example
```bash
# 1. pull today's 1-minute chart for MSFT
curl -X POST localhost:8000/api/v1/fetch -H 'content-type: application/json' \
  -d '{"ticker":"MSFT"}'

# 2. start a replay: emit each slice 1 second apart
curl -X POST localhost:8000/api/v1/replays -H 'content-type: application/json' \
  -d '{"ticker":"MSFT","interval":"1m","replay_interval_seconds":1.0}'

# 3. watch progress
curl localhost:8000/api/v1/replays/<id>
```

## Kafka message
Key = `ticker` (ordering guarantee). Value = JSON:
```json
{
  "schema_version": 1, "session_id": "…", "ticker": "MSFT", "sequence": 42,
  "interval": "1m", "bar_time": "2026-08-21T14:30:00Z",
  "open": 415.20, "high": 415.80, "low": 415.10, "close": 415.55, "volume": 120000,
  "emitted_at": "2026-08-23T10:00:42Z", "is_first": false, "is_last": false
}
```
