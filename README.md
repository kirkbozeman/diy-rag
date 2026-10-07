# diy-rag

Personal RAG project. Airflow 3.3.2 (Docker Compose, LocalExecutor) orchestrates ingestion into Postgres + pgvector.

## Prerequisites

- Docker Desktop
- [Homebrew](https://brew.sh) (macOS) for Ollama
- `make`

Embeddings run locally with [Ollama](https://ollama.com) (`nomic-embed-text`), so no API key or cost. Ollama runs natively on the host, not in Docker: Docker on macOS can't use the Apple GPU, so a containerized Ollama would be CPU-only. Airflow containers reach it at `http://host.docker.internal:11434`.

## Setup

1. Create `.env` in the repo root:

   ```bash
   echo "AIRFLOW_UID=$(id -u)" > .env
   echo "FERNET_KEY=$(python3 -c 'import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')" >> .env
   ```

2. Install Ollama:

   ```bash
   brew install ollama
   ```

3. Start everything:

   ```bash
   make up
   ```

   This starts Ollama as a brew service, pulls `nomic-embed-text` if missing, then runs `docker compose up -d`.

Airflow UI: http://localhost:8080

## Make targets

| Target | Does |
|---|---|
| `make up` | Start Ollama, pull the embedding model, start the compose stack |
| `make down` | Stop the compose stack (Ollama keeps running; `brew services stop ollama` to stop it) |
| `make ollama` | Start Ollama only and wait until it responds |
| `make ollama-pull` | Pull the embedding model if missing |
| `make psql` | psql shell into the vector DB |

## Vector DB

`vectordb` is Postgres 16 with the `pgvector` extension.

Open a shell in the container (Docker Desktop > `vectordb` > Exec), then connect as `rag`:

```bash
psql -U rag -d rag
```

Without `-U`/`-d`, psql tries the `root` role and fails. No host, port or password is needed inside the container.

External clients (DBeaver, etc.) connect to `localhost:5433`, database `rag`, user/password `rag`/`rag`.

Check that pgvector is loaded:

```sql
SELECT extname, extversion FROM pg_extension WHERE extname = 'vector';
SELECT '[1,2,3]'::vector <-> '[1,2,4]'::vector AS l2_distance;
```

Airflow connection: `vectordb_default` (see `config/connections.yaml`).

# TODO

- Ingestion
 - document(s)
 - chunk
 - embed
 - index
- Retrieval
 - query
 - index
 - top K
- Synthesis
 - LLM response

Langchain - python lib, can load docs
