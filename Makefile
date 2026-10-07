# Local dev entrypoint for diy-rag.
#
# Ollama (embeddings) runs natively on the host so it can use the Apple GPU;
# Docker on macOS can't. Everything else (Airflow, Postgres, pgvector) runs in
# Docker Compose. `make up` starts both in order: Ollama, then the compose stack.

EMBED_MODEL ?= nomic-embed-text
OLLAMA_URL  ?= http://localhost:11434

.PHONY: up down ollama ollama-pull psql

## Start Ollama (host), pull the embedding model, then start the compose stack
up: ollama ollama-pull
	docker compose up -d

## Stop the compose stack (Ollama keeps running; stop with `brew services stop ollama`)
down:
	docker compose down

## Start Ollama as a background brew service and wait until it responds
ollama:
	@command -v ollama >/dev/null || { echo "ollama not found. Run: brew install ollama"; exit 1; }
	@curl -sf $(OLLAMA_URL)/api/tags >/dev/null || brew services start ollama
	@until curl -sf $(OLLAMA_URL)/api/tags >/dev/null; do echo "waiting for ollama..."; sleep 1; done

## Pull the embedding model if missing
ollama-pull:
	@ollama list | grep -q "^$(EMBED_MODEL)" || ollama pull $(EMBED_MODEL)

## psql into the vector DB
psql:
	docker compose exec vectordb psql -U rag -d rag
