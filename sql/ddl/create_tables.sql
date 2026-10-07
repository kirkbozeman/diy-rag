CREATE TABLE IF NOT EXISTS documents (
    id          BIGSERIAL PRIMARY KEY,
    version     TEXT NOT NULL,
    path        TEXT NOT NULL,
    git_sha     TEXT NOT NULL,
    raw_text    TEXT NOT NULL,
    config_hash TEXT NOT NULL DEFAULT '',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (version, path)
);

-- Fingerprint of the embedding model / prefix / chunking settings used to ingest the doc.
-- A mismatch with the DAG's current config forces a re-ingest. Backfills tables created before it.
ALTER TABLE documents ADD COLUMN IF NOT EXISTS config_hash TEXT NOT NULL DEFAULT '';

-- vector(768) must match the output dimension of the embedding model (nomic-embed-text).
CREATE TABLE IF NOT EXISTS chunks (
    id           BIGSERIAL PRIMARY KEY,
    document_id  BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    chunk_index  INT NOT NULL,
    heading      TEXT NOT NULL,
    text         TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    embedding    vector(768) NOT NULL,
    UNIQUE (document_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw
    ON chunks USING hnsw (embedding vector_cosine_ops);
