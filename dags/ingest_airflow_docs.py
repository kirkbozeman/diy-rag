"""Ingest the Airflow core docs (RST, from GitHub) into pgvector.

Flow: create tables (sql/ddl) -> check Ollama -> diff GitHub tree vs `documents` -> one mapped task
per changed doc (fetch, chunk, embed, upsert) -> delete docs removed upstream.

Incremental: a doc is re-processed only when its git blob SHA or ingestion config changed. Within a
changed doc, chunks whose content hash is unchanged keep their existing embedding.
"""

import hashlib
import os
import re
from datetime import datetime
from pathlib import Path

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

CONN_ID = "vectordb_default"
DDL_DIR = Path(__file__).parents[1] / "sql" / "ddl"  # /opt/airflow/sql/ddl in the container
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434")
EMBED_MODEL = "nomic-embed-text"
EMBED_BATCH = 32
# nomic-embed-text is trained with task prefixes; documents and queries use different ones.
DOC_PREFIX = "search_document: "

GITHUB_REPO = "apache/airflow"
DOCS_PARENT = "airflow-core"  # directory containing `docs/`
MAX_CHUNK_CHARS = 2000
CHUNKER_VERSION = 1  # bump when chunk_rst's output changes for the same input

# Fingerprint of everything that determines chunk text and embeddings. Stored on each document
# and mixed into every chunk's content_hash, so changing any input re-ingests and re-embeds.
CONFIG_HASH = hashlib.sha256(
    f"{EMBED_MODEL}|{DOC_PREFIX}|{MAX_CHUNK_CHARS}|{CHUNKER_VERSION}".encode()
).hexdigest()[:16]

# RST section underline characters; the first one seen is level 1, and so on.
RST_UNDERLINE = re.compile(r"^([=\-~^\"'`#*+._:])\1{2,}\s*$")


def chunk_rst(text: str) -> list[dict]:
    """Split RST into chunks at section headings, then cap each chunk at MAX_CHUNK_CHARS."""
    lines = text.splitlines()
    levels: list[str] = []  # underline chars in order of first appearance
    stack: list[str] = []  # current heading path
    sections: list[tuple[str, list[str]]] = [("", [])]  # (heading path, body lines)

    i = 0
    while i < len(lines):
        is_heading = (
            i + 1 < len(lines)
            and lines[i].strip()
            and not RST_UNDERLINE.match(lines[i])
            and RST_UNDERLINE.match(lines[i + 1])
            and len(lines[i + 1].strip()) >= len(lines[i].strip())
        )
        if is_heading:
            char = lines[i + 1].strip()[0]
            if char not in levels:
                levels.append(char)
            depth = levels.index(char)
            stack = [*stack[:depth], lines[i].strip()]
            sections.append((" > ".join(stack), []))
            i += 2
        else:
            sections[-1][1].append(lines[i])
            i += 1

    chunks = []
    for heading, body in sections:
        body_text = "\n".join(body).strip()
        if not body_text:
            continue
        current = ""
        for para in re.split(r"\n\s*\n", body_text):
            for piece in split_oversized(para):
                if current and len(current) + 2 + len(piece) > MAX_CHUNK_CHARS:  # 2 = "\n\n"
                    chunks.append({"heading": heading, "text": current})
                    current = ""
                current = f"{current}\n\n{piece}" if current else piece
        chunks.append({"heading": heading, "text": current})
    return chunks


def split_oversized(para: str) -> list[str]:
    """Split a paragraph longer than MAX_CHUNK_CHARS on line breaks, else on a hard boundary."""
    if len(para) <= MAX_CHUNK_CHARS:
        return [para]
    pieces: list[str] = []
    current = ""
    for line in para.split("\n"):
        while len(line) > MAX_CHUNK_CHARS:  # a single line over the cap
            if current:
                pieces.append(current)
                current = ""
            pieces.append(line[:MAX_CHUNK_CHARS])
            line = line[MAX_CHUNK_CHARS:]
        if current and len(current) + 1 + len(line) > MAX_CHUNK_CHARS:  # 1 = "\n"
            pieces.append(current)
            current = ""
        current = f"{current}\n{line}" if current else line
    if current:
        pieces.append(current)
    return pieces


def embed(texts: list[str]) -> list[list[float]]:
    """Embed texts with Ollama in batches; returns one vector per input, in order."""
    out = []
    for start in range(0, len(texts), EMBED_BATCH):
        resp = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": texts[start : start + EMBED_BATCH]},
            timeout=300,
        )
        resp.raise_for_status()
        out.extend(resp.json()["embeddings"])
    return out


def to_pgvector(vec: list[float]) -> str:
    """Format a vector as a pgvector text literal, e.g. '[0.1,0.2]'."""
    return "[" + ",".join(map(str, vec)) + "]"


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={"ref": "main"},  # git branch or tag of apache/airflow; stored as `documents.version`
    tags=["rag", "ingest"],
)
def ingest_airflow_docs():
    """Incrementally ingest Airflow core docs from GitHub into pgvector."""

    @task
    def create_tables():
        """Create `documents` and `chunks` tables on first run or if missing."""
        PostgresHook(postgres_conn_id=CONN_ID).run((DDL_DIR / "create_tables.sql").read_text())

    @task
    def check_ollama():
        """Check that Ollama is good to go."""
        resp = requests.get(f"{OLLAMA_URL}/api/tags", timeout=10)
        resp.raise_for_status()
        models = [m["name"] for m in resp.json()["models"]]
        if not any(m.startswith(EMBED_MODEL) for m in models):
            raise RuntimeError(f"{EMBED_MODEL} not pulled in Ollama. Run: make ollama-pull")

    @task
    def find_changes(params=None):
        """Diff upstream .rst files against `documents`; return docs to (re)ingest."""
        ref = params["ref"]
        api = f"https://api.github.com/repos/{GITHUB_REPO}"

        # Pin the branch/tag to one commit so discovery and content fetches see the same tree.
        commit_resp = requests.get(f"{api}/commits/{ref}", timeout=30)
        commit_resp.raise_for_status()
        commit = commit_resp.json()["sha"]

        # The full repo tree is truncated by the API, so resolve the docs subtree SHA first.
        parent = requests.get(f"{api}/contents/{DOCS_PARENT}", params={"ref": commit}, timeout=30)
        parent.raise_for_status()
        docs_sha = next(e["sha"] for e in parent.json() if e["name"] == "docs")
        tree = requests.get(f"{api}/git/trees/{docs_sha}", params={"recursive": 1}, timeout=30)
        tree.raise_for_status()
        if tree.json().get("truncated"):
            raise RuntimeError("docs tree truncated by GitHub API")

        upstream = {
            f"{DOCS_PARENT}/docs/{e['path']}": e["sha"]
            for e in tree.json()["tree"]
            if e["type"] == "blob" and e["path"].endswith(".rst")
        }

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        stored = {
            path: (git_sha, config_hash)
            for path, git_sha, config_hash in hook.get_records(
                "SELECT path, git_sha, config_hash FROM documents WHERE version = %s", [ref]
            )
        }

        changed = [
            {"ref": ref, "commit": commit, "path": p, "sha": s}
            for p, s in upstream.items()
            if stored.get(p) != (s, CONFIG_HASH)
        ]
        removed = [p for p in stored if p not in upstream]
        print(f"{len(upstream)} upstream, {len(changed)} changed/new, {len(removed)} removed")
        return {"changed": changed, "removed": removed, "ref": ref}

    @task
    def changed_docs(changes: dict):
        """Extract the docs to ingest, as the input list for dynamic task mapping."""
        return changes["changed"]

    @task(max_active_tis_per_dag=2)  # limit concurrent load on Ollama
    def ingest_doc(doc: dict):
        """Fetch, chunk and embed one doc; replace its rows in a single transaction."""
        raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{doc['commit']}/{doc['path']}"
        resp = requests.get(raw_url, timeout=30)
        resp.raise_for_status()
        raw_text = resp.text

        title = doc["path"].removeprefix(f"{DOCS_PARENT}/docs/")
        chunks = chunk_rst(raw_text)
        for c in chunks:
            c["embed_input"] = f"{DOC_PREFIX}{title} > {c['heading']}\n\n{c['text']}"
            c["content_hash"] = hashlib.sha256(
                f"{CONFIG_HASH}|{c['embed_input']}".encode()
            ).hexdigest()

        hook = PostgresHook(postgres_conn_id=CONN_ID)

        # Reuse embeddings for chunks whose content is unchanged. Read and embed before opening
        # the write transaction so slow Ollama calls don't hold locks.
        existing = dict(
            hook.get_records(
                """
                SELECT c.content_hash, c.embedding::text
                FROM chunks c JOIN documents d ON d.id = c.document_id
                WHERE d.version = %s AND d.path = %s
                """,
                [doc["ref"], doc["path"]],
            )
        )
        new = [c for c in chunks if c["content_hash"] not in existing]
        for c, v in zip(new, embed([c["embed_input"] for c in new]), strict=True):
            existing[c["content_hash"]] = to_pgvector(v)

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO documents (version, path, git_sha, raw_text, config_hash)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (version, path) DO UPDATE
                        SET git_sha = EXCLUDED.git_sha,
                            raw_text = EXCLUDED.raw_text,
                            config_hash = EXCLUDED.config_hash,
                            ingested_at = now()
                    RETURNING id
                    """,
                    (doc["ref"], doc["path"], doc["sha"], raw_text, CONFIG_HASH),
                )
                doc_id = cur.fetchone()[0]

                cur.execute("DELETE FROM chunks WHERE document_id = %s", (doc_id,))
                cur.executemany(
                    """
                    INSERT INTO chunks
                        (document_id, chunk_index, heading, text, content_hash, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            doc_id,
                            i,
                            c["heading"],
                            c["text"],
                            c["content_hash"],
                            existing[c["content_hash"]],
                        )
                        for i, c in enumerate(chunks)
                    ],
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return {"path": doc["path"], "chunks": len(chunks), "embedded": len(new)}

    @task
    def delete_removed(changes: dict):
        """Delete docs no longer upstream; chunks go via ON DELETE CASCADE."""
        if not changes["removed"]:
            return
        PostgresHook(postgres_conn_id=CONN_ID).run(
            "DELETE FROM documents WHERE version = %s AND path = ANY(%s)",
            parameters=(changes["ref"], changes["removed"]),
        )

    tables = create_tables()
    ollama = check_ollama()
    changes = find_changes()
    [tables, ollama] >> changes
    ingest_doc.expand(doc=changed_docs(changes))
    delete_removed(changes)  # independent of ingestion, so it still runs when nothing changed


ingest_airflow_docs()
