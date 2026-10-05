from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["smoke-test"])
def hello_vectordb():
    @task
    def check_pgvector():
        hook = PostgresHook(postgres_conn_id="vectordb_default")
        row = hook.get_first("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        if not row:
            raise RuntimeError("pgvector extension not installed")
        print(f"pgvector {row[0]}")

    check_pgvector()


hello_vectordb()
