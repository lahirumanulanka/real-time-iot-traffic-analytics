import os
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

# Load env
ENV_PATH = Path(__file__).resolve().parents[1] / "utils" / "config.env"
load_dotenv(dotenv_path=ENV_PATH)

INIT_SQL_PATH = Path(__file__).parent / "init.sql"


def run_init():
    sql = INIT_SQL_PATH.read_text(encoding="utf-8")
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "traffic"),
        user=os.getenv("POSTGRES_USER", "analytics"),
        password=os.getenv("POSTGRES_PASSWORD", "analytics"),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        print("Database initialized (traffic_metrics).")
    finally:
        conn.close()


if __name__ == "__main__":
    run_init()
