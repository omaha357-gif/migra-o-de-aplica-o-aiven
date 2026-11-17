
"""
Módulo compartilhado: conexão com o Postgres da Aiven e leitura da API
de dados abertos do Banco Central (SGS). Usado por app.py — concentra a lógica
de acesso a dados num único módulo.
"""

import os
import datetime
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import RealDictCursor
import requests


DB_URL = os.environ.get("AIVEN_DB_URL")

# Séries do SGS/BCB — dados abertos, sem autenticação.
# Referência completa de códigos: https://www3.bcb.gov.br/sgspub
SERIES = {
    7326: {
        "name": "Governos municipais",
        "unit": "% m.",
        "casas": 2,
    },
    4507: {
        "name": "Governos estaduais",
        "unit": "% m.",
        "casas": 2,
    },
    4504: {
        "name": "Governo Federal",
        "unit": "% m.",
        "casas": 2,
    },
    4509: {
        "name": "Empresas estatais",
        "unit": "% m.",
        "casas": 2,
    },
}

POINTS_PER_SERIES = 30

# Pool de conexões
_pool = None


def _get_pool():
    global _pool

    if _pool is None:
        if not DB_URL:
            raise RuntimeError("AIVEN_DB_URL não configurada")

        _pool = psycopg2_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DB_URL,
            sslmode="require",
            cursor_factory=RealDictCursor,
            connect_timeout=10,
        )

    return _pool


@contextmanager
def get_connection():
    """
    Pega uma conexão emprestada do pool e devolve ao sair do bloco with.
    """

    pool = _get_pool()
    conn = pool.getconn()

    try:
        yield conn

    except Exception:
        conn.rollback()
        raise

    finally:
        pool.putconn(conn)


def init_db():
    """
    Cria as tabelas caso ainda não existam.

    Trata conflitos de criação simultânea entre workers do Gunicorn.
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bcb_series (
                        series_code INTEGER NOT NULL,
                        ref_date DATE NOT NULL,
                        value NUMERIC NOT NULL,
                        fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (series_code, ref_date)
                    );
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS visits (
                        id SERIAL PRIMARY KEY,
                        path TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )

            conn.commit()

    except psycopg2.errors.DuplicateTable:
        pass

    except psycopg2.errors.UniqueViolation as exc:
        if "pg_type" not in str(exc):
            raise


def fetch_series_from_bcb(
    series_code: int,
    n: int = POINTS_PER_SERIES
):
    """
    Busca os últimos N pontos de uma série no SGS do Banco Central.

    Utiliza dataInicial e dataFinal em vez do endpoint /dados/ultimos/.
    """

    days_back = n * 3

    today = datetime.date.today()
    start = today - datetime.timedelta(days=days_back)

    url = (
        f"https://api.bcb.gov.br/dados/serie/"
        f"bcdata.sgs.{series_code}/dados"
    )

    params = {
        "formato": "json",
        "dataInicial": start.strftime("%d/%m/%Y"),
        "dataFinal": today.strftime("%d/%m/%Y"),
    }

    resp = requests.get(
        url,
        params=params,
        timeout=10
    )

    resp.raise_for_status()

    raw = resp.json()

    parsed = []

    for item in raw:
        ref_date = datetime.datetime.strptime(
            item["data"],
            "%d/%m/%Y"
        ).date()

        parsed.append({
            "ref_date": ref_date,
            "value": float(item["valor"]),
        })

    return parsed[-n:]


def upsert_series(series_code: int, points: list):
    """
    Grava ou atualiza pontos de uma série diretamente no Postgres.
    """

    with get_connection() as conn:
        with conn.cursor() as cur:

            for point in points:
                cur.execute(
                    """
                    INSERT INTO bcb_series (
                        series_code,
                        ref_date,
                        value,
                        fetched_at
                    )
                    VALUES (%s, %s, %s, now())

                    ON CONFLICT (series_code, ref_date)
                    DO UPDATE SET
                        value = EXCLUDED.value,
                        fetched_at = now();
                    """,
                    (
                        series_code,
                        point["ref_date"],
                        point["value"],
                    ),
                )

        conn.commit()
