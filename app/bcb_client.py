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
DATA_INICIAL = datetime.date(2015, 1, 1)
DATA_FINAL = datetime.date(2026, 9, 30)
# Séries do SGS/BCB — dados abertos, sem autenticação.
# Referência completa de códigos: https://www3.bcb.gov.br/sgspub

SERIES = {
    1: {
        "name": "Crescimento do PIB",
        "indicator": "NY.GDP.MKTP.KD.ZG",
        "unit": "%",
        "casas": 2
    },
    2: {
        "name": "Inflação",
        "indicator": "FP.CPI.TOTL.ZG",
        "unit": "%",
        "casas": 2
    },
    3: {
        "name": "Desemprego",
        "indicator": "SL.UEM.TOTL.ZS",
        "unit": "%",
        "casas": 2
    },
    4: {
        "name": "Usuários de internet",
        "indicator": "IT.NET.USER.ZS",
        "unit": "%",
        "casas": 2
    },
}

POINTS_PER_SERIES = 30


# Pool de conexões: a Aiven fica em outra região/rede que o Render, então
# abrir uma conexão nova (TCP + handshake SSL) custa ~1s toda vez. Uma
# única página do dashboard faz várias consultas (visita, cache de cada
# série, contagem de acessos...), e abrir uma conexão nova pra cada uma
# multiplicava esse ~1s por 7-8, deixando a página bem lenta. Com o pool,
# as conexões são abertas uma vez e reaproveitadas entre requisições.
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
    connect_timeout=15,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=3,
)
    return _pool


@contextmanager
@contextmanager
def get_connection():
    """Obtém uma conexão válida e descarta conexões encerradas."""

    pool = _get_pool()
    conn = pool.getconn()

    # Descarta conexões que já chegaram encerradas.
    if conn.closed:
        pool.putconn(conn, close=True)
        conn = pool.getconn()

    try:
        yield conn

    except Exception:
        # Só tenta rollback se a conexão ainda estiver aberta.
        if not conn.closed:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass

        raise

    finally:
        # Nunca devolve ao pool uma conexão encerrada.
        pool.putconn(conn, close=bool(conn.closed))


def init_db():
    """Cria as tabelas caso ainda não existam.

    Em produção, vários workers do gunicorn chamam esta função quase ao
    mesmo tempo na inicialização. `CREATE TABLE IF NOT EXISTS` não é
    100% atômico entre transações concorrentes no Postgres, então dois
    workers podem colidir tentando criar a mesma tabela ao mesmo tempo
    (erro de "duplicate key" no catálogo interno pg_type). Isso é
    inofensivo — só significa que outro worker já criou a tabela — então
    apenas ignoramos esse erro específico em vez de deixar o worker
    inteiro cair.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bcb_series (
                        series_code INTEGER NOT NULL,
                        ref_date    DATE NOT NULL,
                        value       NUMERIC NOT NULL,
                        fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
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
        # Outro worker criou a tabela entre nossa checagem e nosso CREATE.
        # Tudo bem, o objetivo (tabela existir) já foi alcançado.
        pass
    except psycopg2.errors.UniqueViolation as exc:
        # Mesma corrida, mas manifestada como conflito no catálogo interno
        # pg_type em vez de "table already exists". Também inofensivo.
        if "pg_type" not in str(exc):
            raise


def fetch_series_from_bcb(series_code: int):
    """Busca dados históricos na API do Banco Mundial."""

    meta = SERIES[series_code]
    indicator = meta["indicator"]

    url = (
        f"https://api.worldbank.org/v2/"
        f"country/BRA/indicator/{indicator}"
    )

    params = {
        "format": "json",
        "date": f"{DATA_INICIAL}:{DATA_FINAL}",
        "per_page": 100
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    raw = resp.json()

    if not isinstance(raw, list) or len(raw) < 2:
        raise ValueError(f"Resposta inesperada para {indicator}")

    parsed = []

    for item in raw[1] or []:

        if item["value"] is None:
            continue

        year = int(item["date"])

        parsed.append({
            "ref_date": datetime.date(year, 1, 1),
            "value": float(item["value"])
        })

    parsed.sort(key=lambda x: x["ref_date"])

    return parsed

def upsert_series(series_code: int, points: list):
    """Grava (ou atualiza) pontos de uma série direto no Postgres —
    usado pelo cache do app.py."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            for p in points:
                cur.execute(
                    """
                    INSERT INTO bcb_series (series_code, ref_date, value, fetched_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (series_code, ref_date)
                    DO UPDATE SET value = EXCLUDED.value, fetched_at = now();
                    """,
                    (series_code, p["ref_date"], p["value"]),
                )
        conn.commit()
