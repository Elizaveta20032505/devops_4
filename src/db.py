"""Подключение к PostgreSQL. Креды берутся ТОЛЬКО из HashiCorp Vault."""
from __future__ import annotations

import configparser
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection as PGConnection

from src.secrets import fetch_secret


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def _cfg() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(_root() / "config.ini", encoding="utf-8")
    return cfg


def predictions_table() -> str:
    return _cfg()["DB"].get("predictions_table", "predictions")


def training_table() -> str:
    return _cfg()["DB"].get("training_table", "training_data")


_REQUIRED_SECRET_KEYS = ("host", "dbname", "username", "password")


def _conn_params() -> dict:
    secret = fetch_secret()
    missing = [k for k in _REQUIRED_SECRET_KEYS if not secret.get(k)]
    if missing:
        raise RuntimeError(f"В Vault отсутствуют ключи для подключения к БД: {missing}")
    return {
        "host": secret["host"],
        "port": int(secret.get("port", 5432)),
        "dbname": secret["dbname"],
        "user": secret["username"],
        "password": secret["password"],
    }


@contextmanager
def get_conn() -> Iterator[PGConnection]:
    conn = psycopg2.connect(**_conn_params())
    try:
        yield conn
    finally:
        conn.close()


def init_schema() -> None:
    ddl_predictions = f"""
        CREATE TABLE IF NOT EXISTS {predictions_table()} (
            id        BIGSERIAL PRIMARY KEY,
            ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            features  JSONB       NOT NULL,
            label     TEXT        NOT NULL,
            proba     DOUBLE PRECISION
        );
    """
    ddl_training = f"""
        CREATE TABLE IF NOT EXISTS {training_table()} (
            id        BIGSERIAL PRIMARY KEY,
            diagnosis TEXT NOT NULL,
            features  JSONB NOT NULL
        );
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl_predictions)
        cur.execute(ddl_training)
        conn.commit()


def save_prediction(features: dict, label: str, proba: float | None) -> None:
    sql = f"INSERT INTO {predictions_table()} (features, label, proba) VALUES (%s, %s, %s);"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (json.dumps(features), label, proba))
        conn.commit()


def fetch_last_predictions(limit: int = 10) -> list[dict]:
    sql = f"SELECT id, ts, features, label, proba FROM {predictions_table()} ORDER BY id DESC LIMIT %s;"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [
        {"id": r[0], "ts": r[1].isoformat(), "features": r[2], "label": r[3], "proba": r[4]}
        for r in rows
    ]
