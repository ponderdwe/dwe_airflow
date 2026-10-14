"""
Connection env-fallback utilities for dwe_airflow.

DEPLOY_ENV (written to .env by Pulumi at VM boot) drives the fallback chain:
  prod  → [prod]
  stage → [stage, prod]
  dev   → [dev, stage, prod]
  local → [local, dev, stage, prod]

Usage:
    from include.airflow_utils.dag_utils import envify_connection
    conn = envify_connection('TRINO')  # → 'TRINO_prod' or 'TRINO-prod' on Azure

The separator between the base conn_id and the env suffix is '-' when the
Azure Key Vault secrets backend is active (KV normalises '_' to '-' in secret
names), and '_' otherwise.

Airflow 3 dag-processor blocks ORM access during DAG parsing, so connection
existence is checked via a bulk DB query (fresh SQLAlchemy engine) and falls
back to AWS Secrets Manager list when the DB is unavailable.
"""

import concurrent.futures as _cf
import functools
import json
import logging
import os

log = logging.getLogger('airflow.task')

_ENV_FALLBACK_CHAINS: dict[str, list[str]] = {
    'LOCAL': ['local', 'dev', 'stage', 'prod'],
    'DEV':   ['dev',          'stage', 'prod'],
    'STAGE': ['stage',                 'prod'],
    'PROD':  ['prod'],
}

_env_resolutions: dict[str, str] = {}

_DB_UNAVAILABLE = object()
_connection_ids_cache: set[str] | object | None = None
_db_cache_lock = _cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix='dag_utils_db_init')
_exists_executor: _cf.ThreadPoolExecutor | None = None


def log_env_resolutions() -> None:
    if not _env_resolutions:
        log.info('[env-fallback] no env resolutions recorded')
        return
    for key, resolved in sorted(_env_resolutions.items()):
        log.info('[env-fallback] %s  →  %s', key, resolved)


def _get_deploy_env() -> str:
    return os.environ.get('DEPLOY_ENV', 'PROD').upper()


def _env_sep() -> str:
    """Return '-' for Azure KV backend (normalises '_' → '-'), '_' otherwise."""
    backend = os.environ.get('AIRFLOW__SECRETS__BACKEND', '')
    if 'azure' in backend.lower() or 'key_vault' in backend.lower():
        return '-'
    return '_'


def _db_fetch(query: str) -> set[str] | None:
    def _run():
        from sqlalchemy import create_engine, text
        conn_str = os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
        if not conn_str or 'not-allowed' in conn_str:
            return None
        engine = create_engine(conn_str, pool_size=1, max_overflow=0, pool_pre_ping=True)
        try:
            with engine.connect() as conn:
                return {row[0] for row in conn.execute(text(query)).fetchall()}
        finally:
            engine.dispose()

    try:
        return _db_cache_lock.submit(_run).result(timeout=5.0)
    except Exception:
        return None


def _get_sm_prefixes() -> tuple[str, str]:
    kwargs_str = os.environ.get('AIRFLOW__SECRETS__BACKEND_KWARGS', '{}')
    try:
        kwargs = json.loads(kwargs_str.strip("'\""))
    except Exception:
        kwargs = {}
    return (
        kwargs.get('variables_prefix', 'airflow/variables'),
        kwargs.get('connections_prefix', 'airflow/connections'),
    )


def _sm_fetch(prefix: str) -> set[str] | None:
    """List secret names from AWS Secrets Manager under prefix/."""
    def _run():
        import boto3
        kwargs_str = os.environ.get('AIRFLOW__SECRETS__BACKEND_KWARGS', '{}')
        try:
            kwargs = json.loads(kwargs_str.strip("'\""))
        except Exception:
            kwargs = {}
        region = kwargs.get('region_name') or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        full_prefix = prefix + '/'
        client = boto3.client('secretsmanager', region_name=region)
        paginator = client.get_paginator('list_secrets')
        keys: set[str] = set()
        for page in paginator.paginate(Filters=[{'Key': 'name', 'Values': [full_prefix]}]):
            for secret in page.get('SecretList', []):
                name = secret.get('Name', '')
                if name.startswith(full_prefix):
                    keys.add(name[len(full_prefix):])
        return keys

    try:
        return _db_cache_lock.submit(_run).result(timeout=10.0)
    except Exception:
        return None


def _load_connection_ids() -> set[str] | None:
    global _connection_ids_cache
    if _connection_ids_cache is None:
        result = _db_fetch('select conn_id from connection')
        if result is None:
            _, conn_prefix = _get_sm_prefixes()
            result = _sm_fetch(conn_prefix)
        _connection_ids_cache = result if result is not None else _DB_UNAVAILABLE
    if _connection_ids_cache is _DB_UNAVAILABLE:
        return None
    return _connection_ids_cache  # type: ignore[return-value]


def _get_exists_executor() -> _cf.ThreadPoolExecutor:
    global _exists_executor
    if _exists_executor is None:
        _exists_executor = _cf.ThreadPoolExecutor(max_workers=8, thread_name_prefix='dag_utils_exists')
    return _exists_executor


def _check_connection(conn_id: str) -> None:
    cache = _load_connection_ids()
    if cache is not None:
        if conn_id not in cache:
            raise KeyError(conn_id)
        return
    from airflow.hooks.base import BaseHook
    BaseHook.get_connection(conn_id)


@functools.lru_cache(maxsize=256)
def resolve_connection(conn_id_base: str) -> str:
    """Return the first existing connection ID in the DEPLOY_ENV fallback chain."""
    current_env = _get_deploy_env()
    chain = _ENV_FALLBACK_CHAINS.get(current_env, [current_env.lower()])
    sep = _env_sep()
    for env_suffix in chain:
        candidate = f'{conn_id_base}{sep}{env_suffix}'
        f = _get_exists_executor().submit(_check_connection, candidate)
        try:
            f.result(timeout=5.0)
            _env_resolutions[f'conn:{conn_id_base}'] = candidate
            return candidate
        except _cf.TimeoutError:
            pass
        except Exception:
            continue
    resolved = f'{conn_id_base}{sep}{current_env.lower()}'
    _env_resolutions[f'conn:{conn_id_base}'] = f'{resolved} (fallback default)'
    return resolved


def envify_connection(conn_id: str) -> str:
    return resolve_connection(conn_id)
