import os
import re
from pathlib import Path
from typing import Optional, Tuple
import sqlite3
from sqlite3 import Connection
import platformdirs


class JupyterPathLoadingError(Exception):
    pass


def _get_db_path() -> Path:
    p = platformdirs.user_data_dir("jupyter")
    return Path(p) /  "batchjob.db"


def _create_db(p: Path) -> Connection:
    print("-----------------------")
    print(f"Creating {p}")
    print("-----------------------")
    db = sqlite3.connect(p)
    db.execute("create table jobmeta (job_id text, name text, file_path text, datetime text, request_id text, instance_id text, instance_type text, extra text)")
    return db


def open_or_create_db(p: Optional[Path]=None) -> Connection:
    if p is None:
        p = _get_db_path()
    return sqlite3.connect(p) if p.exists() else _create_db(p)


def join_url_parts(*parts):
    return '/'.join(re.sub(r'^/|/$', '', part) for part in parts)


def get_hub_service_url(api: str) -> str:
    base = "http://127.0.0.1"
    return join_url_parts(base,  f"/services/batch", api)


def get_header_auth_keyval() -> Optional[Tuple[str, str]]:
    tokenval = os.environ.get('JUPYTERHUB_API_TOKEN', '')
    if not tokenval:
        return None
    return ('Authorization', f"token {tokenval}")
