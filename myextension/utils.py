import json
import os
import subprocess

from pathlib import Path
from typing import Optional, Tuple
import urllib.parse
import sqlite3
from sqlite3 import Connection


class JupyterPathLoadingError(Exception):
    pass


def _get_db_path() -> Path:
    res = subprocess.run("jupyter --paths --json", shell=True, capture_output=True)
    if res.stdout is None:
        raise JupyterPathLoadingError("Failed to load jupyter --path")
    data = json.loads(res.stdout.decode()).get("data", [])
    if len(data) < 1:
        raise Exception("No Jupyter data folder found.")
    return Path(data[0]) /  "batchjob.sqlite"


def _create_db(p: Path) -> Connection:
    print("-----------------------")
    print(f"Creating {p}")
    print("-----------------------")
    db = sqlite3.connect(p)
    db.execute("create table jobmeta (job_id text, datetime text, request_id text, instance_id text)")
    return db


def open_or_create_db(p: Optional[Path]=None) -> Connection:
    if p is None:
        p = _get_db_path()
    return sqlite3.connect(p) if p.exists() else _create_db(p)


def get_hub_service_url(api: str) -> str:
    url = os.environ.get("JUPYTERHUB_API_URL", "localhost:8888")
    return urllib.parse.urljoin(url,  f"/services/batch/{api}")


def get_header_auth_keyval() -> Tuple[str, str]:
    return ('Authorization', f"token {os.environ.get('JUPYTERHUB_API_TOKEN' ,'')}")
