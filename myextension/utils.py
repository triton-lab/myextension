import json
import os
import re
from pathlib import Path
from typing import Optional, Tuple
import sqlite3
from sqlite3 import Connection
import platformdirs
import urllib.parse

from .types import JobMetadata

class JupyterPathLoadingError(Exception):
    pass


def _get_db_path() -> Path:
    p = platformdirs.user_data_dir("jupyter")
    return Path(p) / "batchjob.db"


def _create_db(p: Path) -> Connection:
    print("-----------------------")
    print(f"Creating {p}")
    print("-----------------------")
    db = sqlite3.connect(p)
    db.execute(
        "create table jobmeta (job_id text, name text, file_path text, datetime text, request_id text, instance_id text, instance_type text, extra text)"
    )
    return db


def open_or_create_db(p: Optional[Path] = None) -> Connection:
    if p is None:
        p = _get_db_path()
    return sqlite3.connect(p) if p.exists() else _create_db(p)


def join_url_parts(*parts):
    return "/".join(re.sub(r"^/|/$", "", part) for part in parts)


def get_hub_service_url(api: str) -> str:
    url = os.environ.get("JUPYTERHUB_API_URL", "http://127.0.0.1")
    parsed = urllib.parse.urlparse(url)
    # FIXME: This port number is hard coded.
    # Must match the "url" in the response from $JUPYTERHUB_API_URL/services/batch
    base = parsed.scheme + "://" + (parsed.hostname or "") + ":12345"
    return join_url_parts(base, f"/services/batch", api)


def get_header_auth_keyval() -> Optional[Tuple[str, str]]:
    tokenval = os.environ.get("JUPYTERHUB_API_TOKEN", "")
    if not tokenval:
        return None
    return ("Authorization", f"token {tokenval}")


def asjson(msg: str) -> str:
    return json.dumps({"data": msg})


def shorten_id(job_id: str) -> str:
    """Assume job_id is UUID4. Returns the first part.
    """
    parts = job_id.split("-")
    return parts[0]


def get_output_path(meta: JobMetadata) -> str:
    """"
    """
    p = Path(meta.file_path)
    job_name = meta.name
    filename = p.name
    root = p.parent
    parent_dir = job_name + "_" + shorten_id(meta.job_id)
    path = root / parent_dir
    path.mkdir(exist_ok=True)
    return (path / filename).as_posix()
