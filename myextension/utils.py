import dataclasses
import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, Optional, Tuple
import sqlite3
from sqlite3 import Connection
import urllib.parse
from http.client import HTTPResponse
import platformdirs

from .types import JobMetadata


class JupyterPathLoadingError(Exception):
    pass


class JobDBUpgradeError(Exception):
    pass


class IncompatibleDbError(Exception):
    pass


def _get_default_db_path() -> Path:
    p = platformdirs.user_data_dir("jupyter")
    return Path(p) / "batchjob.db"


def get_db_path(db: Connection) -> Path:
    res = db.execute("pragma database_list;")
    if res is None:
        return _get_default_db_path()
    return Path(res.fetchone()[2])


def _create_db(p: Path) -> Connection:
    logging.info(">>>-----------------------")
    logging.info("  Creating SQLite DB: %s", p)
    db = sqlite3.connect(p)
    entries = ", ".join(
        f"{field.name} text" for field in dataclasses.fields(JobMetadata)
    )
    db.execute(f"create table jobmeta ({entries})")
    logging.info("<<<-----------------------")
    return db


def open_or_create_db(p: Optional[Path] = None) -> Connection:
    if p is None:
        p = _get_default_db_path()
    db = sqlite3.connect(p) if p.exists() else _create_db(p)

    if _is_db_outdated(db):
        logging.warning(">>>>==============================")
        logging.warning("  Upgrading the job DB")
        db = _upgrade_db(db)
        logging.warning("<<<<==============================")
    return db


def _is_db_outdated(db: Connection) -> bool:
    """Check if the database schema is comptible with with JobMetadata"""
    rows = db.execute("pragma table_info(jobmeta)").fetchall()
    db_fields = tuple(row[1] for row in rows)
    jobmeta_fields = tuple(x.name for x in dataclasses.fields(JobMetadata))
    res = db_fields != jobmeta_fields
    if res:
        logging.info("  db_fields     : %s", db_fields)
        logging.info("  jobmeta_fields: %s", jobmeta_fields)
    return res


def _upgrade_db(db: Connection) -> Connection:
    """Make DB compatible with JobMetadata fields"""
    # extract data to memory
    xs = db.execute("pragma table_info(jobmeta)").fetchall()
    past_field_names = tuple(x[1] for x in xs)
    jobmeta_fields = [x.name for x in dataclasses.fields(JobMetadata)]
    past_rows = db.execute("select * from jobmeta").fetchall()
    p = get_db_path(db)

    if not (set(past_field_names) < set(jobmeta_fields)):
        logging.warning(">>>======================================================")
        logging.warning("  Existing DB is incompatible with JobMetadata")
        logging.warning("   unique past field names: ")
        for name in set(past_field_names) - set(jobmeta_fields):
            logging.warning("     %s", name)
        logging.warning(" ---> Recreating the DB file: %s", p)
        logging.warning(">>>======================================================")
        db.close()
        p.unlink()
        return _create_db(p)

    # remove the original file
    db.close()
    p.unlink()
    db = _create_db(p)

    slots = ", ".join("?" for _ in jobmeta_fields)
    with db:
        for row in past_rows:
            d = dict(zip(past_field_names, row))
            item = JobMetadata(**d)  # supply default value here
            db.execute(
                f"insert into jobmeta values ({slots})", dataclasses.astuple(item)
            )

    return db


def join_url_parts(*parts):
    return "/".join(re.sub(r"^/|/$", "", part) for part in parts)


def get_hub_service_url(api: str) -> str:
    url = os.environ.get("JUPYTERHUB_API_URL", "http://127.0.0.1")
    parsed = urllib.parse.urlparse(url)
    # FIXME: TODO: This port number is hard coded.
    # Must match the "url" in the response from $JUPYTERHUB_API_URL/services/batch
    # Also consider using JUPYTERHUBSERVICE_URL if available.
    # https://jupyterhub.readthedocs.io/en/stable/reference/spawners.html#environment-variables-and-command-line-arguments
    base = parsed.scheme + "://" + (parsed.hostname or "") + ":12345"
    return join_url_parts(base, "/services/batch", api)


def get_header_auth_keyval() -> Optional[Tuple[str, str]]:
    tokenval = os.environ.get("JUPYTERHUB_API_TOKEN", "")
    if not tokenval:
        return None
    return ("Authorization", f"token {tokenval}")


def asjson(msg: str) -> str:
    return json.dumps({"data": msg})


def shorten_id(job_id: str) -> str:
    """Assume job_id is UUID4. Returns the first part."""
    parts = job_id.split("-")
    return parts[0]


def get_output_path(meta: JobMetadata, root_dir: str = "") -> Path:
    """Output path for the batch file described in `meta`.

    [NOTE] This must agree with getOutputPath() in utils.ts
    """
    p = Path(meta.file_path)
    # remove directory and extension if included in the name
    job_name = Path(meta.name).stem
    # root is based on jupyterlab's SingleUserNotebookApp.notebook_dir / ServerApp.root_dir
    root = Path(root_dir) / p.parent
    # This may not be the most user-friendly
    container = job_name + "_" + shorten_id(meta.job_id)
    path = root / container
    path.mkdir(exist_ok=True)
    return path


def get_root_dir(config: Dict) -> str:
    """Dig config and extract root_dir / notebook_dir information"""
    root_dir = config.get("SingleUserNotebookApp", {}).get("notebook_dir", None)

    if root_dir is None:
        root_dir = config.get("SingleUserNotebookApp", {}).get("root_dir", None)

    if root_dir is None:
        root_dir = config.get("ServerApp", {}).get("root_dir", None)

    if root_dir is None:
        root_dir = config.get("ServerApp", {}).get("notebook_dir", None)

    return root_dir or ""


def get_filename_from_response(response: HTTPResponse) -> Optional[str]:
    """Get filename from a HTTP Response"""
    content_disposition = response.getheader("Content-Disposition")
    if content_disposition:
        value, params = content_disposition.split(";")
        if value.lower() == "attachment":
            return params.strip().split("=")[-1].strip('"')
    return None
