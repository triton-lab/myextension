import json
import os
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
import urllib.request
import urllib.parse
from typing import List, Dict, NamedTuple, Iterable, Optional
import uuid
from urllib.error import HTTPError

from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
import tornado.web

from .utils import open_or_create_db, get_hub_service_url, get_header_auth_keyval
from .errors import FailedAwsJobRequestError, JupyterHubNotFoundError


DRY_RUN = (not bool(os.environ.get("JUPYTERHUB_API_URL", ""))) or bool(
    os.environ.get("JUPYTERLAB_BATCH_DRYRUN", "")
)


class RouteHandler(APIHandler):
    # The following decorator should be present on all verb methods (head, get, post,
    # patch, put, delete, options) to ensure only authorized user can request the
    # Jupyter server
    @tornado.web.authenticated
    def get(self):
        self.finish(json.dumps({"data": "This is /myextension/get_example endpoint!"}))


class TestHubHandler(APIHandler):
    # The following decorator should be present on all verb methods (head, get, post,
    # patch, put, delete, options) to ensure only authorized user can request the
    # Jupyter server
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not bool(os.environ.get("JUPYTERHUB_API_URL", "")):
            self.log.error(">>>>========================================")
            self.log.error("  Failed to get JUPYTERHUB_API_URL: running properly?")
            self.log.info("    Activates DRY_RUN due to lack of JupyterHub")
            self.log.error("<<<<========================================")

    @tornado.web.authenticated
    def get(self):
        self.log.info(">>>>========================================")
        self.log.info("   TestHubHandler: GET received")
        self.log.info("<<<<========================================")
        status = self._get_status()
        self.finish(status)

    def _get_status(self) -> Dict:
        url = get_hub_service_url("/status")
        self.log.info(f"Accessing /status in hub: {url}")
        return self._http_get(url)

    def _http_get(self, url):
        self.log.info(">>>>--------------------------------------------")
        self.log.info("  Sending HTTP GET request")
        self.log.info(f"    {url}")
        self.log.info("<<<<--------------------------------------------")
        return self._http_meta(url, method="GET")

    def _http_meta(self, url: str, method: str = "GET"):
        req = urllib.request.Request(url=url, method=method)
        auth_keyval = get_header_auth_keyval()
        if auth_keyval is None:
            self.set_status(500)
            self.write(json.dumps({"data": f"JupyterHub auth info is not found."}))
            raise JupyterHubNotFoundError("JupyterHub auth info is not found.")
        req.add_header(*auth_keyval)
        return self._send_request(req)

    def _send_request(self, req: urllib.request.Request) -> Dict:
        try:
            with urllib.request.urlopen(req) as response:
                result = json.loads(response.read())
                return result
        except HTTPError as e:
            if e.code != 200 or e.code != 201 or e.code != 204:
                self.log.error(">>>>=======================================")
                self.log.error(f"{e.code}: Failed {req.get_full_url()}")
                self.log.error(e)
                self.log.error("<<<<=======================================")
                self.set_status(500)
                self.write(json.dumps({"data": f"JupyterHub service responded with an error: {e}\n{req}"}))
            else:
                self.log.info(f"{e.code}: OK")
        return dict()


class JobStatus(Enum):
    OPENED = auto()
    PREPARING = auto()
    RUNNING = auto()
    STOPPING = auto()
    TERMINATED = auto()
    UNKNOWN = auto()


class JobStatusEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, JobStatus):
            return obj.name
        return super().default(obj)


class JobMetadata(NamedTuple):
    job_id: str
    name: str
    file_path: str
    timestamp: datetime
    request_id: str
    instance_id: str
    instance_type: str
    extra: str


class JobInfo(NamedTuple):
    job_id: str
    name: str
    file_path: str
    timestamp: datetime
    request_id: str
    instance_id: str
    instance_type: str
    extra: str
    status: JobStatus
    console_output: str


def to_status(request_state: str, instance_state: str) -> JobStatus:
    if request_state == "open":
        status = JobStatus.OPENED
    elif instance_state == "pending":
        status = JobStatus.PREPARING
    elif instance_state == "running":
        status = JobStatus.RUNNING
    elif instance_state == "shutting-down":
        status = JobStatus.STOPPING
    elif instance_state == "terminated":
        status = JobStatus.TERMINATED
    else:
        # TODO: Add log saying something is wrong
        status = JobStatus.UNKNOWN
    return status


class JobListHandler(APIHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = open_or_create_db()

        if not bool(os.environ.get("JUPYTERHUB_API_URL", "")):
            self.log.error(">>>>========================================")
            self.log.error("  Failed to get JUPYTERHUB_API_URL: running properly?")
            self.log.info("    Activates DRY_RUN due to lack of JupyterHub")
            self.log.error("<<<<========================================")

    def __del__(self):
        self.db.close()

    @tornado.web.authenticated
    def get(self):
        """Get the job list as JSON"""
        self.log.info(">>>>========================================")
        self.log.info("   GET request received")
        self.log.info("<<<<========================================")
        jobs_info = self._get_job_info()
        jobs_as_dicts = [x._asdict() for x in jobs_info]
        self.log.info(f"{jobs_as_dicts=}")
        s = json.dumps(jobs_as_dicts, cls=JobStatusEncoder)
        self.log.info(f"{s = }")
        self.finish(json.dumps({"data": s}))

    @tornado.web.authenticated
    def post(self):
        """Receive a filepath, and then request a job to the JupyterHub 'batch' service"""
        # tornado.escape.json_decode(self.request.body) works similarly
        self.log.info(">>>>========================================")
        self.log.info("   POST request received")
        self.log.info("<<<<========================================")
        payload: Optional[Dict] = self.get_json_body()
        if payload is None:
            self.set_status(400)
            self.finish(json.dumps({"data": "POST needs 'path' field"}))
            return

        for x in ("name", "path", "instance_type"):
            if x not in payload:
                self.set_status(400)
                self.finish(json.dumps({"data": f"POST needs '{x}' field"}))
                return
        name = payload["name"]
        apipath = payload["path"]
        instance_type = payload["instance_type"]
        self.log.info(f"HTTP POST: Received file '{apipath}'")

        filepath = Path(self.settings["server_root_dir"]).expanduser() / apipath
        self.log.info(f"HTTP POST: filepath: '{filepath}'")
        if not filepath.exists():
            self.set_status(400)
            self.finish(json.dumps({"data": f"The file does not exist: {apipath}"}))
            return

        if filepath.suffix not in (".ipynb", ".sh"):
            self.set_status(400)
            self.finish(
                json.dumps(
                    {
                        "data": f"Batch job takes either a Jupyter notebook or shell script: {apipath}"
                    }
                )
            )
            return

        job_id = str(uuid.uuid4())  # TODO: check collision of job ID?

        if DRY_RUN:
            self.log.debug("DRY_RUN: SpotInstanceRequestId and InstanceId are made up.")
            res = {
                "LaunchTime": datetime.utcnow(),
                "SpotInstanceRequestId": str(uuid.uuid4()),
                "InstanceId": str(uuid.uuid4()),
            }
        else:
            try:
                res = self._start_job(job_id, filepath)
            except JupyterHubNotFoundError:
                msg = f"JupyterHubNotFoundError: This extension works only with JupyterHub."
                self.log.error(">>>>=======================================")
                self.log.error(msg)
                self.log.error(">>>>=======================================")
                self.set_status(500)
                self.finish(json.dumps({"data": msg}))
                return
            except HTTPError:
                msg = f"HTTPError: Check the internet connection."
                self.log.error(">>>>=======================================")
                self.log.error(msg)
                self.log.error(">>>>=======================================")
                self.set_status(500)
                self.finish(json.dumps({"data": msg}))
                return
            except FailedAwsJobRequestError:
                msg = f"Failed to start a job at AWS: {apipath}"
                self.log.error(">>>>=======================================")
                self.log.error(msg)
                self.log.error(">>>>=======================================")
                self.set_status(500)
                self.finish(json.dumps({"data": msg}))
                return

        meta = JobMetadata(
            job_id,
            name,
            apipath,
            res["LaunchTime"],
            res["SpotInstanceRequestId"],
            instance_id=res["InstanceId"],
            instance_type=instance_type,
            extra="",
        )
        self._db_add(meta)
        self.get()

    @tornado.web.authenticated
    def delete(self, job_id: str):
        """send cancel to the JupyterHub service 'batch'"""
        self.log.info(">>>>========================================")
        self.log.info("  DELETE request received")
        self.log.info(f"     Job ID: {job_id}")
        self.log.info("<<<<========================================")
        meta = self._db_read(job_id)
        if DRY_RUN:
            self.log.debug("DRY_RUN activated in delete()")
        else:
            res = self._cancel_job(meta.request_id, meta.instance_id)

        # TODO: check job deletion succeeds
        self._db_delete(job_id)
        # TODO: check if local job deletion succeeds
        self.get()

    def _http_meta(self, url: str, method: str = "GET"):
        req = urllib.request.Request(url=url, method=method)
        auth_keyval = get_header_auth_keyval()
        if auth_keyval is None:
            self.set_status(500)
            self.write(json.dumps({"data": f"JupyterHub auth info is not found."}))
            raise JupyterHubNotFoundError("JupyterHub is not running?")
        req.add_header(*auth_keyval)
        return self._send_request(req)

    def _http_get(self, url):
        self.log.info(">>>>--------------------------------------------")
        self.log.info("  Sending HTTP GET request")
        self.log.info(f"    {url}")
        self.log.info("<<<<--------------------------------------------")
        return self._http_meta(url, method="GET")

    def _http_delete(self, url):
        self.log.info(">>>>--------------------------------------------")
        self.log.info("  Sending HTTP DELETE request")
        self.log.info(f"    {url}")
        self.log.info("<<<<--------------------------------------------")
        return self._http_meta(url, method="DELETE")

    def _http_post(self, url: str, data: bytes):
        self.log.info(">>>>--------------------------------------------")
        self.log.info("  Sending HTTP POST request")
        self.log.info(f"    {url}")
        self.log.info(f"    {data.decode()}")
        self.log.info("<<<<--------------------------------------------")
        req = urllib.request.Request(url=url, data=data, method="POST")
        auth_keyval = get_header_auth_keyval()
        if auth_keyval is None:
            raise JupyterHubNotFoundError("JupyterHub is not running?")

        req.add_header("Content-Type", "text/plain")
        req.add_header(*auth_keyval)
        return self._send_request(req)

    def _start_job(self, job_id: str, filename: Path) -> Dict:
        """Send as POST request to start an instance

        Returns a dictionary including
            - InstanceId
            - SpotInstanceRequestId
            - LaunchTime
        """
        url = get_hub_service_url(f"/job/{job_id}")
        self.log.debug(f"url: {url}")
        with open(filename, "rb") as f:
            data = f.read()
        result = self._http_post(url, data)
        ## TODO: Align with JupyterHub's failure modes
        if (
            "response" not in result
            or not result["response"].ok
            or "status" not in result
            or result["status"].lower().startswith("fail")
        ):
            msg = "AWS somehow failed to start EC2 instance request"
            self.log.error(">>>>=======================================")
            self.log.error(url)
            self.log.error(msg)
            self.log.error(result)
            self.log.error(">>>>=======================================")
            raise FailedAwsJobRequestError(msg)
        return result

    def _ask_jobs_status(
        self, request_ids: Iterable[str], instance_ids: Iterable[str]
    ) -> Dict:
        """
        Returns a dictonary with <InstanceId> as keys
            <InstanceId>:
                SpotInstanceRequestState: 'open|active|closed|cancelled|failed'
                InstanceState 'pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped'
                ConsoleOutput: <str>
                _SpotInstanceRequestResponse: <dict>
                _InstanceStatusResponse: <dict>
        """
        params = urllib.parse.urlencode(
            {
                "request_ids": ",".join(request_ids),
                "instance_ids": ",".join(instance_ids),
            }
        )
        url = get_hub_service_url(f"/job?{params}")
        self.log.info(f"Asking jobs statuses: {url}")
        return self._http_get(url)

    def _cancel_job(self, request_id: str, instance_id: str) -> Dict:
        """
        Return a dict with response keys:
            - CancelSpotInstanceRequestsResponse
                - SpotInstanceRequestId
                - State
            - TerminateInstanceRequestsResponse
                - InstanceId
                - CurrentState
                    - Code
                    - Name
        """
        params = urllib.parse.urlencode(
            {
                "request_id": request_id,
                "instance_id": instance_id,
            }
        )
        url = get_hub_service_url(f"/job?{params}")

        self.log.debug(f"Canceling job: {url}")
        return self._http_delete(url)

    def _send_request(self, req: urllib.request.Request) -> Dict:
        try:
            with urllib.request.urlopen(req) as response:
                result = json.loads(response.read())
                return result
        except HTTPError as e:
            if e.code != 200 or e.code != 201 or e.code != 204:
                self.log.error(">>>>=======================================")
                self.log.error(f"{e.code}: Failed {req.get_full_url()}")
                self.log.error(e)
                self.log.error("<<<<=======================================")
                self.set_status(500)
                self.write(json.dumps({"data": f"JupyterHub service responded with an error: {e}"}))
            else:
                self.log.info(f"{e.code}: OK")
        return dict()

    def _get_job_info(self) -> List[JobInfo]:
        jobs_metadata = self._get_jobmeta_all()

        if DRY_RUN:
            self.log.debug(f"*** DRY_RUN activated ***")
            self.log.debug("making up job status, console_output")

            def to_info(x: JobMetadata) -> JobInfo:
                d = {
                    "status": JobStatus.OPENED,
                    "console_output": "---------  OUTPUT -----------",
                    "extra": "",
                    **x._asdict(),
                }
                return JobInfo(**d)

            return [to_info(meta) for meta in jobs_metadata]

        request_ids = [meta.request_id for meta in jobs_metadata]
        instance_ids = [meta.instance_id for meta in jobs_metadata]
        instance_type = [meta.instance_type for meta in jobs_metadata]
        if not jobs_metadata:
            return []

        r = self._ask_jobs_status(request_ids=request_ids, instance_ids=instance_ids)
        result: List[JobInfo] = []
        for x in jobs_metadata:
            id_ = x.instance_id
            request_state = r[id_]["SpotInstanceRequestState"]
            instance_state = r[id_]["InstanceState"]
            console_output = r[id_]["ConsoleOutput"]
            # TODO: check instance_type and other fields agrees with `x`
            status = to_status(
                request_state=request_state, instance_state=instance_state
            )
            jobinfo = JobInfo(
                x.job_id,
                x.name,
                x.file_path,
                x.timestamp,
                x.request_id,
                x.instance_id,
                x.instance_type,
                "",
                status,
                console_output,
            )
            result.append(jobinfo)
        return result

    def _get_jobmeta_all(self) -> List[JobMetadata]:
        self.log.debug("Reading all from db")
        res = self.db.execute("select * from jobmeta")
        ## TODO: Get status
        return [JobMetadata._make(tup) for tup in res.fetchall()]

    def _db_read(self, job_id: str) -> JobMetadata:
        self.log.debug(f"Reading an entry from db: {job_id}")
        cur = self.db.execute("select * from jobmeta where job_id=?", (job_id,))
        res = JobMetadata._make(cur.fetchone())
        return res

    def _db_add(self, jobmeta: JobMetadata) -> None:
        self.log.debug(f"Adding an entry to db: {jobmeta}")
        with self.db:
            self.db.execute(
                "insert into jobmeta values (?, ?, ?, ?, ?, ?, ?, ?)", jobmeta
            )

    def _db_delete(self, job_id: str) -> None:
        self.log.debug(f"Deleting an entry to db: {job_id}")
        with self.db:
            self.db.execute("delete from jobmeta where job_id=?", (job_id,))


def setup_handlers(web_app):
    base_url = web_app.settings["base_url"]

    def f(api_name: str) -> str:
        return url_path_join(base_url, "myextension", api_name)

    handlers = [
        (f("get_example"), RouteHandler),
        (f("testhub"), TestHubHandler),
        (f("jobs"), JobListHandler),
        (f("jobs/(.*)"), JobListHandler),
    ]

    host_pattern = ".*$"
    web_app.add_handlers(host_pattern, handlers)
