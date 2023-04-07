from contextlib import closing
import json
import os
from datetime import datetime

from pathlib import Path
from sqlite3 import Connection
import requests
import urllib.request
import urllib.parse
from typing import List, Dict, Iterable, Optional
import uuid
from urllib.error import HTTPError

from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
import tornado.web

from . import utils
from .utils import open_or_create_db, get_hub_service_url, get_header_auth_keyval
from .errors import FailedAwsJobRequestError, JupyterHubNotFoundError
from .types import JobInfo, JobMetadata, JobStatus, JobStatusEncoder, to_status


DRY_RUN = (not bool(os.environ.get("JUPYTERHUB_API_URL", ""))) or bool(
    os.environ.get("JUPYTERLAB_BATCH_DRYRUN", "")
)


class RouteHandler(APIHandler):
    # The following decorator should be present on all verb methods (head, get, post,
    # patch, put, delete, options) to ensure only authorized user can request the
    # Jupyter server
    @tornado.web.authenticated
    def get(self):
        self.log.info(">>>>========================================")
        self.log.info("   RouteHandler: GET received")
        self.log.info("<<<<========================================")
        self.log.info("")
        self.finish(json.dumps({"data": "This is /myextension/get_example endpoint!"}))


class ConfigViewHandler(APIHandler):
    """For debug / testing purposes
    """
    @tornado.web.authenticated
    def get(self):
        self.log.info(">>>>========================================")
        self.log.info(" ConfigHandler: GET received")
        self.log.info("   settings:")
        for k, v in sorted(self.settings.items()):
            self.log.info(f"      {k}: {v}")
        self.log.info("   lab_config:")
        for k, v in sorted(self.settings["lab_config"].items()):
            self.log.info(f"      {k}: {v}")
        self.log.info(f"   config: {self.config}")
        self.log.info("<<<<========================================")
        self.log.info("")
        self.finish(json.dumps(self.config))


class TestHubHandler(APIHandler):
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
                self.write(
                    json.dumps(
                        {
                            "data": f"JupyterHub service responded with an error: {e}\n{req}"
                        }
                    )
                )
            else:
                self.log.info(f"{e.code}: OK")
        return dict()


class B2DownloadHandler(APIHandler):
    """Download file(s) from B2 as batch job results

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = open_or_create_db()
        self.root_dir = utils.get_root_dir(self.config) # type: ignore

    @tornado.web.authenticated
    def get(self, job_id):
        self.log.info(">>>>========================================")
        self.log.info("  B2DownloadHandler: GET received")
        self.log.info(f"    job_id: {job_id}")

        meta = db_read(self.db, job_id)
        filename = Path(meta.file_path).name
        params = urllib.parse.urlencode({"job_id": job_id, "filename": filename})
        url = get_hub_service_url(f"/download?{params}")
        output_path = utils.get_output_path(meta, self.root_dir)

        self.log.info(f"    filename: {filename}")
        self.log.info(f"    Accessing with HTTP GET: {url}")
        self.log.info("<<<<========================================")
        req = urllib.request.Request(url=url, method='GET')
        auth_keyval = get_header_auth_keyval()
        if auth_keyval is None:
            self.set_status(500)
            self.write(json.dumps({"data": f"JupyterHub auth info is not found."}))
            raise JupyterHubNotFoundError("JupyterHub is not running?")
        req.add_header(*auth_keyval)
        try:
            with closing(urllib.request.urlopen(req)) as response:
                with open(output_path, 'wb') as f:
                    content = response.read()
                    f.write(content)

            self.log.info(">>>>=================================================")
            self.log.info(f"    Content of {output_path}")
            self.log.info(content)
            self.log.info("<<<<=================================================")
        except HTTPError as e:
            msg = f"Error: Unable to download the file. Status code: {e.code}"
            self.log.error(">>>>========================================")
            self.log.error(f"Error: {msg}")
            self.log.error("<<<<========================================")
            self.set_status(e.code)
            self.write(utils.asjson(msg))
        except Exception as e:
            msg = f"Error: {e}"
            self.log.error(">>>>========================================")
            self.log.error(f"Error downloading and saving file: {msg}")
            self.log.error("<<<<========================================")
            self.set_status(500)
            self.write(utils.asjson(msg))


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
        self.finish(s)

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

        params: Dict[str, str] = dict()
        for x in ("name", "path", "instance_type", "max_coins_per_hour"):
            if x not in payload:
                self.set_status(400)
                self.finish(json.dumps({"data": f"POST needs '{x}' field"}))
                return
            params[x] = payload[x]
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
                res = self._start_job(job_id, filepath, params)
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
            except FailedAwsJobRequestError as e:
                self.log.error(">>>>=======================================")
                self.log.error(f"Failed to start a job at AWS: {apipath}")
                self.log.error(e)
                self.log.error(">>>>=======================================")
                self.set_status(500)
                self.finish(json.dumps(e, cls=JobStatusEncoder, indent=1))
                return

        ## TODO: Should I include `max_coins_per_hour` information here?
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
        self.log.debug(f"Adding an entry to db: {meta}")
        db_add(self.db, meta)
        self.get()

    @tornado.web.authenticated
    def delete(self, job_id: str):
        """send cancel to the JupyterHub service 'batch'"""
        self.log.info(">>>>========================================")
        self.log.info("  DELETE request received")
        self.log.info(f"     Job ID: {job_id}")
        self.log.info("<<<<========================================")

        self.log.debug(f"Reading an entry from db: {job_id}")
        meta = db_read(self.db, job_id)
        if DRY_RUN:
            self.log.debug("DRY_RUN activated in delete()")
        else:
            self.log.debug(f"---- Cancel Job: {job_id} ----")
            res = self._cancel_job(meta.request_id, meta.instance_id)
            self.log.debug(res)

        # TODO: delete wisely based on `res`
        self.log.debug(f"Deleting an entry to db: {job_id}")
        db_delete(self.db, job_id)
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

    def _http_post_multipart(
        self, url: str, filename: Path, params: Dict
    ) -> requests.Response:
        self.log.info(">>>>--------------------------------------------")
        self.log.info("  Sending HTTP POST request")
        self.log.info(f"    {url}")
        self.log.info(f"    {filename}")
        self.log.info(f"    {params}")
        self.log.info("<<<<--------------------------------------------")
        files = {
            "file": (filename.name, filename.open("rb"), "text/plain", {"Expires": "0"})
        }
        auth_keyval = get_header_auth_keyval()
        if auth_keyval is None:
            raise JupyterHubNotFoundError("JupyterHub is not running?")

        headers = dict([auth_keyval])
        res = requests.post(url, files=files, headers=headers, data=params)
        return res

    def _start_job(self, job_id: str, filename: Path, params: Dict) -> Dict:
        """Send as POST request to start an instance

        Returns a dictionary including
            - InstanceId
            - SpotInstanceRequestId
            - LaunchTime
        """
        url = get_hub_service_url(f"/job/{job_id}")
        self.log.debug(f"url: {url}")
        result = self._http_post_multipart(url, filename, params)
        ## TODO: Align with JupyterHub's failure modes
        if not result.ok:
            self.log.error(">>>>=======================================")
            self.log.error("AWS somehow failed to start EC2 instance request")
            self.log.error(url)
            self.log.error(result.json())
            self.log.error(">>>>=======================================")
            raise FailedAwsJobRequestError(result.json())
        return result.json()

    def _ask_jobs_status(
        self, request_ids: Iterable[str], instance_ids: Iterable[str]
    ) -> Dict:
        """
        Returns a dictonary with <InstanceId> as keys
            <InstanceId>:
                request: open|active|closed|cancelled|failed|notfound-id,
                instance pending|running|shutting-down|terminated|stopping|stopped|info-empty|notfound-id,
                (optinal) console: <str>
                (optinal) _SpotInstanceRequest: <dict>
                (optinal) _InstanceStatus: <dict>
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
            cancel: notfound_request_id|error...|<cancel_spot_instance_requests response>
            terminate: notfound_instance_id|error...|<terminate_instances response>
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
                self.write(
                    json.dumps(
                        {"data": f"JupyterHub service responded with an error: {e}"}
                    )
                )
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
        if not jobs_metadata:
            return []

        r = self._ask_jobs_status(request_ids=request_ids, instance_ids=instance_ids)
        self.log.debug("---- Job status ----")
        self.log.debug(r)

        result: List[JobInfo] = []
        for x in jobs_metadata:
            id_ = x.instance_id
            request_state = r[id_]["request"]
            instance_state = r[id_]["instance"]
            console_output = r[id_].get("console", "")
            # TODO: check instance_type and other fields agrees with `x`
            status = to_status(request_state, instance_state, console_output)
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



def setup_handlers(web_app):
    base_url = web_app.settings["base_url"]

    def f(api_name: str) -> str:
        return url_path_join(base_url, "myextension", api_name)

    handlers = [
        (f("get_example"), RouteHandler),
        (f("testhub"), TestHubHandler),
        (f("config"), ConfigViewHandler),
        (f("jobs"), JobListHandler),
        (f("jobs/(.*)"), JobListHandler),
        (f("download/(.*)"), B2DownloadHandler),
    ]

    host_pattern = ".*$"
    web_app.add_handlers(host_pattern, handlers)




def db_read(db: Connection, job_id: str) -> JobMetadata:

    cur = db.execute("select * from jobmeta where job_id=?", (job_id,))
    res = JobMetadata._make(cur.fetchone())
    return res

def db_add(db: Connection, jobmeta: JobMetadata) -> None:
    with db:
        db.execute(
            "insert into jobmeta values (?, ?, ?, ?, ?, ?, ?, ?)", jobmeta
        )

def db_delete(db: Connection, job_id: str) -> None:
    with db:
        db.execute("delete from jobmeta where job_id=?", (job_id,))
