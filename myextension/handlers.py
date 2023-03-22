import json
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


class RouteHandler(APIHandler):
    # The following decorator should be present on all verb methods (head, get, post,
    # patch, put, delete, options) to ensure only authorized user can request the
    # Jupyter server
    @tornado.web.authenticated
    def get(self):
        self.finish(json.dumps({
            "data": "This is /myextension/get_example endpoint!"
        }))



class JobStatus(Enum):
    OPENED = auto()
    PREPARING = auto()
    RUNNING = auto()
    STOPPING = auto()
    TERMINATED = auto()
    UNKNOWN = auto()



class JobMetadata(NamedTuple):
    job_id: str
    timestamp: datetime
    request_id: str
    instance_id: str


class JobInfo(NamedTuple):
    job_id: str
    timestamp: datetime
    status: JobStatus
    request_id: str
    instance_id: str
    console: str


def to_status(request_state: str, instance_state: str) -> JobStatus:
    if request_state == "open":
        status = JobStatus.OPENED
    elif instance_state == "pending":
        status = JobStatus.PREPARING
    elif instance_state == "running":
        status = JobStatus.RUNNING
    elif instance_state == 'shutting-down':
        status = JobStatus.STOPPING
    elif instance_state == 'terminated':
        status = JobStatus.TERMINATED
    else:
        # TODO: Add log saying something is wrong
        status = JobStatus.UNKNOWN
    return status


class JobListHandler(APIHandler):

    db = open_or_create_db()

    def __del__(self):
        self.db.close()

    def get(self):
        """Get the job list as JSON"""
        print(f"HTTP GET: Received a request'")
        jobs_info = self._get_job_info()
        jobs_as_dicts = [x._asdict() for x in jobs_info]
        self.finish(json.dumps(jobs_as_dicts))


    def post(self):
        """Receive a filepath, and then request a job to the JupyterHub 'batch' service """
        # tornado.escape.json_decode(self.request.body) works similarly
        payload: Optional[Dict] = self.get_json_body()
        if (payload is None) or ('path' not in payload):
            self.set_status(400)
            self.finish("POST needs 'path' field")
            return

        apipath = payload['path']
        print(f"HTTP POST: Received file '{apipath}'")

        filepath = Path(self.settings["server_root_dir"]) / apipath
        if not filepath.exists():
            self.set_status(400)
            self.finish(f"The file does not exist: {apipath}")
            return

        if not filepath.suffix not in (".ipynb", ".sh"):
            self.set_status(400)
            self.finish(f"Batch job takes either a Jupyter notebook or shell script: {apipath}")
            return

        job_id = str(uuid.uuid4())  # TODO: check collision of job ID?
        res = self._start_job(job_id, filepath)
        meta = JobMetadata(job_id, res['LaunchTime'], res['SpotInstanceRequestId'], res['InstanceId'])
        self._db_add(meta)
        self.get()


    def delete(self, job_id: str):
        """send cancel to the JupyterHub service 'batch'"""
        print(f"HTTP DELETE: received {job_id}")
        meta = self._db_read(job_id)
        res = self._cancel_job(meta.request_id, meta.instance_id)
        # TODO: check job deletion succeeds
        self._db_delete(job_id)
        # TODO: check if local job deletion succeeds
        self.get()


    def _http_meta(self, url: str, method: str="GET"):
        req = urllib.request.Request(url=url, method=method)
        req.add_header(*get_header_auth_keyval())
        return self._send_request(req)


    def _http_get(self, url):
        return self._http_meta(url, method='GET')


    def _http_delete(self, url):
        return self._http_meta(url, method='DELETE')


    def _http_post(self, url: str, data: bytes):
        req = urllib.request.Request(url=url, data=data, method='POST')
        req.add_header('Content-Type', 'text/plain')
        req.add_header(*get_header_auth_keyval())
        return self._send_request(req)


    def _start_job(self, job_id: str, filename: Path) -> Dict:
        """Send as POST request to start an instance

        Returns a dictionary including
            - InstanceId
            - SpotInstanceRequestId
            - LaunchTime
        """
        url = get_hub_service_url(f"/job/{job_id}")
        with open(filename, 'rb') as f:
            data = f.read()
        return self._http_post(url, data)


    def _get_status(self) -> Dict:
        url = get_hub_service_url('/status')
        return self._http_get(url)


    def _ask_jobs_status(self, request_ids: Iterable[str], instance_ids: Iterable[str]) -> Dict:
        """
        Returns a dictonary with <InstanceId> as keys
            <InstanceId>:
                SpotInstanceRequestState: 'open|active|closed|cancelled|failed'
                InstanceState 'pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped'
                ConsoleOutput: <str>
                _SpotInstanceRequestResponse: <dict>
                _InstanceStatusResponse: <dict>
        """
        params = urllib.parse.urlencode({
            'request_ids': ','.join(request_ids),
            'instance_ids': ','.join(instance_ids),
        })
        url = get_hub_service_url(f"/job?{params}")
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
        params = urllib.parse.urlencode({
            'request_id': request_id,
            'instance_id': instance_id,
        })
        url = get_hub_service_url(f"/job?{params}")
        return self._http_delete(url)


    def _send_request(self, req: urllib.request.Request) -> Dict:
        try:
            with urllib.request.urlopen(req) as response:
                result = json.loads(response.read())
                return result
        except HTTPError as e:
            if e.code != 200 or e.code != 201 or e.code != 204:
                # TODO: Add error message to the log
                print(f"{e.code}: Failed {req.get_full_url()}")
        # TODO: add json parse error handling

        return dict()


    def _get_job_info(self) -> List[JobInfo]:
        jobs_metadata = self._get_jobmeta_all()
        request_ids = [meta.request_id for meta in jobs_metadata]
        instance_ids = [meta.instance_id for meta in jobs_metadata]
        if not jobs_metadata:
            return []

        r = self._ask_jobs_status(request_ids=request_ids, instance_ids=instance_ids)
        result: List[JobInfo] = []
        for jobmeta in jobs_metadata:
            id_ = jobmeta.instance_id
            request_state = r[id_]["SpotInstanceRequestState"]
            instance_state = r[id_]["InstanceState"]
            console_output = r[id_]["ConsoleOutput"]
            status = to_status(request_state=request_state, instance_state=instance_state)
            jobinfo = JobInfo(jobmeta.job_id, jobmeta.timestamp, status, jobmeta.request_id, jobmeta.instance_id, console_output)
            result.append(jobinfo)
        return result


    def _get_jobmeta_all(self) -> List[JobMetadata]:
        res =  self.db.execute("select * from jobmeta")
        ## TODO: Get status
        return [JobMetadata._make(tup) for tup in res.fetchall()]


    def _db_read(self, job_id: str) -> JobMetadata:
        cur = self.db.execute("select * from jobmeta where job_id=?", job_id)
        res = JobMetadata._make(cur.fetchone())
        return res


    def _db_add(self, jobmeta: JobMetadata) -> None:
        with self.db:
            self.db.execute("insert into jobmeta values (?, ?, ?, ?)", jobmeta)


    def _db_delete(self, job_id: str) -> None:
        with self.db:
            self.db.execute("delete from jobmeta where job_id=?", job_id)


def setup_handlers(web_app):
    base_url = web_app.settings["base_url"]
    def f(api_name: str) -> str:
        return url_path_join(base_url, "myextension", api_name)

    handlers = [
        (f("get_example"), RouteHandler),
        (f("jobs"), JobListHandler),
        (f("jobs/(.*)"), JobListHandler),
    ]

    host_pattern = ".*$"
    web_app.add_handlers(host_pattern, handlers)
