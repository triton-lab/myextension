import json

class JupyterHubNotFoundError(Exception):
    pass


class FailedAwsJobRequestError(Exception):
    def __init__(self, data):
        super().__init__()
        self.data = data


class FailedB2DownloadError(Exception):
    def __init__(self, data):
        super().__init__()
        self.data = data


class ErrorStatusEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, JupyterHubNotFoundError):
            return {"data": "JupyterHubNotFoundError"}
        elif isinstance(obj, FailedAwsJobRequestError):
            return obj.data
        return super().default(obj)
