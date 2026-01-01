import io
import zipfile

import pytest
from starlette.datastructures import UploadFile

import api.server as server


class DummyProcess:
    def __init__(self, target=None, args=()):
        self._target = target
        self._args = args
        self.exitcode = None
        self.pid = 1234
        self._alive = False

    def start(self):
        self._alive = True

    def is_alive(self):
        return self._alive

    def terminate(self):
        self._alive = False
        self.exitcode = -15

    def join(self, timeout=None):
        return None


class DummyQueue:
    def __init__(self):
        self._items = []

    def put(self, item):
        self._items.append(item)

    def get_nowait(self):
        if not self._items:
            raise Exception("empty")
        return self._items.pop(0)

    def empty(self):
        return len(self._items) == 0


@pytest.fixture()
def api_state(tmp_path, monkeypatch):
    monkeypatch.setattr(server.multiprocessing, "Process", DummyProcess)
    monkeypatch.setattr(server.multiprocessing, "Queue", DummyQueue)
    monkeypatch.setattr(server, "_BASE_RUN_DIR", str(tmp_path))
    with server._jobs_lock:
        server._jobs.clear()
    server._scenario_index.clear()
    return tmp_path


def _build_input_zip():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("network/agency.txt", "")
        archive.writestr("demand/config_ft.txt", "")
        archive.writestr("demand/pathweight_ft.txt", "")
    buffer.seek(0)
    return buffer


def test_upload_without_file_returns_failure(api_state):
    resp = server.upload_input(scenarioId="scenario_a", needFile=None)
    assert resp.code == 1
    assert resp.value == 1


def test_upload_starts_run_and_status_reports_running(api_state):
    zip_buffer = _build_input_zip()
    upload = UploadFile(filename="input.zip", file=zip_buffer)
    resp = server.upload_input(scenarioId="scenario_b", needFile=upload)
    assert resp.code == 0
    assert resp.value == 0

    status = server.get_scenario_status("scenario_b")
    assert status.code == 0
    assert status.value.scenarioId == "scenario_b"
    assert status.value.status == "running"
    assert status.value.progress == 0


def test_result_endpoint_requires_known_scenario(api_state):
    resp = server.receive_result(scenarioId="unknown", code=1, message="ok", resultFile=None)
    assert resp.code == 1
    assert resp.success is False
    assert resp.message == "scenarioId not exist"


def test_result_endpoint_accepts_result_file(api_state):
    zip_buffer = _build_input_zip()
    upload = UploadFile(filename="input.zip", file=zip_buffer)
    resp = server.upload_input(scenarioId="scenario_c", needFile=upload)
    assert resp.code == 0

    result_zip = io.BytesIO()
    with zipfile.ZipFile(result_zip, "w") as archive:
        archive.writestr("pathset_links.csv", "col\n")
        archive.writestr("pathset_paths.csv", "col\n")
    result_zip.seek(0)

    result_upload = UploadFile(filename="resultFile.zip", file=result_zip)
    resp = server.receive_result(
        scenarioId="scenario_c",
        code=1,
        message="done",
        resultFile=result_upload,
    )
    assert resp.code == 0
    assert resp.success is True
    assert resp.message == "done"
