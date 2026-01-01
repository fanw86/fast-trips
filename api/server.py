from __future__ import annotations

import datetime
import os
import threading
import uuid
import zipfile
from typing import Dict, List, Literal, Optional

import multiprocessing
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from fasttrips.Run import run_fasttrips


app = FastAPI(title="Transit Assignment API", version="0.2.0")


class RunRequest(BaseModel):
    pathfinding_type: Literal["deterministic", "stochastic", "file"]
    iters: int = Field(..., ge=0)
    run_config: str
    input_network_dir: str
    input_demand_dir: str
    input_weights: str
    output_dir: str

    output_folder: Optional[str] = None
    trace_only: bool = False
    num_trips: Optional[int] = Field(default=None, ge=1)
    dispersion: Optional[float] = None
    max_stop_process_count: Optional[int] = Field(default=None, ge=1)
    capacity: bool = False
    overlap_variable: Optional[Literal["None", "count", "distance", "time"]] = None
    overlap_split_transit: bool = False
    transfer_fare_ignore_pathfinding: bool = False
    transfer_fare_ignore_pathenum: bool = False
    debug_output_columns: bool = False
    time_window: Optional[float] = None
    pf_iters: Optional[int] = Field(default=None, ge=1)
    number_of_processes: Optional[int] = Field(default=None, ge=1)
    output_pathset_per_sim_iter: bool = False


class RunStatus(BaseModel):
    run_id: str
    status: Literal["running", "succeeded", "failed"]
    pid: int
    started_at: str
    finished_at: Optional[str]
    exit_code: Optional[int]
    output_dir: str
    info_log: str
    debug_log: str
    performance_csv: str
    error: Optional[str] = None


_jobs_lock = threading.Lock()
_jobs: Dict[str, Dict[str, object]] = {}
_scenario_index: Dict[str, str] = {}
_BASE_RUN_DIR = os.environ.get("FASTTRIPS_API_RUN_DIR", os.path.join("api", "runs"))


def _model_to_dict(model: BaseModel) -> Dict[str, object]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _default_output_folder(req: RunRequest) -> str:
    cap_suffix = "cap" if req.capacity else "nocap"
    folder = f"output_{req.pathfinding_type}_iter{req.iters}_{cap_suffix}"
    if req.trace_only:
        folder = f"{folder}_trace"
    return folder


def _build_run_kwargs(req: RunRequest) -> Dict[str, object]:
    raw = _model_to_dict(req)
    kwargs: Dict[str, object] = {}
    for key, value in raw.items():
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                kwargs[key] = value
            continue
        kwargs[key] = value
    return kwargs


def _run_fasttrips_target(kwargs: Dict[str, object], error_queue: multiprocessing.Queue) -> None:
    try:
        run_fasttrips(**kwargs)
    except Exception as exc:  # noqa: BLE001 - surface fast-trips errors to API
        error_queue.put(str(exc))
        raise


def _tail_file(path: str, max_lines: int) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        lines = handle.readlines()
    return "".join(lines[-max_lines:])


def _job_status(run_id: str) -> RunStatus:
    with _jobs_lock:
        job = _jobs.get(run_id)
    if not job:
        raise HTTPException(status_code=404, detail="run_id not found")

    process: multiprocessing.Process = job["process"]  # type: ignore[assignment]
    error_queue: multiprocessing.Queue = job["error_queue"]  # type: ignore[assignment]

    exit_code = process.exitcode
    finished_at = None
    status = "running"
    if exit_code is not None:
        status = "succeeded" if exit_code == 0 else "failed"
        finished_at = job.get("finished_at")
        if not finished_at:
            finished_at = datetime.datetime.utcnow().isoformat() + "Z"
            job["finished_at"] = finished_at

    error = job.get("error")
    if error is None and not error_queue.empty():
        try:
            error = error_queue.get_nowait()
            job["error"] = error
        except Exception:
            error = None

    return RunStatus(
        run_id=run_id,
        status=status,
        pid=process.pid or 0,
        started_at=job["started_at"],
        finished_at=finished_at,
        exit_code=exit_code,
        output_dir=job["output_dir"],
        info_log=job["info_log"],
        debug_log=job["debug_log"],
        performance_csv=job["performance_csv"],
        error=error,
    )


@app.post("/runs", response_model=RunStatus)
def create_run(req: RunRequest) -> RunStatus:
    run_id = uuid.uuid4().hex
    output_folder = req.output_folder or _default_output_folder(req)
    full_output_dir = os.path.join(req.output_dir, output_folder)
    info_log = os.path.join(full_output_dir, "ft_info.log")
    debug_log = os.path.join(full_output_dir, "ft_debug.log")
    performance_csv = os.path.join(full_output_dir, "ft_output_performance.csv")

    kwargs = _build_run_kwargs(req)
    error_queue: multiprocessing.Queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=_run_fasttrips_target, args=(kwargs, error_queue))
    process.start()

    with _jobs_lock:
        _jobs[run_id] = {
            "process": process,
            "error_queue": error_queue,
            "started_at": datetime.datetime.utcnow().isoformat() + "Z",
            "output_dir": full_output_dir,
            "info_log": info_log,
            "debug_log": debug_log,
            "performance_csv": performance_csv,
            "finished_at": None,
            "error": None,
        }

    return _job_status(run_id)


@app.get("/runs", response_model=List[RunStatus])
def list_runs() -> List[RunStatus]:
    with _jobs_lock:
        run_ids = list(_jobs.keys())
    return [_job_status(run_id) for run_id in run_ids]


@app.get("/runs/{run_id}", response_model=RunStatus)
def get_run(run_id: str) -> RunStatus:
    return _job_status(run_id)


@app.get("/runs/{run_id}/log", response_class=PlainTextResponse)
def get_run_log(
    run_id: str,
    log_type: Literal["info", "debug"] = Query(default="info"),
    lines: int = Query(default=200, ge=1, le=2000),
) -> str:
    status = _job_status(run_id)
    path = status.info_log if log_type == "info" else status.debug_log
    return _tail_file(path, lines)


@app.post("/runs/{run_id}/stop", response_model=RunStatus)
def stop_run(run_id: str) -> RunStatus:
    with _jobs_lock:
        job = _jobs.get(run_id)
    if not job:
        raise HTTPException(status_code=404, detail="run_id not found")

    process: multiprocessing.Process = job["process"]  # type: ignore[assignment]
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
    return _job_status(run_id)


class UploadResponse(BaseModel):
    code: int
    value: int


def _prepare_input_from_zip(scenario_id: str, zip_path: str) -> Dict[str, str]:
    base_dir = os.path.join(_BASE_RUN_DIR, scenario_id)
    input_dir = os.path.join(base_dir, "input")
    output_dir = os.path.join(base_dir, "output")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(input_dir)

    network_dir = os.path.join(input_dir, "network")
    demand_dir = os.path.join(input_dir, "demand")
    run_config = os.path.join(demand_dir, "config_ft.txt")
    input_weights = os.path.join(demand_dir, "pathweight_ft.txt")

    return {
        "input_network_dir": network_dir,
        "input_demand_dir": demand_dir,
        "run_config": run_config,
        "input_weights": input_weights,
        "output_dir": output_dir,
    }


def _start_scenario_run(scenario_id: str, run_kwargs: Dict[str, object]) -> str:
    run_id = uuid.uuid4().hex
    error_queue: multiprocessing.Queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=_run_fasttrips_target, args=(run_kwargs, error_queue))
    process.start()

    output_dir = run_kwargs["output_dir"]
    info_log = os.path.join(output_dir, "ft_info.log")
    debug_log = os.path.join(output_dir, "ft_debug.log")
    performance_csv = os.path.join(output_dir, "ft_output_performance.csv")

    with _jobs_lock:
        _jobs[run_id] = {
            "process": process,
            "error_queue": error_queue,
            "started_at": datetime.datetime.utcnow().isoformat() + "Z",
            "output_dir": output_dir,
            "info_log": info_log,
            "debug_log": debug_log,
            "performance_csv": performance_csv,
            "finished_at": None,
            "error": None,
            "scenario_id": scenario_id,
        }
        _scenario_index[scenario_id] = run_id

    return run_id


@app.post("/scenario/upload", response_model=UploadResponse)
def upload_input(
    scenarioId: str = Form(...),
    needFile: Optional[UploadFile] = File(default=None),
) -> UploadResponse:
    if not needFile:
        if scenarioId in _scenario_index:
            return UploadResponse(code=0, value=0)
        return UploadResponse(code=1, value=1)

    os.makedirs(_BASE_RUN_DIR, exist_ok=True)
    zip_path = os.path.join(_BASE_RUN_DIR, f"{scenarioId}.zip")
    with open(zip_path, "wb") as handle:
        handle.write(needFile.file.read())

    inputs = _prepare_input_from_zip(scenarioId, zip_path)
    run_kwargs = {
        "pathfinding_type": "stochastic",
        "iters": 10,
        "run_config": inputs["run_config"],
        "input_network_dir": inputs["input_network_dir"],
        "input_demand_dir": inputs["input_demand_dir"],
        "input_weights": inputs["input_weights"],
        "output_dir": inputs["output_dir"],
    }
    _start_scenario_run(scenarioId, run_kwargs)
    return UploadResponse(code=0, value=0)


class StatusValue(BaseModel):
    scenarioId: str
    status: Literal["running", "succeeded", "failed"]
    progress: int
    message: Optional[str] = None


class StatusResponse(BaseModel):
    code: int
    value: StatusValue


@app.get("/scenario/{scenario_id}/status", response_model=StatusResponse)
def get_scenario_status(
    scenario_id: str,
) -> StatusResponse:
    run_id = _scenario_index.get(scenario_id)
    if not run_id:
        raise HTTPException(status_code=404, detail="scenarioId not found")

    status = _job_status(run_id)
    progress = 0 if status.status == "running" else 100
    value = StatusValue(
        scenarioId=scenario_id,
        status=status.status,
        progress=progress,
        message=status.error,
    )
    return StatusResponse(code=0, value=value)


class ResultResponse(BaseModel):
    code: int
    success: bool
    message: str
    timestamp: int


@app.post("/scenario/result", response_model=ResultResponse)
def receive_result(
    scenarioId: str = Form(...),
    code: int = Form(...),
    message: str = Form(...),
    resultFile: Optional[UploadFile] = File(default=None),
) -> ResultResponse:
    os.makedirs(_BASE_RUN_DIR, exist_ok=True)
    scenario_dir = os.path.join(_BASE_RUN_DIR, scenarioId)
    os.makedirs(scenario_dir, exist_ok=True)

    if resultFile:
        result_path = os.path.join(scenario_dir, "resultFile.zip")
        with open(result_path, "wb") as handle:
            handle.write(resultFile.file.read())

    success = bool(code == 1)
    ts = int(datetime.datetime.utcnow().timestamp())
    if scenarioId not in _scenario_index:
        return ResultResponse(code=1, success=False, message="scenarioId not exist", timestamp=ts)
    return ResultResponse(code=0, success=success, message=message, timestamp=ts)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.server:app", host="0.0.0.0", port=8000, reload=False)
