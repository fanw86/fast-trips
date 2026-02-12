from __future__ import annotations

import configparser
import datetime
import json
import os
import re
import sys
import threading
import uuid
import zipfile
from typing import Dict, List, Literal, Optional

import multiprocessing
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

from fasttrips.Run import run_fasttrips


app = FastAPI(title="Transit Assignment API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_event():
    """Load past runs from disk on server startup"""
    print("Loading past runs from disk...")
    _load_all_past_runs()


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
    status: Literal["running", "succeeded", "failed", "stopped"]
    pid: int
    started_at: str
    finished_at: Optional[str]
    exit_code: Optional[int]
    output_dir: str
    info_log: str
    debug_log: str
    terminal_log: str
    performance_csv: str
    error: Optional[str] = None
    progress: int = 0
    current_iteration: Optional[int] = None
    max_iterations: Optional[int] = None
    pathfinding_iteration: Optional[int] = None
    paths_sought: Optional[int] = None
    total_paths: Optional[int] = None
    num_passengers_arrived: Optional[int] = None
    num_bumped_passengers: Optional[int] = None
    converged: Optional[bool] = None
    convergence_gap: Optional[float] = None
    convergence_threshold: Optional[float] = None
    termination_reason: Optional[Literal["converged", "max_iterations", "failed", "stopped"]] = None


_jobs_lock = threading.Lock()
_jobs: Dict[str, Dict[str, object]] = {}
_scenario_index: Dict[str, str] = {}
_BASE_RUN_DIR = os.environ.get("FASTTRIPS_API_RUN_DIR", os.path.join("api", "runs"))


def _model_to_dict(model: BaseModel) -> Dict[str, object]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _default_output_folder(req: RunRequest) -> str:
    # Determine capacity: if explicitly True use it, otherwise check config file
    if req.capacity:
        capacity = True
    else:
        capacity = _parse_capacity_constraint(req.run_config) if req.run_config else False
    cap_suffix = "cap" if capacity else "nocap"
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


def _run_fasttrips_target(
    kwargs: Dict[str, object],
    error_queue: multiprocessing.Queue,
    terminal_log_path: str
) -> None:
    # Enable progress reporting for API runs
    os.environ['FASTTRIPS_PROGRESS_ENABLED'] = '1'

    # Ensure output directory exists before creating terminal log
    os.makedirs(os.path.dirname(terminal_log_path), exist_ok=True)

    # Save original file descriptors
    original_stdout_fd = os.dup(1)
    original_stderr_fd = os.dup(2)

    # Open terminal log file
    terminal_log_fd = os.open(terminal_log_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)

    try:
        # Redirect file descriptors (captures C++ output too)
        os.dup2(terminal_log_fd, 1)  # stdout
        os.dup2(terminal_log_fd, 2)  # stderr

        # Also redirect Python's sys.stdout and sys.stderr
        sys.stdout = os.fdopen(os.dup(terminal_log_fd), "w", encoding="utf-8")
        sys.stderr = sys.stdout

        try:
            run_fasttrips(**kwargs)
        except Exception as exc:  # noqa: BLE001 - surface fast-trips errors to API
            error_queue.put(str(exc))
            raise
    finally:
        # Restore original file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        os.dup2(original_stdout_fd, 1)
        os.dup2(original_stderr_fd, 2)
        os.close(original_stdout_fd)
        os.close(original_stderr_fd)
        os.close(terminal_log_fd)


def _tail_file(path: str, max_lines: int) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        lines = handle.readlines()
    return "".join(lines[-max_lines:])


def _parse_convergence_threshold(config_path: str) -> Optional[float]:
    """Parse convergence_gap from config_ft.txt file. Returns None if not set."""
    if not config_path or not os.path.exists(config_path):
        return None
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        if config.has_option("fasttrips", "convergence_gap"):
            return config.getfloat("fasttrips", "convergence_gap")
    except (configparser.Error, ValueError):
        pass
    return None


def _parse_capacity_constraint(config_path: str) -> bool:
    """Parse capacity_constraint from config_ft.txt file. Returns False if not set."""
    if not config_path or not os.path.exists(config_path):
        return False
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        if config.has_option("fasttrips", "capacity_constraint"):
            return config.getboolean("fasttrips", "capacity_constraint")
    except (configparser.Error, ValueError):
        pass
    return False


def _parse_max_iterations(config_path: str) -> int:
    """Parse max_iterations from config_ft.txt file. Returns 1 if not set."""
    if not config_path or not os.path.exists(config_path):
        return 1
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        if config.has_option("fasttrips", "max_iterations"):
            return config.getint("fasttrips", "max_iterations")
    except (configparser.Error, ValueError):
        pass
    return 1


def _parse_pathfinding_type(config_path: str) -> str:
    """Parse pathfinding_type from config_ft.txt file. Returns 'stochastic' if not set."""
    if not config_path or not os.path.exists(config_path):
        return "stochastic"
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        if config.has_option("pathfinding", "pathfinding_type"):
            return config.get("pathfinding", "pathfinding_type")
    except (configparser.Error, ValueError):
        pass
    return "stochastic"


def _estimate_progress(job: Dict[str, object]) -> Dict[str, object]:
    """Estimate run progress from progress file or info log.

    First tries to read the structured progress file (ft_progress.json).
    Falls back to parsing info log if progress file is unavailable.
    """
    output_dir = job.get("output_dir", "")
    max_iterations = job.get("max_iterations", 10)
    convergence_threshold = job.get("convergence_threshold")

    # Try progress file first (preferred method)
    progress_path = os.path.join(output_dir, "ft_progress.json") if output_dir else ""
    if progress_path and os.path.exists(progress_path):
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            phase = data.get("phase", "")
            iteration = data.get("iteration", 1)
            file_max_iterations = data.get("max_iterations", max_iterations)
            paths_sought = data.get("paths_sought", 0)
            total_paths = data.get("total_paths", 0)
            num_passengers_arrived = data.get("num_passengers_arrived")
            num_bumped_passengers = data.get("num_bumped_passengers")
            capacity_gap = data.get("capacity_gap")
            converged = data.get("converged")

            # Calculate progress based on outer iteration + phase
            if phase == "completed":
                progress = 100
                termination_reason = "converged" if converged else "max_iterations"
            else:
                # Base progress from completed iterations
                base = ((iteration - 1) / file_max_iterations) * 100 if file_max_iterations > 0 else 0

                # Add phase weight within current iteration
                if phase == "pathfinding" and total_paths > 0:
                    phase_weight = 0.8 * (paths_sought / total_paths)
                elif phase == "simulation_complete":
                    phase_weight = 0.9
                elif phase == "iteration_complete":
                    phase_weight = 1.0
                else:  # iteration_start
                    phase_weight = 0.0

                progress = int(base + (phase_weight / file_max_iterations) * 100) if file_max_iterations > 0 else 0
                progress = min(99, max(0, progress))
                termination_reason = None

            return {
                "progress": progress,
                "current_iteration": iteration,
                "max_iterations": file_max_iterations,
                "pathfinding_iteration": data.get("pathfinding_iteration"),
                "paths_sought": paths_sought,
                "total_paths": total_paths,
                "num_passengers_arrived": num_passengers_arrived,
                "num_bumped_passengers": num_bumped_passengers,
                "converged": converged,
                "convergence_gap": capacity_gap,
                "convergence_threshold": convergence_threshold,
                "termination_reason": termination_reason,
            }
        except (json.JSONDecodeError, IOError, KeyError):
            pass  # Fall back to log parsing

    # Fall back to log parsing
    info_log = job.get("info_log", "")
    if not info_log or not os.path.exists(info_log):
        return {
            "progress": 0,
            "current_iteration": 0,
            "max_iterations": max_iterations,
            "paths_sought": 0,
            "total_paths": 0,
            "convergence_threshold": convergence_threshold,
        }

    # Read more lines to ensure we capture iteration markers
    # (simulation phases can produce many log lines)
    content = _tail_file(info_log, 2000)

    # Find iteration markers: ***** ITERATION X PATHFINDING ITERATION Y *****
    iteration_matches = re.findall(r'\*+ ITERATION (\d+)', content)
    current_iteration = int(iteration_matches[-1]) if iteration_matches else 0

    # Find pathfinding progress: X paths sought, Y paths found of Z paths total
    pf_matches = re.findall(r'(\d+) paths sought.*?of (\d+) paths total', content)
    paths_sought, total_paths = (int(pf_matches[-1][0]), int(pf_matches[-1][1])) if pf_matches else (0, 0)

    # Parse CAPACITY GAP values from log
    gap_matches = re.findall(r'CAPACITY GAP:\s+([\d.]+)', content)
    convergence_gap = float(gap_matches[-1]) if gap_matches else None

    # Check for successful completion
    completed = "Successfully completed!" in content

    # Determine convergence status and termination reason
    converged = None
    termination_reason = None

    if completed:
        if convergence_threshold is not None and convergence_gap is not None:
            # Can determine convergence only if both threshold and gap are available
            if convergence_gap < convergence_threshold:
                converged = True
                termination_reason = "converged"
            else:
                converged = False
                termination_reason = "max_iterations"
        else:
            # Completed but can't determine convergence - assume max_iterations
            converged = False
            termination_reason = "max_iterations"

    # Calculate progress percentage
    if max_iterations > 0 and total_paths > 0:
        pf_progress = paths_sought / total_paths  # 0 to 1
        progress = int(((current_iteration - 1 + pf_progress) / max_iterations) * 100)
        progress = max(0, min(100, progress))
    elif current_iteration > 0:
        progress = int((current_iteration / max_iterations) * 100)
    else:
        progress = 0

    return {
        "progress": progress,
        "current_iteration": current_iteration,
        "max_iterations": max_iterations,
        "paths_sought": paths_sought,
        "total_paths": total_paths,
        "converged": converged,
        "convergence_gap": convergence_gap,
        "convergence_threshold": convergence_threshold,
        "termination_reason": termination_reason,
    }


def _get_metadata_path(run_id: str, scenario_id: Optional[str] = None) -> str:
    """Get the path to the metadata file for a run"""
    if scenario_id:
        # For scenario uploads, store in scenario directory
        scenario_dir = os.path.join(_BASE_RUN_DIR, scenario_id)
        return os.path.join(scenario_dir, "run_metadata.json")
    else:
        # For direct runs, store in .jobs directory
        jobs_dir = os.path.join(_BASE_RUN_DIR, ".jobs")
        os.makedirs(jobs_dir, exist_ok=True)
        return os.path.join(jobs_dir, f"{run_id}.json")


def _save_job_metadata(run_id: str, job_data: Dict[str, object]) -> None:
    """Save job metadata to disk for persistence"""
    scenario_id = job_data.get("scenario_id")
    metadata_path = _get_metadata_path(run_id, scenario_id)

    # Ensure parent directory exists
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

    # Prepare serializable metadata (exclude non-JSON objects)
    metadata = {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "started_at": job_data["started_at"],
        "finished_at": job_data.get("finished_at"),
        "output_dir": job_data["output_dir"],
        "info_log": job_data["info_log"],
        "debug_log": job_data["debug_log"],
        "terminal_log": job_data["terminal_log"],
        "performance_csv": job_data["performance_csv"],
        "error": job_data.get("error"),
        "exit_code": job_data.get("exit_code"),
        "pid": job_data.get("pid"),
        "max_iterations": job_data.get("max_iterations"),
        "convergence_threshold": job_data.get("convergence_threshold"),
        "converged": job_data.get("converged"),
        "convergence_gap": job_data.get("convergence_gap"),
        "termination_reason": job_data.get("termination_reason"),
    }

    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)


def _load_job_metadata(run_id: str, scenario_id: Optional[str] = None) -> Optional[Dict[str, object]]:
    """Load job metadata from disk"""
    metadata_path = _get_metadata_path(run_id, scenario_id)

    if not os.path.exists(metadata_path):
        return None

    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def _detect_completion_from_files(output_dir: str) -> Optional[Dict[str, object]]:
    """Detect completion from ft_progress.json.

    This is the primary method for detecting completion status, since ft_progress.json
    is written by the FastTrips process and reliably captures the final state.
    """
    progress_path = os.path.join(output_dir, "ft_progress.json")
    if os.path.exists(progress_path):
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("phase") == "completed":
                return {
                    "capacity_gap": data.get("capacity_gap"),
                    "converged": data.get("converged"),
                    "num_passengers_arrived": data.get("num_passengers_arrived"),
                    "num_bumped_passengers": data.get("num_bumped_passengers"),
                    "total_demand": data.get("total_demand"),
                    "timestamp": data.get("timestamp"),
                }
        except (json.JSONDecodeError, IOError):
            pass
    return None


def _load_all_past_runs() -> None:
    """Load all past runs from disk on server startup"""
    if not os.path.exists(_BASE_RUN_DIR):
        return

    loaded_count = 0

    # Load scenario runs
    for item in os.listdir(_BASE_RUN_DIR):
        item_path = os.path.join(_BASE_RUN_DIR, item)
        if os.path.isdir(item_path) and item != ".jobs":
            metadata_path = os.path.join(item_path, "run_metadata.json")
            if os.path.exists(metadata_path):
                try:
                    with open(metadata_path, "r", encoding="utf-8") as f:
                        metadata = json.load(f)

                    run_id = metadata["run_id"]
                    scenario_id = metadata.get("scenario_id")
                    output_dir = metadata["output_dir"]

                    # Get completion data from ft_progress.json (primary source)
                    progress_data = _detect_completion_from_files(output_dir)

                    # Start with metadata values
                    converged = metadata.get("converged")
                    convergence_gap = metadata.get("convergence_gap")
                    termination_reason = metadata.get("termination_reason")
                    finished_at = metadata.get("finished_at")
                    exit_code = metadata.get("exit_code")

                    # Override with progress data if available (more reliable)
                    if progress_data:
                        if converged is None:
                            converged = progress_data.get("converged")
                        if convergence_gap is None:
                            convergence_gap = progress_data.get("capacity_gap")
                        # If metadata doesn't have finished_at, infer from progress file
                        if finished_at is None:
                            finished_at = progress_data.get("timestamp") or metadata["started_at"]
                            exit_code = 0  # Completed successfully
                        # Determine termination_reason
                        if termination_reason is None:
                            if converged:
                                termination_reason = "converged"
                            else:
                                termination_reason = "max_iterations"

                    # Create a job entry with metadata (no process object)
                    with _jobs_lock:
                        _jobs[run_id] = {
                            "process": None,  # Past run, no active process
                            "error_queue": None,
                            "started_at": metadata["started_at"],
                            "finished_at": finished_at,
                            "output_dir": output_dir,
                            "info_log": metadata["info_log"],
                            "debug_log": metadata["debug_log"],
                            "terminal_log": metadata.get("terminal_log", ""),
                            "performance_csv": metadata["performance_csv"],
                            "error": metadata.get("error"),
                            "exit_code": exit_code,
                            "pid": metadata.get("pid"),
                            "scenario_id": scenario_id,
                            "max_iterations": metadata.get("max_iterations", 10),
                            "convergence_threshold": metadata.get("convergence_threshold"),
                            "converged": converged,
                            "convergence_gap": convergence_gap,
                            "termination_reason": termination_reason,
                        }
                        if scenario_id:
                            _scenario_index[scenario_id] = run_id

                    loaded_count += 1
                except (json.JSONDecodeError, IOError, KeyError):
                    continue

    # Load direct runs from .jobs directory
    jobs_dir = os.path.join(_BASE_RUN_DIR, ".jobs")
    if os.path.exists(jobs_dir):
        for filename in os.listdir(jobs_dir):
            if filename.endswith(".json"):
                metadata_path = os.path.join(jobs_dir, filename)
                try:
                    with open(metadata_path, "r", encoding="utf-8") as f:
                        metadata = json.load(f)

                    run_id = metadata["run_id"]
                    output_dir = metadata["output_dir"]

                    # Get completion data from ft_progress.json (primary source)
                    progress_data = _detect_completion_from_files(output_dir)

                    # Start with metadata values
                    converged = metadata.get("converged")
                    convergence_gap = metadata.get("convergence_gap")
                    termination_reason = metadata.get("termination_reason")
                    finished_at = metadata.get("finished_at")
                    exit_code = metadata.get("exit_code")

                    # Override with progress data if available (more reliable)
                    if progress_data:
                        if converged is None:
                            converged = progress_data.get("converged")
                        if convergence_gap is None:
                            convergence_gap = progress_data.get("capacity_gap")
                        # If metadata doesn't have finished_at, infer from progress file
                        if finished_at is None:
                            finished_at = progress_data.get("timestamp") or metadata["started_at"]
                            exit_code = 0  # Completed successfully
                        # Determine termination_reason
                        if termination_reason is None:
                            if converged:
                                termination_reason = "converged"
                            else:
                                termination_reason = "max_iterations"

                    with _jobs_lock:
                        _jobs[run_id] = {
                            "process": None,
                            "error_queue": None,
                            "started_at": metadata["started_at"],
                            "finished_at": finished_at,
                            "output_dir": output_dir,
                            "info_log": metadata["info_log"],
                            "debug_log": metadata["debug_log"],
                            "terminal_log": metadata.get("terminal_log", ""),
                            "performance_csv": metadata["performance_csv"],
                            "error": metadata.get("error"),
                            "exit_code": exit_code,
                            "pid": metadata.get("pid"),
                            "scenario_id": None,
                            "max_iterations": metadata.get("max_iterations", 10),
                            "convergence_threshold": metadata.get("convergence_threshold"),
                            "converged": converged,
                            "convergence_gap": convergence_gap,
                            "termination_reason": termination_reason,
                        }

                    loaded_count += 1
                except (json.JSONDecodeError, IOError, KeyError):
                    continue

    if loaded_count > 0:
        print(f"Loaded {loaded_count} past run(s) from disk")


def _job_status(run_id: str) -> RunStatus:
    with _jobs_lock:
        job = _jobs.get(run_id)
    if not job:
        raise HTTPException(status_code=404, detail="run_id not found")

    process: Optional[multiprocessing.Process] = job.get("process")  # type: ignore[assignment]
    error_queue: Optional[multiprocessing.Queue] = job.get("error_queue")  # type: ignore[assignment]

    # Handle past runs (process is None)
    if process is None:
        # This is a past run loaded from disk
        exit_code = job.get("exit_code")
        finished_at = job.get("finished_at")
        stored_termination_reason = job.get("termination_reason")

        # Infer status from termination_reason or exit_code
        if stored_termination_reason == "stopped":
            status = "stopped"
        elif exit_code is not None:
            status = "succeeded" if exit_code == 0 else "failed"
        elif finished_at:
            # Has finished_at but no exit_code - assume succeeded if logs exist
            status = "succeeded" if os.path.exists(job["info_log"]) else "failed"
        else:
            # No exit code or finished_at - treat as failed (incomplete)
            status = "failed"
            if not finished_at:
                finished_at = job.get("started_at")  # Use start time as fallback

        # Calculate progress for past runs
        if status == "succeeded":
            progress_info = _estimate_progress(job)
            progress_info["progress"] = 100
        else:
            progress_info = _estimate_progress(job)

        # Determine termination reason for past runs
        termination_reason = stored_termination_reason or progress_info.get("termination_reason")
        if status == "stopped":
            termination_reason = "stopped"
        elif status == "failed" and termination_reason is None:
            termination_reason = "failed"

        return RunStatus(
            run_id=run_id,
            status=status,
            pid=job.get("pid") or 0,  # Handle None pid
            started_at=job["started_at"],
            finished_at=finished_at,
            exit_code=exit_code,
            output_dir=job["output_dir"],
            info_log=job["info_log"],
            debug_log=job["debug_log"],
            terminal_log=job.get("terminal_log", ""),
            performance_csv=job["performance_csv"],
            error=job.get("error"),
            progress=progress_info.get("progress", 0),
            current_iteration=progress_info.get("current_iteration"),
            max_iterations=progress_info.get("max_iterations"),
            pathfinding_iteration=progress_info.get("pathfinding_iteration"),
            paths_sought=progress_info.get("paths_sought"),
            total_paths=progress_info.get("total_paths"),
            num_passengers_arrived=progress_info.get("num_passengers_arrived"),
            num_bumped_passengers=progress_info.get("num_bumped_passengers"),
            converged=progress_info.get("converged"),
            convergence_gap=progress_info.get("convergence_gap"),
            convergence_threshold=progress_info.get("convergence_threshold"),
            termination_reason=termination_reason,
        )

    # Handle active runs (process is not None)
    exit_code = process.exitcode
    finished_at = None
    status = "running"
    if exit_code is not None:
        # Check if this was a user-initiated stop
        if job.get("termination_reason") == "stopped":
            status = "stopped"
        else:
            status = "succeeded" if exit_code == 0 else "failed"
        finished_at = job.get("finished_at")
        if not finished_at:
            finished_at = datetime.datetime.utcnow().isoformat() + "Z"
            job["finished_at"] = finished_at
            job["exit_code"] = exit_code
            job["pid"] = process.pid

            # Save updated metadata to disk
            _save_job_metadata(run_id, job)

    error = job.get("error")
    if error is None and error_queue and not error_queue.empty():
        try:
            error = error_queue.get_nowait()
            job["error"] = error
            # Save error to metadata
            _save_job_metadata(run_id, job)
        except Exception:
            error = None

    # Check for completion data from ft_progress.json when run finishes
    if exit_code is not None and job.get("converged") is None:
        progress_data = _detect_completion_from_files(job["output_dir"])
        if progress_data:
            job["convergence_gap"] = progress_data.get("capacity_gap")
            job["converged"] = progress_data.get("converged")
            if job.get("converged"):
                job["termination_reason"] = "converged"
            else:
                job["termination_reason"] = "max_iterations"
            # Save updated metadata
            _save_job_metadata(run_id, job)

    # Calculate progress for active runs
    if status == "succeeded":
        progress_info = _estimate_progress(job)
        progress_info["progress"] = 100
    elif status == "failed":
        progress_info = _estimate_progress(job)
    else:  # running
        progress_info = _estimate_progress(job)

    # Determine termination reason
    termination_reason = job.get("termination_reason") or progress_info.get("termination_reason")
    if status == "stopped":
        termination_reason = "stopped"
    elif status == "failed" and termination_reason is None:
        termination_reason = "failed"
    elif status == "running":
        termination_reason = None

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
        terminal_log=job.get("terminal_log", ""),
        performance_csv=job["performance_csv"],
        error=error,
        progress=progress_info.get("progress", 0),
        current_iteration=progress_info.get("current_iteration"),
        max_iterations=progress_info.get("max_iterations"),
        pathfinding_iteration=progress_info.get("pathfinding_iteration"),
        paths_sought=progress_info.get("paths_sought"),
        total_paths=progress_info.get("total_paths"),
        num_passengers_arrived=progress_info.get("num_passengers_arrived"),
        num_bumped_passengers=progress_info.get("num_bumped_passengers"),
        converged=progress_info.get("converged"),
        convergence_gap=progress_info.get("convergence_gap"),
        convergence_threshold=progress_info.get("convergence_threshold"),
        termination_reason=termination_reason,
    )


@app.post("/runs", response_model=RunStatus)
def create_run(req: RunRequest) -> RunStatus:
    run_id = uuid.uuid4().hex
    output_folder = req.output_folder or _default_output_folder(req)
    full_output_dir = os.path.join(req.output_dir, output_folder)
    info_log = os.path.join(full_output_dir, "ft_info.log")
    debug_log = os.path.join(full_output_dir, "ft_debug.log")
    terminal_log = os.path.join(full_output_dir, "ft_terminal.log")
    performance_csv = os.path.join(full_output_dir, "ft_output_performance.csv")

    # Parse convergence_gap from config file (None if not set)
    convergence_threshold = _parse_convergence_threshold(req.run_config)

    kwargs = _build_run_kwargs(req)
    error_queue: multiprocessing.Queue = multiprocessing.Queue()
    process = multiprocessing.Process(
        target=_run_fasttrips_target,
        args=(kwargs, error_queue, terminal_log)
    )
    process.start()

    with _jobs_lock:
        _jobs[run_id] = {
            "process": process,
            "error_queue": error_queue,
            "started_at": datetime.datetime.utcnow().isoformat() + "Z",
            "output_dir": full_output_dir,
            "info_log": info_log,
            "debug_log": debug_log,
            "terminal_log": terminal_log,
            "performance_csv": performance_csv,
            "finished_at": None,
            "error": None,
            "exit_code": None,
            "pid": process.pid,
            "scenario_id": None,
            "max_iterations": req.iters,
            "convergence_threshold": convergence_threshold,
        }

        # Save metadata to disk for persistence
        _save_job_metadata(run_id, _jobs[run_id])

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
    log_type: Literal["info", "debug", "terminal"] = Query(default="info"),
    lines: int = Query(default=200, ge=1, le=2000),
) -> str:
    status = _job_status(run_id)
    if log_type == "info":
        path = status.info_log
    elif log_type == "debug":
        path = status.debug_log
    else:  # terminal
        path = status.terminal_log
    return _tail_file(path, lines)


@app.post("/runs/{run_id}/stop", response_model=RunStatus)
def stop_run(run_id: str) -> RunStatus:
    """Stop a running simulation.

    Terminates the process and marks the run with termination_reason='stopped'.
    """
    with _jobs_lock:
        job = _jobs.get(run_id)
    if not job:
        raise HTTPException(status_code=404, detail="run_id not found")

    process: Optional[multiprocessing.Process] = job.get("process")  # type: ignore[assignment]
    if process is None:
        # Past run loaded from disk - can't stop
        raise HTTPException(status_code=400, detail="Run already finished (past run)")

    if process.is_alive():
        process.terminate()
        process.join(timeout=5)

        # If still alive after terminate, force kill
        if process.is_alive():
            process.kill()
            process.join(timeout=2)

        # Mark as stopped by user
        with _jobs_lock:
            job["termination_reason"] = "stopped"
            job["finished_at"] = datetime.datetime.utcnow().isoformat() + "Z"
            job["exit_code"] = process.exitcode
            _save_job_metadata(run_id, job)
    else:
        # Process already finished
        pass

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

    # Calculate the expected output folder name that Fast-Trips will create
    pathfinding_type = run_kwargs.get("pathfinding_type", "stochastic")
    iters = run_kwargs.get("iters", 1)
    output_folder = run_kwargs.get("output_folder")

    # Determine capacity setting: use explicit kwarg if set, otherwise read from config file
    if "capacity" in run_kwargs:
        capacity = run_kwargs["capacity"]
    else:
        run_config = run_kwargs.get("run_config")
        capacity = _parse_capacity_constraint(run_config) if run_config else False

    if not output_folder:
        cap_suffix = "cap" if capacity else "nocap"
        output_folder = f"output_{pathfinding_type}_iter{iters}_{cap_suffix}"

    base_output_dir = run_kwargs["output_dir"]
    full_output_dir = os.path.join(base_output_dir, output_folder)
    info_log = os.path.join(full_output_dir, "ft_info.log")
    debug_log = os.path.join(full_output_dir, "ft_debug.log")
    terminal_log = os.path.join(full_output_dir, "ft_terminal.log")
    performance_csv = os.path.join(full_output_dir, "ft_output_performance.csv")

    # Parse convergence_gap from config file (None if not set)
    run_config = run_kwargs.get("run_config")
    convergence_threshold = _parse_convergence_threshold(run_config) if run_config else None

    error_queue: multiprocessing.Queue = multiprocessing.Queue()
    process = multiprocessing.Process(
        target=_run_fasttrips_target,
        args=(run_kwargs, error_queue, terminal_log)
    )
    process.start()

    with _jobs_lock:
        _jobs[run_id] = {
            "process": process,
            "error_queue": error_queue,
            "started_at": datetime.datetime.utcnow().isoformat() + "Z",
            "output_dir": full_output_dir,
            "info_log": info_log,
            "debug_log": debug_log,
            "terminal_log": terminal_log,
            "performance_csv": performance_csv,
            "finished_at": None,
            "error": None,
            "exit_code": None,
            "pid": process.pid,
            "scenario_id": scenario_id,
            "max_iterations": iters,
            "convergence_threshold": convergence_threshold,
        }
        _scenario_index[scenario_id] = run_id

        # Save metadata to disk for persistence
        _save_job_metadata(run_id, _jobs[run_id])

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
    run_config = inputs["run_config"]
    run_kwargs = {
        "pathfinding_type": _parse_pathfinding_type(run_config),
        "iters": _parse_max_iterations(run_config),
        "capacity": _parse_capacity_constraint(run_config),  # Pass explicitly so Run.py uses same folder name
        "run_config": run_config,
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


class FileInfo(BaseModel):
    name: str
    path: str
    size: int
    is_dir: bool


class FileListResponse(BaseModel):
    run_id: str
    output_dir: str
    files: List[FileInfo]


@app.get("/runs/{run_id}/files", response_model=FileListResponse)
def list_run_files(run_id: str) -> FileListResponse:
    """List all result files in a run's output directory."""
    status = _job_status(run_id)
    output_dir = status.output_dir

    if not os.path.exists(output_dir):
        raise HTTPException(status_code=404, detail="Output directory not found")

    files = []
    for root, dirs, filenames in os.walk(output_dir):
        for filename in filenames:
            full_path = os.path.join(root, filename)
            rel_path = os.path.relpath(full_path, output_dir)
            try:
                size = os.path.getsize(full_path)
            except OSError:
                size = 0
            files.append(FileInfo(
                name=filename,
                path=rel_path,
                size=size,
                is_dir=False,
            ))
        for dirname in dirs:
            full_path = os.path.join(root, dirname)
            rel_path = os.path.relpath(full_path, output_dir)
            files.append(FileInfo(
                name=dirname,
                path=rel_path,
                size=0,
                is_dir=True,
            ))

    # Sort: directories first, then files, both alphabetically
    files.sort(key=lambda f: (not f.is_dir, f.name.lower()))

    return FileListResponse(
        run_id=run_id,
        output_dir=output_dir,
        files=files,
    )


@app.get("/runs/{run_id}/files/{file_path:path}")
def download_run_file(run_id: str, file_path: str) -> FileResponse:
    """Download a specific file from a run's output directory."""
    status = _job_status(run_id)
    output_dir = status.output_dir

    # Construct full path and ensure it's within output_dir (prevent path traversal)
    full_path = os.path.normpath(os.path.join(output_dir, file_path))
    if not full_path.startswith(os.path.normpath(output_dir)):
        raise HTTPException(status_code=403, detail="Access denied")

    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")

    if os.path.isdir(full_path):
        raise HTTPException(status_code=400, detail="Path is a directory, not a file")

    return FileResponse(
        path=full_path,
        filename=os.path.basename(file_path),
    )


@app.get("/runs/{run_id}/download")
def download_run_results(run_id: str) -> StreamingResponse:
    """Download all result files as a ZIP archive."""
    status = _job_status(run_id)
    output_dir = status.output_dir

    if not os.path.exists(output_dir):
        raise HTTPException(status_code=404, detail="Output directory not found")

    # Create a temporary zip file
    import tempfile
    import io

    # Use in-memory zip creation
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, filenames in os.walk(output_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                arcname = os.path.relpath(full_path, output_dir)
                zipf.write(full_path, arcname)

    zip_buffer.seek(0)

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename=run_{run_id}_results.zip"
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.server:app", host="0.0.0.0", port=8000, reload=False)
